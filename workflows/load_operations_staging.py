from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os
from pathlib import Path
import pickle

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_source_path():
    """Get the correct path to source files."""
    base_paths = [
        '/opt/airflow/source/operations-department',
        '/home/vgr/dev/school/shopzada_3CSD_grp5/source/operations-department'
    ]
    
    for path in base_paths:
        if os.path.exists(path):
            print(f"Using source path: {path}")
            return path
    
    raise FileNotFoundError(f"Could not find source files in any of: {base_paths}")

def get_db_engine():
    """Create SQLAlchemy engine for database connection."""
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER', 'airflow')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'airflow')}@"
        f"{os.getenv('POSTGRES_HOST', 'postgres')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/shopzada"
    )

def detect_and_load_file(file_path):
    """Detect file type and load data accordingly."""
    file_ext = Path(file_path).suffix.lower()
    file_name = Path(file_path).name
    
    print(f"Processing file: {file_name}")
    
    try:
        if file_ext in ['.xlsx', '.xls']:
            df = pd.read_excel(file_path)
            print(f"  → Loaded as Excel")
            
        elif file_ext == '.csv':
            try:
                df = pd.read_csv(file_path, index_col=0)
                df.reset_index(inplace=True)
                df.rename(columns={df.columns[0]: 'raw_index'}, inplace=True)
            except:
                df = pd.read_csv(file_path)
            print(f"  → Loaded as CSV")
            
        elif file_ext == '.parquet':
            df = pd.read_parquet(file_path)
            print(f"  → Loaded as Parquet")
            
        elif file_ext == '.json':
            df = pd.read_json(file_path)
            print(f"  → Loaded as JSON")
            
        elif file_ext == '.pickle':
            with open(file_path, 'rb') as f:
                df = pickle.load(f)
            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame(df)
            print(f"  → Loaded as Pickle")
            
        elif file_ext == '.html':
            # Read HTML tables
            try:
                dfs = pd.read_html(file_path)
                if len(dfs) > 0:
                    df = dfs[0]  # Take the first table
                    print(f"  → Loaded as HTML (found {len(dfs)} tables, using first)")
                else:
                    print(f"  ✗ No tables found in HTML file")
                    return None, None
            except Exception as html_error:
                print(f"  ✗ Error reading HTML file: {str(html_error)}")
                print(f"  → Trying alternative HTML parsing...")
                try:
                    # Try with different encoding
                    dfs = pd.read_html(file_path, encoding='utf-8')
                    if len(dfs) > 0:
                        df = dfs[0]
                        print(f"  → Successfully loaded with UTF-8 encoding")
                    else:
                        return None, None
                except:
                    return None, None
        else:
            print(f"  ✗ Unsupported file type: {file_ext}")
            return None, None
        
        # Store the original index as raw_index if it exists and wasn't already handled
        if 'raw_index' not in df.columns:
            if df.index.name or not all(df.index == range(len(df))):
                df.reset_index(inplace=True)
                df.rename(columns={df.columns[0]: 'raw_index'}, inplace=True)
        
        print(f"  → Shape: {df.shape}")
        print(f"  → Columns: {df.columns.tolist()}")
        return df, file_name
        
    except Exception as e:
        print(f"  ✗ Error loading file: {str(e)}")
        import traceback
        traceback.print_exc()
        return None, None

def determine_target_table(file_name):
    """Determine which staging table based on filename."""
    file_lower = file_name.lower()
    
    if 'line_item_data_prices' in file_lower:
        return 'ops_order_item_prices_raw'
    elif 'line_item_data_products' in file_lower:
        return 'ops_order_item_products_raw'
    elif 'order_delays' in file_lower:
        return 'ops_order_delays_raw'
    elif 'order_data' in file_lower:
        return 'ops_orders_raw'
    else:
        return None

def load_operations_staging_data(**context):
    """Load all operations department files into staging schema."""
    try:
        source_path = get_source_path()
    except FileNotFoundError as e:
        print(f"✗ Error: {str(e)}")
        print("Please verify the source path exists and contains operations department files.")
        raise
    
    engine = get_db_engine()
    
    print("=" * 70)
    print("LOADING OPERATIONS DEPARTMENT DATA TO STAGING SCHEMA")
    print("=" * 70)
    print(f"Source path: {source_path}")
    
    # Discover all files
    all_files = list(Path(source_path).glob('*'))
    data_files = [f for f in all_files if f.is_file() and not f.name.startswith('.')]
    
    if len(data_files) == 0:
        print(f"⚠ WARNING: No files found in {source_path}")
        print("Please check that your source files are in the correct location.")
        return
    
    print(f"\nFound {len(data_files)} files to process:")
    for f in data_files:
        print(f"  - {f.name}")
    print()
    
    load_summary = []
    
    for file_path in data_files:
        print(f"Processing: {file_path.name}")
        print(f"Full path: {file_path}")
        
        try:
            df, file_name = detect_and_load_file(str(file_path))
        except Exception as e:
            print(f"  ✗ Exception while loading file: {str(e)}")
            import traceback
            traceback.print_exc()
            load_summary.append({
                'file': file_path.name,
                'table': 'N/A',
                'rows': 0,
                'status': f'LOAD ERROR: {str(e)}'
            })
            print()
            continue
        
        if df is None:
            print(f"⚠ Skipping {file_name} (could not load)\n")
            load_summary.append({
                'file': file_name,
                'table': 'N/A',
                'rows': 0,
                'status': 'SKIPPED - Could not load'
            })
            continue
        
        # Determine target table
        table_name = determine_target_table(file_name)
        
        if table_name is None:
            print(f"  ⚠ Could not determine target table for {file_name}")
            print(f"  → File doesn't match any expected pattern")
            print(f"  → Skipping file\n")
            load_summary.append({
                'file': file_name,
                'table': 'UNKNOWN',
                'rows': len(df),
                'status': 'SKIPPED - Unknown file type'
            })
            continue
        
        print(f"  → Target table: staging.{table_name}")
        
        # Debug: Show what columns we have
        print(f"  → DataFrame columns: {df.columns.tolist()}")
        
        # Convert all columns to string (staging requirement)
        for col in df.columns:
            try:
                df[col] = df[col].astype(str)
            except Exception as e:
                print(f"  ⚠ Warning: Could not convert column {col} to string: {str(e)}")
        
        # Add metadata columns
        df['_source_file'] = file_name
        df['_ingested_at'] = pd.Timestamp.now()
        
        # Define expected columns for each table
        expected_columns_map = {
            'ops_order_item_prices_raw': ['raw_index', 'order_id', 'price', 'quantity'],
            'ops_order_item_products_raw': ['raw_index', 'order_id', 'product_name', 'product_id'],
            'ops_orders_raw': ['raw_index', 'order_id', 'user_id', 'estimated_arrival', 'transaction_date'],
            'ops_order_delays_raw': ['raw_index', 'order_id', 'delay_in_days']
        }
        
        expected_columns = expected_columns_map.get(table_name, [])
        
        # Keep only columns that exist in both df and expected columns, plus metadata
        available_cols = [col for col in expected_columns if col in df.columns]
        final_cols = available_cols + ['_source_file', '_ingested_at']
        df_to_load = df[final_cols].copy()
        
        print(f"  → Loading {len(df_to_load)} rows with {len(final_cols)} columns")
        print(f"  → Columns: {final_cols}")
        
        try:
            # Load to staging with chunking for large files
            chunk_size = 10000
            total_rows = len(df_to_load)
            
            if total_rows > chunk_size:
                print(f"  → Large file detected, loading in chunks of {chunk_size}")
                
                for i in range(0, total_rows, chunk_size):
                    chunk = df_to_load.iloc[i:i+chunk_size]
                    chunk.to_sql(
                        name=table_name,
                        con=engine,
                        schema='staging',
                        if_exists='append',
                        index=False,
                        method='multi'
                    )
                    
                    # Progress indicator
                    progress = min(i + chunk_size, total_rows)
                    print(f"     Progress: {progress:,}/{total_rows:,} rows ({progress/total_rows*100:.1f}%)")
            else:
                # Small file, load all at once
                df_to_load.to_sql(
                    name=table_name,
                    con=engine,
                    schema='staging',
                    if_exists='append',
                    index=False,
                    method='multi'
                )
            
            print(f"  ✓ Successfully loaded to staging.{table_name}")
            load_summary.append({
                'file': file_name,
                'table': table_name,
                'rows': len(df_to_load),
                'status': 'SUCCESS'
            })
            
        except Exception as e:
            print(f"  ✗ Error loading to staging: {str(e)}")
            load_summary.append({
                'file': file_name,
                'table': table_name,
                'rows': 0,
                'status': f'FAILED: {str(e)}'
            })
        
        print()
    
    # Print summary
    print("=" * 70)
    print("LOAD SUMMARY")
    print("=" * 70)
    for item in load_summary:
        status_icon = "✓" if item['status'] == 'SUCCESS' else "✗"
        print(f"{status_icon} {item['file']} → staging.{item['table']}")
        print(f"   Rows: {item['rows']} | Status: {item['status']}")
    print("=" * 70)
    
    # Verify data in staging tables
    conn = engine.raw_connection()
    cursor = conn.cursor()
    
    print("\nSTAGING TABLE ROW COUNTS:")
    tables = [
        'ops_order_item_prices_raw',
        'ops_order_item_products_raw', 
        'ops_orders_raw',
        'ops_order_delays_raw'
    ]
    
    for table in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM staging.{table};")
            count = cursor.fetchone()[0]
            print(f"  staging.{table}: {count} rows")
        except Exception as e:
            print(f"  staging.{table}: Error - {str(e)}")
    
    cursor.close()
    conn.close()
    
    # Store summary in XCom
    context['ti'].xcom_push(key='load_summary', value=load_summary)

def truncate_staging_tables(**context):
    """Truncate all operations staging tables before loading fresh data."""
    engine = get_db_engine()
    conn = engine.raw_connection()
    cursor = conn.cursor()
    
    print("Truncating operations department staging tables...")
    
    tables = [
        'ops_order_item_prices_raw',
        'ops_order_item_products_raw',
        'ops_orders_raw',
        'ops_order_delays_raw'
    ]
    
    for table in tables:
        try:
            cursor.execute(f"TRUNCATE TABLE staging.{table};")
            print(f"  ✓ Truncated staging.{table}")
        except Exception as e:
            print(f"  ⚠ Could not truncate staging.{table}: {str(e)}")
    
    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    'load_operations_staging',
    default_args=default_args,
    description='Load raw operations department data (orders, line items, delays) into staging schema',
    schedule_interval=None,
    catchup=False,
    tags=['staging', 'operations', 'raw', 'orders', 'line-items'],
) as dag:
    
    truncate_task = PythonOperator(
        task_id='truncate_staging_tables',
        python_callable=truncate_staging_tables,
    )
    
    load_task = PythonOperator(
        task_id='load_operations_staging_data',
        python_callable=load_operations_staging_data,
    )
    
    truncate_task >> load_task