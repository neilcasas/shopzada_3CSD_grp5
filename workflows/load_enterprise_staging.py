from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os
from pathlib import Path

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
        '/opt/airflow/source/enterprise-department',
        '/home/vgr/dev/school/shopzada_3CSD_grp5/source/enterprise-department'
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
        if file_ext in ['.parquet', '.parquet']:
            df = pd.read_parquet(file_path)
            print(f"  → Loaded as Parquet")
        elif file_ext == '.csv':
            df = pd.read_csv(file_path)
            print(f"  → Loaded as CSV")
        elif file_ext in ['.html', '.htm']:
            dfs = pd.read_html(file_path)
            df = dfs[0]  # Load first table
            print(f"  → Loaded as HTML")
        else:
            print(f"  ✗ Unsupported file type: {file_ext}")
            return None, None
        
    except Exception as e:
        print(f"  ✗ Error loading file: {str(e)}")
        import traceback
        traceback.print_exc()
        return None, None
def determine_staging_table(file_name, columns):
    """Determine the target staging table based on file name and columns."""
    file_name_lower = file_name.lower()
    cols_lower = [col.lower() for col in columns]
    
    if 'ent_products' in file_name_lower:
        return 'ent_products_raw'
    elif 'ent_products' in file_name_lower:
        return 'ent_products_raw'
    else:
        return None
    
def load_enterprise_staging_data(**context):
    """Load all enterprise department files into staging schema."""
    source_path = get_source_path()
    engine = get_db_engine()
    
    # Discover all files
    all_files = list(Path(source_path).glob('*'))
    data_files = [f for f in all_files if f.is_file() and not f.name.startswith('.')]

    load_summary = []
    
    for file_path in data_files:
        df, file_name = detect_and_load_file(str(file_path))
        if df is None:
            print(f"⚠ Skipping {file_name}\n")
            continue
        
        # Determine target table based on file name and columns
        table_name = determine_staging_table(file_name, df.columns)
        if table_name is None:
            print(f"  ✗ Could not determine target table for {file_name}")
            continue
        
        print(f"  → Target table: staging.{table_name}")
        
        # Convert all columns to string (staging requirement)
        for col in df.columns:
            df[col] = df[col].astype(str)
        
        # Add metadata columns
        df['_source_file'] = file_name
        df['_ingested_at'] = pd.Timestamp.now()
        
        # Expected columns for ent_products_raw
        expected_columns = ['raw_index', 'product_id', 'product_name', 'product_type', 'price']
        
        # Keep only columns that exist in both df and expected columns, plus metadata
        available_cols = [col for col in expected_columns if col in df.columns]
        final_cols = available_cols + ['_source_file', '_ingested_at']
        df_to_load = df[final_cols].copy()
        
        print(f"  → Loading {len(df_to_load)} rows with {len(final_cols)} columns")
        print(f"  → Columns: {final_cols}")
        
        try:
            # Load to staging (append mode - allows multiple files)
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
    
    # Verify data in staging table
    conn = engine.raw_connection()
    cursor = conn.cursor()
    
    print("\nSTAGING TABLE ROW COUNT:")
    try:
        cursor.execute("SELECT COUNT(*) FROM staging.ent_products_raw;")
        count = cursor.fetchone()[0]
        print(f"  staging.ent_products_raw: {count} rows")
    except Exception as e:
        print(f"  staging.ent_products_raw: Error - {str(e)}")
    
    cursor.close()
    conn.close()
    
    # Store summary in XCom
    context['ti'].xcom_push(key='load_summary', value=load_summary)

def truncate_staging_table(**context):
    """Truncate staging table before loading fresh data."""
    engine = get_db_engine()
    conn = engine.raw_connection()
    cursor = conn.cursor()
    
    print("Truncating enterprise department staging table...")

    tables = [
        'staging.ent_products_raw',
        'staging.ent_merchants_raw',
        'staging.ent_staff_raw'
        ]
    for table in tables:
        try:
            cursor.execute(f"TRUNCATE TABLE {table};")
            print(f"  ✓ Truncated {table}")
        except Exception as e:
            print(f"  ⚠ Could not truncate {table}: {str(e)}")
        
    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    'load_enterprise_staging',
    default_args=default_args,
    description='Load raw enterprise department data into staging schema',
    schedule_interval=None,
    catchup=False,
    tags=['staging', 'enterprise', 'raw'],
) as dag:
    
    truncate_task = PythonOperator(
        task_id='truncate_staging_table',
        python_callable=truncate_staging_table,
    )
    
    load_task = PythonOperator(
        task_id='load_enterprise_staging_data',
        python_callable=load_enterprise_staging_data,
    )
    
    truncate_task >> load_task
