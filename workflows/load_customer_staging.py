from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import pickle
import os
import json
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
        '/opt/airflow/source/customer-management-department',
        '/home/vgr/dev/school/shopzada_3CSD_grp5/source/customer-management-department'
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
        if file_ext == '.json':
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Check if it's column-oriented or row-oriented
            if isinstance(data, dict) and all(isinstance(v, (list, dict)) for v in data.values()):
                df = pd.DataFrame(data)
                print(f"  → Loaded as column-oriented JSON")
            elif isinstance(data, list):
                df = pd.DataFrame(data)
                print(f"  → Loaded as row-oriented JSON")
            else:
                df = pd.DataFrame([data])
                print(f"  → Loaded as single JSON object")
                
        elif file_ext == '.csv':
            df = pd.read_csv(file_path)
            # Add raw_index as the row number (not from data columns)
            if 'raw_index' not in df.columns:
                df.insert(0, 'raw_index', range(len(df)))
            print(f"  → Loaded as CSV")
            
        elif file_ext in ['.pickle', '.pkl']:
            with open(file_path, 'rb') as f:
                df = pickle.load(f)
            print(f"  → Loaded as pickle")
            
        else:
            print(f"  ✗ Unsupported file type: {file_ext}")
            return None, None
        
        print(f"  → Shape: {df.shape}")
        print(f"  → Columns: {df.columns.tolist()}")
        return df, file_name
        
    except Exception as e:
        print(f"  ✗ Error loading file: {str(e)}")
        import traceback
        traceback.print_exc()
        return None, None

def determine_staging_table(file_name, columns):
    """Determine which staging table to use based on file name and columns."""
    file_name_lower = file_name.lower()
    columns_lower = set(col.lower() for col in columns)
    
    if 'credit_card' in file_name_lower or 'issuing_bank' in columns_lower:
        return 'cust_credit_cards_raw'
    elif 'job' in file_name_lower or ('job_title' in columns_lower and 'job_level' in columns_lower):
        return 'cust_user_jobs_raw'
    elif 'user' in file_name_lower or 'profile' in file_name_lower:
        return 'cust_user_profiles_raw'
    else:
        # Default to profiles if it has user_id
        if 'user_id' in columns_lower:
            return 'cust_user_profiles_raw'
    
    return None

def load_customer_staging_data(**context):
    """Load all customer management files into staging schema."""
    source_path = get_source_path()
    engine = get_db_engine()
    
    print("=" * 70)
    print("LOADING CUSTOMER MANAGEMENT DATA TO STAGING SCHEMA")
    print("=" * 70)
    
    # Discover all files
    all_files = list(Path(source_path).glob('*'))
    data_files = [f for f in all_files if f.is_file() and not f.name.startswith('.')]
    
    print(f"\nFound {len(data_files)} files to process:")
    for f in data_files:
        print(f"  - {f.name}")
    print()
    
    load_summary = []
    
    for file_path in data_files:
        df, file_name = detect_and_load_file(str(file_path))
        
        if df is None:
            print(f"⚠ Skipping {file_name}\n")
            continue
        
        # Determine target table
        table_name = determine_staging_table(file_name, df.columns)
        
        if not table_name:
            print(f"⚠ Could not determine staging table for {file_name}\n")
            continue
        
        print(f"  → Target table: staging.{table_name}")
        
        # Convert all columns to string (staging requirement)
        for col in df.columns:
            df[col] = df[col].astype(str)
        
        # Add metadata columns
        df['_source_file'] = file_name
        df['_ingested_at'] = pd.Timestamp.now()
        
        # Ensure column names match staging table exactly
        # Remove any columns that don't exist in staging
        expected_columns = {
            'cust_credit_cards_raw': ['user_id', 'name', 'credit_card_number', 'issuing_bank'],
            'cust_user_profiles_raw': ['user_id', 'name', 'gender', 'birthdate', 'street', 
                                       'city', 'state', 'country', 'device_address', 
                                       'creation_date', 'user_type'],
            'cust_user_jobs_raw': ['raw_index', 'user_id', 'name', 'job_title', 'job_level']
        }
        
        table_cols = expected_columns.get(table_name, [])
        # Keep only columns that exist in both df and expected columns, plus metadata
        available_cols = [col for col in table_cols if col in df.columns]
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
    for table in ['cust_credit_cards_raw', 'cust_user_profiles_raw', 'cust_user_jobs_raw']:
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
    """Truncate staging tables before loading fresh data."""
    engine = get_db_engine()
    conn = engine.raw_connection()
    cursor = conn.cursor()
    
    tables = [
        'staging.cust_credit_cards_raw',
        'staging.cust_user_profiles_raw',
        'staging.cust_user_jobs_raw'
    ]
    
    print("Truncating customer management staging tables...")
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
    'load_customer_staging',
    default_args=default_args,
    description='Load raw customer management data into staging schema (dynamic file type detection)',
    schedule_interval=None,
    catchup=False,
    tags=['staging', 'customer', 'raw', 'dynamic'],
) as dag:
    
    truncate_task = PythonOperator(
        task_id='truncate_staging_tables',
        python_callable=truncate_staging_tables,
    )
    
    load_task = PythonOperator(
        task_id='load_customer_staging_data',
        python_callable=load_customer_staging_data,
    )
    
    truncate_task >> load_task
