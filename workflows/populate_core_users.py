from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import pickle
import hashlib
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

def detect_file_type_and_load(file_path):
    """Detect file type and load data accordingly."""
    file_ext = Path(file_path).suffix.lower()
    file_name = Path(file_path).stem
    
    print(f"Processing file: {file_name}{file_ext}")
    
    try:
        if file_ext == '.json':
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Check if it's column-oriented or row-oriented
            if isinstance(data, dict) and all(isinstance(v, (list, dict)) for v in data.values()):
                # Column-oriented - check if values are dicts (nested) or lists
                first_val = next(iter(data.values()))
                if isinstance(first_val, dict):
                    # Nested dict structure - need to flatten
                    df = pd.DataFrame(data)
                    print(f"  → Loaded as column-oriented JSON with nested structure")
                else:
                    df = pd.DataFrame(data)
                    print(f"  → Loaded as column-oriented JSON")
            elif isinstance(data, list):
                df = pd.DataFrame(data)
                print(f"  → Loaded as row-oriented JSON")
            else:
                df = pd.DataFrame([data])
                print(f"  → Loaded as single JSON object")
                
        elif file_ext == '.csv':
            # Try with index column first
            try:
                df = pd.read_csv(file_path, index_col=0)
            except:
                df = pd.read_csv(file_path)
            print(f"  → Loaded as CSV")
            
        elif file_ext in ['.pickle', '.pkl']:
            with open(file_path, 'rb') as f:
                df = pickle.load(f)
            print(f"  → Loaded as pickle")
            
        elif file_ext in ['.xlsx', '.xls']:
            df = pd.read_excel(file_path)
            print(f"  → Loaded as Excel")
            
        elif file_ext == '.parquet':
            df = pd.read_parquet(file_path)
            print(f"  → Loaded as Parquet")
            
        else:
            print(f"  ✗ Unsupported file type: {file_ext}")
            return None
        
        # Ensure user_id is string if it exists
        if 'user_id' in df.columns:
            df['user_id'] = df['user_id'].astype(str)
        
        print(f"  → Shape: {df.shape}")
        print(f"  → Columns: {df.columns.tolist()}")
        return df
        
    except Exception as e:
        print(f"  ✗ Error loading file: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def discover_and_extract_files(**context):
    """Discover all files in source directory and extract them."""
    source_path = get_source_path()
    
    print(f"Discovering files in: {source_path}")
    print("=" * 60)
    
    all_files = list(Path(source_path).glob('*'))
    data_files = [f for f in all_files if f.is_file() and not f.name.startswith('.')]
    
    print(f"Found {len(data_files)} files to process:")
    for f in data_files:
        print(f"  - {f.name}")
    print("=" * 60)
    
    # Extract all files and categorize by content
    extracted_data = {}
    
    for file_path in data_files:
        df = detect_file_type_and_load(str(file_path))
        
        if df is not None:
            file_name = file_path.stem
            
            # Identify content type based on columns
            columns = set(df.columns.str.lower())
            
            # Categorize based on column patterns
            if 'user_id' in columns:
                if 'credit_card' in file_name.lower() or 'issuing_bank' in columns:
                    category = 'credit_cards'
                    # Tokenize credit card numbers if present
                    if 'credit_card_number' in df.columns:
                        df['credit_card_token'] = df['credit_card_number'].apply(
                            lambda x: hashlib.sha256(str(x).encode()).hexdigest() if pd.notna(x) else None
                        )
                        df = df.drop(columns=['credit_card_number'])
                        print(f"  → Tokenized credit card numbers")
                elif 'job' in file_name.lower() or 'job_title' in columns or 'job_level' in columns:
                    category = 'jobs'
                else:
                    category = 'users'
                
                print(f"  → Categorized as: {category}")
                
                # Store or merge with existing category
                if category in extracted_data:
                    # Merge with existing data
                    existing_df = extracted_data[category]
                    merged = pd.concat([existing_df, df], ignore_index=True)
                    extracted_data[category] = merged
                    print(f"  → Merged with existing {category} data")
                else:
                    extracted_data[category] = df
            else:
                print(f"  ⚠ Skipping - no user_id column found")
        
        print()
    
    # Push categorized data to XCom - use orient='split' to preserve dtypes better
    for category, df in extracted_data.items():
        print(f"Final {category} data: {df.shape}")
        # Convert to split format which handles complex types better
        context['ti'].xcom_push(key=category, value=df.to_json(orient='split', date_format='iso'))
    
    # Push metadata about what was extracted
    context['ti'].xcom_push(key='categories', value=list(extracted_data.keys()))

def clean_and_merge_data(**context):
    """Clean data and merge all sources."""
    ti = context['ti']
    
    # Get categories that were extracted
    categories = ti.xcom_pull(key='categories', task_ids='discover_and_extract')
    print(f"Processing categories: {categories}")
    
    # Retrieve data from XCom based on what was found
    dataframes = {}
    for category in categories:
        data_json = ti.xcom_pull(key=category, task_ids='discover_and_extract')
        if data_json:
            dataframes[category] = pd.read_json(data_json, orient='split')
            print(f"{category} shape: {dataframes[category].shape}")
    
    # Start with base user data
    if 'users' in dataframes:
        merged = dataframes['users'].copy()
        # Ensure user_id is string
        if 'user_id' in merged.columns:
            merged['user_id'] = merged['user_id'].astype(str)
    else:
        print("⚠ No base user data found, using empty dataframe")
        merged = pd.DataFrame()
        return
    
    # Merge job data if available
    if 'jobs' in dataframes:
        jobs_df = dataframes['jobs'].copy()
        # Ensure user_id is string
        if 'user_id' in jobs_df.columns:
            jobs_df['user_id'] = jobs_df['user_id'].astype(str)
        
        job_cols = [col for col in jobs_df.columns if col in ['user_id', 'job_title', 'job_level']]
        if len(job_cols) > 1:  # Must have user_id plus at least one other column
            merged = merged.merge(
                jobs_df[job_cols],
                on='user_id',
                how='left',
                suffixes=('', '_job')
            )
            print(f"Merged job data: {len(job_cols)} columns")
    
    # Merge credit card data if available
    if 'credit_cards' in dataframes:
        cc_df = dataframes['credit_cards'].copy()
        # Ensure user_id is string
        if 'user_id' in cc_df.columns:
            cc_df['user_id'] = cc_df['user_id'].astype(str)
        
        cc_cols = [col for col in cc_df.columns if col in ['user_id', 'issuing_bank', 'credit_card_token']]
        if len(cc_cols) > 1:
            merged = merged.merge(
                cc_df[cc_cols],
                on='user_id',
                how='left',
                suffixes=('', '_cc')
            )
            print(f"Merged credit card data: {len(cc_cols)} columns")
    
    print(f"Merged data shape: {merged.shape}")
    
    # Remove duplicates before cleaning
    merged = merged.drop_duplicates(subset=['user_id'])
    print(f"After deduplication: {len(merged)} records")
    
    # Clean anomalies
    print("Cleaning anomalies...")
    
    # 1. Remove records with missing critical fields
    initial_count = len(merged)
    merged = merged[merged['user_id'].notna()]
    if 'name' in merged.columns:
        merged = merged[merged['name'].notna()]
    print(f"Removed {initial_count - len(merged)} records with missing critical fields")
    
    # 2. Standardize gender values
    if 'gender' in merged.columns:
        merged['gender'] = merged['gender'].astype(str).str.strip().str.title()
        valid_genders = ['Male', 'Female', 'Other', 'Non-Binary', 'Prefer Not To Say']
        merged.loc[~merged['gender'].isin(valid_genders), 'gender'] = None
    
    # 3. Fix birthdate anomalies
    if 'birthdate' in merged.columns:
        merged['birthdate'] = pd.to_datetime(merged['birthdate'], errors='coerce')
        merged.loc[merged['birthdate'] > datetime.now(), 'birthdate'] = None
        merged.loc[merged['birthdate'] < pd.Timestamp('1900-01-01'), 'birthdate'] = None
    
    # 4. Fix creation_date anomalies
    if 'creation_date' in merged.columns:
        merged['creation_date'] = pd.to_datetime(merged['creation_date'], errors='coerce')
        merged.loc[merged['creation_date'] > datetime.now(), 'creation_date'] = None
    
    # 5. Standardize user_type
    if 'user_type' in merged.columns:
        merged['user_type'] = merged['user_type'].astype(str).str.strip().str.title()
    
    # 6. Handle Students (no job level)
    if 'job_title' in merged.columns and 'job_level' in merged.columns:
        student_mask = merged['job_title'] == 'Student'
        merged.loc[student_mask, 'job_level'] = 'N/A'
    
    # 7. Remove any remaining duplicates
    merged = merged.drop_duplicates(subset=['user_id'], keep='first')
    
    print(f"Final cleaned data shape: {merged.shape}")
    print(f"Columns: {merged.columns.tolist()}")
    
    # Store in XCom
    context['ti'].xcom_push(key='cleaned_data', value=merged.to_json(orient='records', date_format='iso'))

def load_to_postgres(**context):
    """Load cleaned data into ods.core_users table."""
    ti = context['ti']
    
    # Retrieve cleaned data
    cleaned_data_json = ti.xcom_pull(key='cleaned_data', task_ids='clean_and_merge')
    
    if not cleaned_data_json:
        print("No cleaned data to load")
        return
    
    cleaned_data = pd.read_json(cleaned_data_json)
    
    print(f"Loading {len(cleaned_data)} records to PostgreSQL")
    
    # Database connection
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=os.getenv('POSTGRES_PORT', '5432'),
        database='shopzada',
        user=os.getenv('POSTGRES_USER', 'airflow'),
        password=os.getenv('POSTGRES_PASSWORD', 'airflow')
    )
    
    cursor = conn.cursor()
    
    # Truncate table first
    print("Truncating ods.core_users table...")
    cursor.execute("TRUNCATE TABLE ods.core_users CASCADE;")
    
    # Get the columns available in the table
    cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'ods' AND table_name = 'core_users'
        ORDER BY ordinal_position;
    """)
    db_columns = [row[0] for row in cursor.fetchall()]
    print(f"Database columns: {db_columns}")
    
    # Map dataframe columns to database columns
    available_columns = [col for col in db_columns if col in cleaned_data.columns]
    print(f"Columns to insert: {available_columns}")
    
    # Insert data
    insert_count = 0
    error_count = 0
    
    for _, row in cleaned_data.iterrows():
        try:
            # Build dynamic INSERT statement
            placeholders = ', '.join(['%s'] * len(available_columns))
            columns_str = ', '.join(available_columns)
            
            values = tuple(row.get(col) for col in available_columns)
            
            cursor.execute(
                f"INSERT INTO ods.core_users ({columns_str}) VALUES ({placeholders})",
                values
            )
            insert_count += 1
        except Exception as e:
            error_count += 1
            if error_count <= 5:  # Only print first 5 errors
                print(f"Error inserting row {row.get('user_id')}: {str(e)}")
    
    conn.commit()
    
    # Verify the load
    cursor.execute("SELECT COUNT(*) FROM ods.core_users;")
    final_count = cursor.fetchone()[0]
    
    print(f"Successfully inserted {insert_count} records")
    print(f"Failed inserts: {error_count}")
    print(f"Final count in ods.core_users: {final_count}")
    
    cursor.close()
    conn.close()

with DAG(
    'populate_core_users',
    default_args=default_args,
    description='Dynamically extract customer management data, clean anomalies, and load to ods.core_users',
    schedule_interval=None,
    catchup=False,
    tags=['etl', 'ods', 'customer', 'dynamic'],
) as dag:
    
    discover_task = PythonOperator(
        task_id='discover_and_extract',
        python_callable=discover_and_extract_files,
    )
    
    clean_and_merge_task = PythonOperator(
        task_id='clean_and_merge',
        python_callable=clean_and_merge_data,
    )
    
    load_to_postgres_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
    )
    
    # Set dependencies
    discover_task >> clean_and_merge_task >> load_to_postgres_task
