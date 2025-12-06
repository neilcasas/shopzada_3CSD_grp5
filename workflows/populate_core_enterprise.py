from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_db_engine():
    """Create SQLAlchemy engine for database connection."""
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER', 'airflow')}:" +
        f"{os.getenv('POSTGRES_PASSWORD', 'airflow')}@" +
        f"{os.getenv('POSTGRES_HOST', 'postgres')}:" +
        f"{os.getenv('POSTGRES_PORT', '5432')}/shopzada"
    )

def extract_from_staging(**context):
    engine = get_db_engine()
    print("=" * 70)
    print("EXTRACTING ENTERPRISE DATA FROM STAGING SCHEMA")
    print("=" * 70)

    try:
        merchants = pd.read_sql("SELECT * FROM staging.ent_merchants_raw", engine)
        staff = pd.read_sql("SELECT * FROM staging.ent_staff_raw", engine)

        print(f"✓ Loaded {len(merchants)} merchants")
        print(f"✓ Loaded {len(staff)} staff")

        context['ti'].xcom_push(key='merchants', value=merchants.to_json(orient='split'))
        context['ti'].xcom_push(key='staff', value=staff.to_json(orient='split'))

    except Exception as e:
        print(f"✗ Error extracting from staging: {str(e)}")
        raise

def transform_and_clean(**context):
    ti = context['ti']
    merchants = pd.read_json(ti.xcom_pull(key='merchants', task_ids='extract_from_staging'), orient='split')
    staff = pd.read_json(ti.xcom_pull(key='staff', task_ids='extract_from_staging'), orient='split')

    print("=" * 70)
    print("TRANSFORMING AND CLEANING ENTERPRISE DATA")
    print("=" * 70)

    # Deduplicate
    merchants = merchants.drop_duplicates(subset=['merchant_id'], keep='first')
    staff = staff.drop_duplicates(subset=['staff_id'], keep='first')

    # Convert creation_date to timestamp
    if 'creation_date' in merchants.columns:
        merchants['creation_date'] = pd.to_datetime(merchants['creation_date'], errors='coerce')
    if 'creation_date' in staff.columns:
        staff['creation_date'] = pd.to_datetime(staff['creation_date'], errors='coerce')

    # Standardize job_level
    if 'job_level' in staff.columns:
        staff['job_level'] = staff['job_level'].astype(str).str.strip().str.title()
        staff['job_level'] = staff['job_level'].replace('Nan', None)

    # Drop metadata columns
    metadata_cols = ['_source_file', '_ingested_at', 'raw_index']
    merchants = merchants.drop(columns=[c for c in metadata_cols if c in merchants.columns], errors='ignore')
    staff = staff.drop(columns=[c for c in metadata_cols if c in staff.columns], errors='ignore')

    print(f"✓ Merchants cleaned: {merchants.shape}")
    print(f"✓ Staff cleaned: {staff.shape}")

    ti.xcom_push(key='clean_merchants', value=merchants.to_json(orient='split', date_format='iso'))
    ti.xcom_push(key='clean_staff', value=staff.to_json(orient='split', date_format='iso'))

def load_to_ods(table, xcom_key, **context):
    ti = context['ti']
    engine = get_db_engine()
    df = pd.read_json(ti.xcom_pull(key=xcom_key, task_ids='transform_and_clean'), orient='split')

    print("=" * 70)
    print(f"LOADING DATA TO ODS.CORE_{table.upper()}")
    print("=" * 70)
    print(f"Loading {len(df)} records")

    conn = engine.raw_connection()
    cursor = conn.cursor()

    # Truncate before load
    cursor.execute(f"TRUNCATE TABLE ods.core_{table} CASCADE;")
    conn.commit()

    # Get DB columns
    cursor.execute(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'ods' AND table_name = 'core_{table}'
        ORDER BY ordinal_position;
    """)
    db_columns = [row[0] for row in cursor.fetchall()]
    available_cols = [col for col in db_columns if col in df.columns]

    print(f"Columns to insert: {available_cols}")

    insert_count = 0
    error_count = 0
    for _, row in df.iterrows():
        try:
            placeholders = ', '.join(['%s'] * len(available_cols))
            columns_str = ', '.join(available_cols)
            values = tuple(row.get(col) for col in available_cols)
            cursor.execute(
                f"INSERT INTO ods.core_{table} ({columns_str}) VALUES ({placeholders})",
                values
            )
            insert_count += 1
        except Exception as e:
            error_count += 1
            if error_count <= 5:
                print(f"  ✗ Error inserting row: {str(e)}")

    conn.commit()
    print(f"✓ Successfully inserted {insert_count} records into ods.core_{table}")
    if error_count > 0:
        print(f"  ⚠ Failed inserts: {error_count}")

    cursor.close()
    conn.close()

with DAG(
    'populate_core_enterprise',
    default_args=default_args,
    description='Extract enterprise data from staging, transform, and load to ods.core_merchants and ods.core_staff',
    schedule_interval=None,
    catchup=False,
    tags=['etl', 'ods', 'enterprise', 'staging-to-ods'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract_from_staging',
        python_callable=extract_from_staging,
    )

    transform_task = PythonOperator(
        task_id='transform_and_clean',
        python_callable=transform_and_clean,
    )

    load_merchants = PythonOperator(
        task_id='load_merchants',
        python_callable=load_to_ods,
        op_kwargs={'table': 'merchants', 'xcom_key': 'clean_merchants'},
    )

    load_staff = PythonOperator(
        task_id='load_staff',
        python_callable=load_to_ods,
        op_kwargs={'table': 'staff', 'xcom_key': 'clean_staff'},
    )

    # Dependencies
    extract_task >> transform_task >> [load_merchants, load_staff]