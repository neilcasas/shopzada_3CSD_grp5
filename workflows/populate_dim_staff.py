from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
import os
import psycopg2.extras

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
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER', 'airflow')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'airflow')}@"
        f"{os.getenv('POSTGRES_HOST', 'postgres')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/shopzada"
    )


def populate_dim_staff(**context):
    """Populate dim_staff dimension table from ODS with surrogate keys."""
    engine = get_db_engine()

    print("=" * 70)
    print("POPULATING DW.DIM_STAFF (KIMBALL DIMENSION)")
    print("=" * 70)

    # Read from ODS
    print("Reading ods.core_staff ...")
    staff_df = pd.read_sql("""
        SELECT staff_id, name, job_level
        FROM ods.core_staff
        ORDER BY staff_id
    """, engine)

    print(f"Rows read from ODS: {len(staff_df):,}")
    if len(staff_df) == 0:
        print("⚠ No rows in ods.core_staff - skipping dimension load (run populate_core_enterprise DAG first)")
        print("=" * 70)
        return

    # Basic cleaning
    for col in ['staff_id', 'name', 'job_level']:
        if col in staff_df.columns:
            staff_df[col] = staff_df[col].astype(str).str.strip().replace({'nan': None, 'None': None})

    # Ensure business key exists
    before = len(staff_df)
    staff_df = staff_df[staff_df['staff_id'].notna() & (staff_df['staff_id'].astype(str) != '')]
    print(f"Dropped {before - len(staff_df):,} rows without staff_id")

    # Dedupe by staff_id
    staff_df = staff_df.drop_duplicates(subset=['staff_id'], keep='first')
    print(f"Unique staff to load: {len(staff_df):,}")

    # Prepare records
    records = []
    for _, r in staff_df.iterrows():
        records.append((
            r['staff_id'],
            r['name'],
            r['job_level']
        ))

    # Bulk load
    raw_conn = engine.raw_connection()
    cur = raw_conn.cursor()

    try:
        print("Truncating dw.dim_staff and resetting identity...")
        cur.execute("TRUNCATE TABLE dw.dim_staff RESTART IDENTITY CASCADE;")
        raw_conn.commit()
    except Exception as e:
        raw_conn.rollback()
        print(f"Warning truncating dim_staff: {e}")

    # Insert in chunks
    batch_size = 2000
    inserted = 0
    insert_sql = """
        INSERT INTO dw.dim_staff (staff_id, name, job_level)
        VALUES %s
    """
    try:
        for i in range(0, len(records), batch_size):
            chunk = records[i:i + batch_size]
            psycopg2.extras.execute_values(cur, insert_sql, chunk, page_size=batch_size)
            raw_conn.commit()
            inserted += len(chunk)
            print(f"  Inserted {inserted:,}/{len(records):,}")
    except Exception as e:
        raw_conn.rollback()
        print(f"Error during bulk insert: {e}")
        raise

    cur.close()
    raw_conn.close()

    print(f"\nDone. Inserted: {inserted:,}")
    print("=" * 70)


def verify_dim_staff(**context):
    """Verify the dimension table."""
    engine = get_db_engine()
    conn = engine.raw_connection()
    cur = conn.cursor()

    print("=" * 70)
    print("VERIFY DW.DIM_STAFF")
    print("=" * 70)

    cur.execute("SELECT COUNT(*) FROM dw.dim_staff;")
    count = cur.fetchone()[0]
    print(f"dw.dim_staff row count: {count:,}")

    cur.execute("""
        SELECT COUNT(*) as total, COUNT(DISTINCT job_level) as unique_levels,
               MIN(staff_key) as min_sk, MAX(staff_key) as max_sk
        FROM dw.dim_staff;
    """)
    stats = cur.fetchone()
    if stats:
        print(f"Total staff: {stats[0]:,}, unique job levels: {stats[1]}, surrogate keys: {stats[2]}..{stats[3]}")

    cur.execute("""
        SELECT staff_key, staff_id, name, job_level
        FROM dw.dim_staff
        ORDER BY staff_key
        LIMIT 10;
    """)
    print("\nSample rows:")
    for r in cur.fetchall():
        print(f"  {r}")

    cur.close()
    conn.close()
    print("=" * 70)


with DAG(
    'populate_dim_staff',
    default_args=default_args,
    description='Populate dw.dim_staff from ods.core_staff (Kimball dimension)',
    schedule_interval=None,
    catchup=False,
    tags=['dw', 'kimball', 'dimension', 'staff'],
) as dag:

    t_populate = PythonOperator(task_id='populate_dim_staff', python_callable=populate_dim_staff)
    t_verify = PythonOperator(task_id='verify_dim_staff', python_callable=verify_dim_staff)

    t_populate >> t_verify
