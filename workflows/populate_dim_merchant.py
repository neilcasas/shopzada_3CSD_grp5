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


def populate_dim_merchant(**context):
    """Populate dim_merchant dimension table from ODS with surrogate keys."""
    engine = get_db_engine()

    print("=" * 70)
    print("POPULATING DW.DIM_MERCHANT (KIMBALL DIMENSION)")
    print("=" * 70)

    # Read from ODS
    print("Reading ods.core_merchants ...")
    merchants_df = pd.read_sql("""
        SELECT merchant_id, name, city, state, country
        FROM ods.core_merchants
        ORDER BY merchant_id
    """, engine)

    print(f"Rows read from ODS: {len(merchants_df):,}")
    if len(merchants_df) == 0:
        print("⚠ No rows in ods.core_merchants - skipping dimension load (run populate_core_enterprise DAG first)")
        print("=" * 70)
        return

    # Basic cleaning
    for col in ['merchant_id', 'name', 'city', 'state', 'country']:
        if col in merchants_df.columns:
            merchants_df[col] = merchants_df[col].astype(str).str.strip().replace({'nan': None, 'None': None})

    # Ensure business key exists
    before = len(merchants_df)
    merchants_df = merchants_df[merchants_df['merchant_id'].notna() & (merchants_df['merchant_id'].astype(str) != '')]
    print(f"Dropped {before - len(merchants_df):,} rows without merchant_id")

    # Dedupe by merchant_id
    merchants_df = merchants_df.drop_duplicates(subset=['merchant_id'], keep='first')
    print(f"Unique merchants to load: {len(merchants_df):,}")

    # Prepare records
    records = []
    for _, r in merchants_df.iterrows():
        records.append((
            r['merchant_id'],
            r['name'],
            r['city'],
            r['state'],
            r['country']
        ))

    # Bulk load
    raw_conn = engine.raw_connection()
    cur = raw_conn.cursor()

    try:
        print("Truncating dw.dim_merchant and resetting identity...")
        cur.execute("TRUNCATE TABLE dw.dim_merchant RESTART IDENTITY CASCADE;")
        raw_conn.commit()
    except Exception as e:
        raw_conn.rollback()
        print(f"Warning truncating dim_merchant: {e}")

    # Insert in chunks
    batch_size = 2000
    inserted = 0
    insert_sql = """
        INSERT INTO dw.dim_merchant (merchant_id, name, city, state, country)
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


def verify_dim_merchant(**context):
    """Verify the dimension table."""
    engine = get_db_engine()
    conn = engine.raw_connection()
    cur = conn.cursor()

    print("=" * 70)
    print("VERIFY DW.DIM_MERCHANT")
    print("=" * 70)

    cur.execute("SELECT COUNT(*) FROM dw.dim_merchant;")
    count = cur.fetchone()[0]
    print(f"dw.dim_merchant row count: {count:,}")

    cur.execute("""
        SELECT COUNT(*) as total, COUNT(DISTINCT country) as unique_countries,
               MIN(merchant_key) as min_sk, MAX(merchant_key) as max_sk
        FROM dw.dim_merchant;
    """)
    stats = cur.fetchone()
    if stats:
        print(f"Total merchants: {stats[0]:,}, unique countries: {stats[1]}, surrogate keys: {stats[2]}..{stats[3]}")

    cur.execute("""
        SELECT merchant_key, merchant_id, name, city, state, country
        FROM dw.dim_merchant
        ORDER BY merchant_key
        LIMIT 10;
    """)
    print("\nSample rows:")
    for r in cur.fetchall():
        print(f"  {r}")

    cur.close()
    conn.close()
    print("=" * 70)


with DAG(
    'populate_dim_merchant',
    default_args=default_args,
    description='Populate dw.dim_merchant from ods.core_merchants (Kimball dimension)',
    schedule_interval=None,
    catchup=False,
    tags=['dw', 'kimball', 'dimension', 'merchant'],
) as dag:

    t_populate = PythonOperator(task_id='populate_dim_merchant', python_callable=populate_dim_merchant)
    t_verify = PythonOperator(task_id='verify_dim_merchant', python_callable=verify_dim_merchant)

    t_populate >> t_verify
