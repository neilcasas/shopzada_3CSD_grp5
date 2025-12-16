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


def populate_dim_campaign(**context):
    """Populate dim_campaign dimension table from ODS with surrogate keys."""
    engine = get_db_engine()

    print("=" * 70)
    print("POPULATING DW.DIM_CAMPAIGN (KIMBALL DIMENSION)")
    print("=" * 70)

    # Read from ODS
    print("Reading ods.core_campaigns ...")
    campaigns_df = pd.read_sql("""
        SELECT campaign_id, campaign_name, discount
        FROM ods.core_campaigns
        ORDER BY campaign_id
    """, engine)

    print(f"Rows read from ODS: {len(campaigns_df):,}")
    if len(campaigns_df) == 0:
        print("⚠ No rows in ods.core_campaigns - skipping dimension load (run populate_core_campaigns DAG first)")
        print("=" * 70)
        return

    # Basic cleaning
    for col in ['campaign_id', 'campaign_name']:
        if col in campaigns_df.columns:
            campaigns_df[col] = campaigns_df[col].astype(str).str.strip().replace({'nan': None, 'None': None})

    # Clean discount - convert to numeric
    if 'discount' in campaigns_df.columns:
        campaigns_df['discount'] = pd.to_numeric(campaigns_df['discount'], errors='coerce')

    # Ensure business key exists
    before = len(campaigns_df)
    campaigns_df = campaigns_df[campaigns_df['campaign_id'].notna() & (campaigns_df['campaign_id'].astype(str) != '')]
    print(f"Dropped {before - len(campaigns_df):,} rows without campaign_id")

    # Dedupe by campaign_id
    campaigns_df = campaigns_df.drop_duplicates(subset=['campaign_id'], keep='first')
    print(f"Unique campaigns to load: {len(campaigns_df):,}")

    # Helper function to convert NaN to None
    def to_python_value(val):
        if pd.isna(val):
            return None
        return val

    # Prepare records
    records = []
    for _, r in campaigns_df.iterrows():
        records.append((
            r['campaign_id'],
            r['campaign_name'],
            to_python_value(r['discount'])
        ))

    # Bulk load
    raw_conn = engine.raw_connection()
    cur = raw_conn.cursor()

    try:
        print("Truncating dw.dim_campaign and resetting identity...")
        cur.execute("TRUNCATE TABLE dw.dim_campaign RESTART IDENTITY CASCADE;")
        raw_conn.commit()
    except Exception as e:
        raw_conn.rollback()
        print(f"Warning truncating dim_campaign: {e}")

    # Insert in chunks
    batch_size = 2000
    inserted = 0
    insert_sql = """
        INSERT INTO dw.dim_campaign (campaign_id, campaign_name, discount)
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


def verify_dim_campaign(**context):
    """Verify the dimension table."""
    engine = get_db_engine()
    conn = engine.raw_connection()
    cur = conn.cursor()

    print("=" * 70)
    print("VERIFY DW.DIM_CAMPAIGN")
    print("=" * 70)

    cur.execute("SELECT COUNT(*) FROM dw.dim_campaign;")
    count = cur.fetchone()[0]
    print(f"dw.dim_campaign row count: {count:,}")

    cur.execute("""
        SELECT COUNT(*) as total,
               AVG(discount) as avg_discount,
               MIN(campaign_key) as min_sk, MAX(campaign_key) as max_sk
        FROM dw.dim_campaign;
    """)
    stats = cur.fetchone()
    if stats:
        print(f"Total campaigns: {stats[0]:,}, avg discount: {stats[1]}, surrogate keys: {stats[2]}..{stats[3]}")

    cur.execute("""
        SELECT campaign_key, campaign_id, campaign_name, discount
        FROM dw.dim_campaign
        ORDER BY campaign_key
        LIMIT 10;
    """)
    print("\nSample rows:")
    for r in cur.fetchall():
        print(f"  {r}")

    cur.close()
    conn.close()
    print("=" * 70)


with DAG(
    'populate_dim_campaign',
    default_args=default_args,
    description='Populate dw.dim_campaign from ods.core_campaigns (Kimball dimension)',
    schedule_interval=None,
    catchup=False,
    tags=['dw', 'kimball', 'dimension', 'campaign'],
) as dag:

    t_populate = PythonOperator(task_id='populate_dim_campaign', python_callable=populate_dim_campaign)
    t_verify = PythonOperator(task_id='verify_dim_campaign', python_callable=verify_dim_campaign)

    t_populate >> t_verify
