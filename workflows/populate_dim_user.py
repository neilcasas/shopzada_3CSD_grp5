from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
import os
import psycopg2
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
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER','airflow')}:"
        f"{os.getenv('POSTGRES_PASSWORD','airflow')}@"
        f"{os.getenv('POSTGRES_HOST','postgres')}:"
        f"{os.getenv('POSTGRES_PORT','5432')}/shopzada"
    )

def populate_dim_user(**context):
    engine = get_db_engine()

    print("=" * 70)
    print("POPULATING DW.DIM_USER (KIMBALL DIMENSION)")
    print("=" * 70)

    # Read from ODS
    print("Reading ods.core_users ...")
    users_df = pd.read_sql("""
        SELECT user_id, name, gender, birthdate, street, city, state, country,
               device_address, creation_date, user_type, job_title, job_level
        FROM ods.core_users
        ORDER BY user_id
    """, engine)

    print(f"Rows read from ODS: {len(users_df):,}")
    if len(users_df) == 0:
        raise ValueError("No rows in ods.core_users. Run populate_core_users DAG first.")

    # Basic cleaning (consistent with your populate_core_users)
    cols_to_trim = ['user_id','name','gender','street','city','state','country','user_type','job_title','job_level']
    for c in cols_to_trim:
        if c in users_df.columns:
            users_df[c] = users_df[c].astype(str).str.strip().replace({'nan': None, 'None': None})

    # Parse birthdate -> date only
    if 'birthdate' in users_df.columns:
        users_df['birthdate'] = pd.to_datetime(users_df['birthdate'], errors='coerce').dt.date

    # Ensure business key exists
    before = len(users_df)
    users_df = users_df[users_df['user_id'].notna() & (users_df['user_id'].astype(str) != '')]
    print(f"Dropped {before - len(users_df):,} rows without user_id")

    # Dedupe by user_id (keep first)
    users_df = users_df.drop_duplicates(subset=['user_id'], keep='first')
    print(f"Unique users to load: {len(users_df):,}")

    # Prepare rows for bulk insert (order must match insert columns)
    insert_cols = [
        'user_id','name','gender','birthdate','street','city','state','country',
        'user_type','job_title','job_level'
    ]
    # Ensure columns exist, add None otherwise
    for c in insert_cols:
        if c not in users_df.columns:
            users_df[c] = None

    records = []
    for _, r in users_df.iterrows():
        records.append((
            r['user_id'],
            r['name'],
            r['gender'],
            r['birthdate'],   # python date or None
            r['street'],
            r['city'],
            r['state'],
            r['country'],
            r['user_type'],
            r['job_title'],
            r['job_level']
        ))

    # Bulk load using psycopg2 execute_values for performance
    raw_conn = engine.raw_connection()
    cur = raw_conn.cursor()

    try:
        print("Truncating dw.dim_user and resetting identity...")
        cur.execute("TRUNCATE TABLE dw.dim_user RESTART IDENTITY CASCADE;")
        raw_conn.commit()
    except Exception as e:
        raw_conn.rollback()
        print(f"Warning truncating dim_user: {e}")

    # Insert in chunks
    batch_size = 2000
    inserted = 0
    errors = 0
    insert_sql = f"""
        INSERT INTO dw.dim_user ({', '.join(insert_cols)})
        VALUES %s
    """
    try:
        for i in range(0, len(records), batch_size):
            chunk = records[i:i+batch_size]
            psycopg2.extras.execute_values(cur, insert_sql, chunk, template=None, page_size=batch_size)
            raw_conn.commit()
            inserted += len(chunk)
            print(f"  Inserted {inserted:,}/{len(records):,}")
    except Exception as e:
        raw_conn.rollback()
        errors += 1
        print(f"Error during bulk insert: {e}")

    cur.close()
    raw_conn.close()

    print(f"\nDone. Inserted: {inserted:,} (errors: {errors})")
    print("=" * 70)

def verify_dim_user(**context):
    engine = get_db_engine()
    conn = engine.raw_connection()
    cur = conn.cursor()

    print("=" * 70)
    print("VERIFY DW.DIM_USER")
    print("=" * 70)

    cur.execute("SELECT COUNT(*) FROM dw.dim_user;")
    count = cur.fetchone()[0]
    print(f"dw.dim_user row count: {count:,}")

    cur.execute("""
        SELECT COUNT(*) as total_users, COUNT(DISTINCT city) as unique_cities,
               MIN(user_key) as min_sk, MAX(user_key) as max_sk
        FROM dw.dim_user;
    """)
    stats = cur.fetchone()
    if stats:
        print(f"Total users: {stats[0]:,}, unique cities: {stats[1]}, surrogate keys: {stats[2]}..{stats[3]}")

    cur.execute("""
        SELECT user_key, user_id, name, city, job_title
        FROM dw.dim_user
        ORDER BY user_key
        LIMIT 10;
    """)
    print("\nSample rows:")
    for r in cur.fetchall():
        print(r)

    cur.close()
    conn.close()
    print("=" * 70)

with DAG(
    'populate_dim_user',
    default_args=default_args,
    description='Populate dw.dim_user from ods.core_users (Kimball dimension)',
    schedule_interval=None,
    catchup=False,
    tags=['dw','kimball','dimension','user'],
) as dag:

    t_populate = PythonOperator(task_id='populate_dim_user', python_callable=populate_dim_user)
    t_verify = PythonOperator(task_id='verify_dim_user', python_callable=verify_dim_user)

    t_populate >> t_verify
