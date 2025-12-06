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


def ensure_dim_date(**context):
    """Ensure dim_date has necessary date records."""
    engine = get_db_engine()
    conn = engine.raw_connection()
    cur = conn.cursor()

    print("=" * 70)
    print("ENSURING DIM_DATE IS POPULATED")
    print("=" * 70)

    # Check if dim_date has data
    cur.execute("SELECT COUNT(*) FROM dw.dim_date;")
    count = cur.fetchone()[0]

    if count == 0:
        print("dim_date is empty. Populating with dates from 2019-01-01 to 2025-12-31...")

        start_date = datetime(2019, 1, 1)
        end_date = datetime(2025, 12, 31)
        current_date = start_date

        records = []
        while current_date <= end_date:
            date_key = int(current_date.strftime('%Y%m%d'))
            day_of_week = current_date.weekday() + 1
            month = current_date.month
            month_names = ['January', 'February', 'March', 'April', 'May', 'June',
                           'July', 'August', 'September', 'October', 'November', 'December']
            month_name = month_names[month - 1]
            quarter = (month - 1) // 3 + 1
            year = current_date.year
            is_weekend = day_of_week >= 6

            records.append((
                date_key,
                current_date.date(),
                day_of_week,
                month,
                month_name,
                quarter,
                year,
                is_weekend
            ))

            current_date += timedelta(days=1)

        insert_sql = """
            INSERT INTO dw.dim_date (date_key, date, day_of_week, month, month_name, quarter, year, is_weekend)
            VALUES %s
            ON CONFLICT (date_key) DO NOTHING
        """
        psycopg2.extras.execute_values(cur, insert_sql, records, page_size=1000)
        conn.commit()
        print(f"  ✓ Inserted {len(records):,} dates")
    else:
        print(f"dim_date already has {count:,} records")

    cur.close()
    conn.close()
    print("=" * 70)


def populate_fact_orders(**context):
    """Populate fact_orders from ODS with proper dimension lookups."""
    engine = get_db_engine()

    print("=" * 70)
    print("POPULATING DW.FACT_ORDERS (KIMBALL FACT TABLE)")
    print("=" * 70)

    conn = engine.raw_connection()
    cur = conn.cursor()

    # Truncate fact table
    print("Truncating dw.fact_orders...")
    cur.execute("TRUNCATE TABLE dw.fact_orders RESTART IDENTITY;")
    conn.commit()

    # Get dimension lookups
    print("\nLoading dimension lookups...")

    cur.execute("SELECT user_id, user_key FROM dw.dim_user;")
    user_lookup = {row[0]: row[1] for row in cur.fetchall()}
    print(f"  dim_user: {len(user_lookup):,} entries")

    cur.execute("SELECT merchant_id, merchant_key FROM dw.dim_merchant;")
    merchant_lookup = {row[0]: row[1] for row in cur.fetchall()}
    print(f"  dim_merchant: {len(merchant_lookup):,} entries")

    cur.execute("SELECT staff_id, staff_key FROM dw.dim_staff;")
    staff_lookup = {row[0]: row[1] for row in cur.fetchall()}
    print(f"  dim_staff: {len(staff_lookup):,} entries")

    # Get total orders
    cur.execute("SELECT COUNT(*) FROM ods.core_orders;")
    total_orders = cur.fetchone()[0]
    print(f"\nTotal orders to process: {total_orders:,}")

    if total_orders == 0:
        print("  ⚠ No orders in ODS. Run populate_core_operations DAG first.")
        cur.close()
        conn.close()
        return

    # Process in chunks
    chunk_size = 50000
    inserted = 0
    skipped = 0

    for offset in range(0, total_orders, chunk_size):
        print(f"\nProcessing chunk {offset:,} to {min(offset + chunk_size, total_orders):,}")

        orders_df = pd.read_sql(f"""
            SELECT 
                order_id,
                user_id,
                merchant_id,
                staff_id,
                transaction_date,
                estimated_arrival,
                delay_in_days,
                is_delayed
            FROM ods.core_orders
            ORDER BY order_id
            LIMIT {chunk_size} OFFSET {offset}
        """, engine)

        records = []
        for _, row in orders_df.iterrows():
            # Get surrogate keys
            user_key = user_lookup.get(row['user_id'])
            merchant_key = merchant_lookup.get(row['merchant_id']) if pd.notna(row.get('merchant_id')) else None
            staff_key = staff_lookup.get(row['staff_id']) if pd.notna(row.get('staff_id')) else None

            # Get date keys
            order_date_key = None
            estimated_arrival_date_key = None

            if pd.notna(row.get('transaction_date')):
                try:
                    dt = pd.to_datetime(row['transaction_date'])
                    order_date_key = int(dt.strftime('%Y%m%d'))
                except:
                    pass

            if pd.notna(row.get('estimated_arrival')):
                try:
                    dt = pd.to_datetime(row['estimated_arrival'])
                    estimated_arrival_date_key = int(dt.strftime('%Y%m%d'))
                except:
                    pass

            # Get delay info
            delay_in_days = int(row['delay_in_days']) if pd.notna(row.get('delay_in_days')) else 0
            is_delayed = bool(row['is_delayed']) if pd.notna(row.get('is_delayed')) else False

            records.append((
                row['order_id'],
                order_date_key,
                estimated_arrival_date_key,
                user_key,
                merchant_key,
                staff_key,
                delay_in_days,
                is_delayed
            ))

        # Bulk insert
        if records:
            insert_sql = """
                INSERT INTO dw.fact_orders 
                (order_id, order_date_key, estimated_arrival_date_key, user_key, merchant_key, staff_key, 
                 delay_in_days, is_delayed)
                VALUES %s
            """
            try:
                psycopg2.extras.execute_values(cur, insert_sql, records, page_size=5000)
                conn.commit()
                inserted += len(records)
                print(f"  ✓ Inserted {len(records):,} records (total: {inserted:,})")
            except Exception as e:
                conn.rollback()
                print(f"  ✗ Error: {e}")
                skipped += len(records)

    cur.close()
    conn.close()

    print(f"\n✓ Total inserted: {inserted:,}")
    if skipped > 0:
        print(f"  ⚠ Skipped: {skipped:,}")
    print("=" * 70)


def verify_fact_orders(**context):
    """Verify the fact table."""
    engine = get_db_engine()
    conn = engine.raw_connection()
    cur = conn.cursor()

    print("=" * 70)
    print("VERIFY DW.FACT_ORDERS")
    print("=" * 70)

    cur.execute("SELECT COUNT(*) FROM dw.fact_orders;")
    count = cur.fetchone()[0]
    print(f"dw.fact_orders row count: {count:,}")

    cur.execute("""
        SELECT 
            COUNT(*) as total_orders,
            SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) as delayed_orders,
            AVG(delay_in_days) as avg_delay,
            MAX(delay_in_days) as max_delay
        FROM dw.fact_orders;
    """)
    stats = cur.fetchone()
    if stats and stats[0]:
        print(f"Total orders: {stats[0]:,}")
        print(f"Delayed orders: {stats[1]:,} ({stats[1]/stats[0]*100:.1f}%)" if stats[1] else "Delayed orders: 0")
        print(f"Average delay: {stats[2]:.1f} days" if stats[2] else "Avg delay: 0 days")
        print(f"Max delay: {stats[3]} days" if stats[3] else "Max delay: 0 days")

    cur.execute("""
        SELECT order_key, order_id, order_date_key, user_key, merchant_key, delay_in_days, is_delayed
        FROM dw.fact_orders
        ORDER BY order_key
        LIMIT 5;
    """)
    print("\nSample rows:")
    for r in cur.fetchall():
        print(f"  {r}")

    cur.close()
    conn.close()
    print("=" * 70)


with DAG(
    'populate_fact_orders',
    default_args=default_args,
    description='Populate dw.fact_orders from ODS (Kimball fact table)',
    schedule_interval=None,
    catchup=False,
    tags=['dw', 'kimball', 'fact', 'orders'],
) as dag:

    t_dim_date = PythonOperator(task_id='ensure_dim_date', python_callable=ensure_dim_date)
    t_populate = PythonOperator(task_id='populate_fact_orders', python_callable=populate_fact_orders)
    t_verify = PythonOperator(task_id='verify_fact_orders', python_callable=verify_fact_orders)

    t_dim_date >> t_populate >> t_verify
