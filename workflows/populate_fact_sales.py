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

        # Generate dates
        start_date = datetime(2019, 1, 1)
        end_date = datetime(2025, 12, 31)
        current_date = start_date

        records = []
        while current_date <= end_date:
            date_key = int(current_date.strftime('%Y%m%d'))
            day_of_week = current_date.weekday() + 1  # 1=Monday, 7=Sunday
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

        # Insert dates
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


def populate_fact_sales(**context):
    """Populate fact_sales from ODS with proper dimension lookups."""
    engine = get_db_engine()

    print("=" * 70)
    print("POPULATING DW.FACT_SALES (KIMBALL FACT TABLE)")
    print("=" * 70)

    conn = engine.raw_connection()
    cur = conn.cursor()

    # Truncate fact table
    print("Truncating dw.fact_sales...")
    cur.execute("TRUNCATE TABLE dw.fact_sales RESTART IDENTITY;")
    conn.commit()

    # Get dimension lookups
    print("\nLoading dimension lookups...")

    # User lookup
    cur.execute("SELECT user_id, user_key FROM dw.dim_user;")
    user_lookup = {row[0]: row[1] for row in cur.fetchall()}
    print(f"  dim_user: {len(user_lookup):,} entries")

    # Product lookup
    cur.execute("SELECT product_id, product_key FROM dw.dim_product;")
    product_lookup = {row[0]: row[1] for row in cur.fetchall()}
    print(f"  dim_product: {len(product_lookup):,} entries")

    # Merchant lookup
    cur.execute("SELECT merchant_id, merchant_key FROM dw.dim_merchant;")
    merchant_lookup = {row[0]: row[1] for row in cur.fetchall()}
    print(f"  dim_merchant: {len(merchant_lookup):,} entries")

    # Staff lookup
    cur.execute("SELECT staff_id, staff_key FROM dw.dim_staff;")
    staff_lookup = {row[0]: row[1] for row in cur.fetchall()}
    print(f"  dim_staff: {len(staff_lookup):,} entries")

    # Campaign lookup
    cur.execute("SELECT campaign_id, campaign_key FROM dw.dim_campaign;")
    campaign_lookup = {row[0]: row[1] for row in cur.fetchall()}
    print(f"  dim_campaign: {len(campaign_lookup):,} entries")

    # Get total line items
    cur.execute("SELECT COUNT(*) FROM ods.core_line_items;")
    total_items = cur.fetchone()[0]
    print(f"\nTotal line items to process: {total_items:,}")

    if total_items == 0:
        print("  ⚠ No line items in ODS. Run populate_core_operations DAG first.")
        cur.close()
        conn.close()
        return

    # Process in chunks
    chunk_size = 50000
    inserted = 0
    skipped = 0

    for offset in range(0, total_items, chunk_size):
        print(f"\nProcessing chunk {offset:,} to {min(offset + chunk_size, total_items):,}")

        # Read line items with order info
        line_items_df = pd.read_sql(f"""
            SELECT 
                li.order_id,
                li.product_id,
                li.quantity,
                li.price,
                o.user_id,
                o.transaction_date,
                o.merchant_id,
                o.staff_id,
                o.campaign_id
            FROM ods.core_line_items li
            LEFT JOIN ods.core_orders o ON li.order_id = o.order_id
            ORDER BY li.line_item_id
            LIMIT {chunk_size} OFFSET {offset}
        """, engine)

        records = []
        for _, row in line_items_df.iterrows():
            # Get surrogate keys
            user_key = user_lookup.get(row['user_id'])
            product_key = product_lookup.get(row['product_id'])
            merchant_key = merchant_lookup.get(row['merchant_id']) if pd.notna(row.get('merchant_id')) else None
            staff_key = staff_lookup.get(row['staff_id']) if pd.notna(row.get('staff_id')) else None
            campaign_key = campaign_lookup.get(row['campaign_id']) if pd.notna(row.get('campaign_id')) else None

            # Get date key
            order_date_key = None
            if pd.notna(row.get('transaction_date')):
                try:
                    dt = pd.to_datetime(row['transaction_date'])
                    order_date_key = int(dt.strftime('%Y%m%d'))
                except:
                    pass

            # Calculate sale amount
            quantity = int(row['quantity']) if pd.notna(row['quantity']) else 0
            unit_price = float(row['price']) if pd.notna(row['price']) else 0.0
            sale_amount = quantity * unit_price

            records.append((
                row['order_id'],
                order_date_key,
                user_key,
                product_key,
                merchant_key,
                staff_key,
                campaign_key,
                quantity,
                unit_price,
                sale_amount
            ))

        # Bulk insert
        if records:
            insert_sql = """
                INSERT INTO dw.fact_sales 
                (order_id, order_date_key, user_key, product_key, merchant_key, staff_key, campaign_key, 
                 quantity_sold, unit_price, sale_amount)
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


def verify_fact_sales(**context):
    """Verify the fact table."""
    engine = get_db_engine()
    conn = engine.raw_connection()
    cur = conn.cursor()

    print("=" * 70)
    print("VERIFY DW.FACT_SALES")
    print("=" * 70)

    cur.execute("SELECT COUNT(*) FROM dw.fact_sales;")
    count = cur.fetchone()[0]
    print(f"dw.fact_sales row count: {count:,}")

    cur.execute("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT order_id) as unique_orders,
            SUM(quantity_sold) as total_quantity,
            SUM(sale_amount) as total_sales,
            AVG(unit_price) as avg_price
        FROM dw.fact_sales;
    """)
    stats = cur.fetchone()
    if stats and stats[0]:
        print(f"Total records: {stats[0]:,}")
        print(f"Unique orders: {stats[1]:,}")
        print(f"Total quantity sold: {stats[2]:,}" if stats[2] else "Total quantity: 0")
        print(f"Total sales amount: ${stats[3]:,.2f}" if stats[3] else "Total sales: $0")
        print(f"Average unit price: ${stats[4]:.2f}" if stats[4] else "Avg price: $0")

    cur.execute("""
        SELECT sales_key, order_id, order_date_key, user_key, product_key, quantity_sold, unit_price, sale_amount
        FROM dw.fact_sales
        ORDER BY sales_key
        LIMIT 5;
    """)
    print("\nSample rows:")
    for r in cur.fetchall():
        print(f"  {r}")

    cur.close()
    conn.close()
    print("=" * 70)


with DAG(
    'populate_fact_sales',
    default_args=default_args,
    description='Populate dw.fact_sales from ODS (Kimball fact table)',
    schedule_interval=None,
    catchup=False,
    tags=['dw', 'kimball', 'fact', 'sales'],
) as dag:

    t_dim_date = PythonOperator(task_id='ensure_dim_date', python_callable=ensure_dim_date)
    t_populate = PythonOperator(task_id='populate_fact_sales', python_callable=populate_fact_sales)
    t_verify = PythonOperator(task_id='verify_fact_sales', python_callable=verify_fact_sales)

    t_dim_date >> t_populate >> t_verify
