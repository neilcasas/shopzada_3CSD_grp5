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


def populate_fact_campaign_response(**context):
    """Populate fact_campaign_response from ODS with proper dimension lookups."""
    engine = get_db_engine()

    print("=" * 70)
    print("POPULATING DW.FACT_CAMPAIGN_RESPONSE (KIMBALL FACT TABLE)")
    print("=" * 70)

    conn = engine.raw_connection()
    cur = conn.cursor()

    # Truncate fact table
    print("Truncating dw.fact_campaign_response...")
    cur.execute("TRUNCATE TABLE dw.fact_campaign_response RESTART IDENTITY;")
    conn.commit()

    # Get dimension lookups
    print("\nLoading dimension lookups...")

    cur.execute("SELECT user_id, user_key FROM dw.dim_user;")
    user_lookup = {row[0]: row[1] for row in cur.fetchall()}
    print(f"  dim_user: {len(user_lookup):,} entries")

    cur.execute("SELECT merchant_id, merchant_key FROM dw.dim_merchant;")
    merchant_lookup = {row[0]: row[1] for row in cur.fetchall()}
    print(f"  dim_merchant: {len(merchant_lookup):,} entries")

    cur.execute("SELECT campaign_id, campaign_key FROM dw.dim_campaign;")
    campaign_lookup = {row[0]: row[1] for row in cur.fetchall()}
    print(f"  dim_campaign: {len(campaign_lookup):,} entries")

    # Check if we have transactional campaign data in ODS
    # This should come from core_campaigns or a related source with order linkage
    cur.execute("""
        SELECT COUNT(*) 
        FROM ods.core_orders o
        WHERE o.campaign_id IS NOT NULL;
    """)
    orders_with_campaigns = cur.fetchone()[0]
    print(f"\nOrders with campaign attribution: {orders_with_campaigns:,}")

    if orders_with_campaigns == 0:
        # Try to get from core_campaigns if it has transactional data
        cur.execute("SELECT COUNT(*) FROM ods.core_campaigns WHERE campaign_id IS NOT NULL;")
        trans_campaigns = cur.fetchone()[0]

        if trans_campaigns > 0:
            print(f"Found {trans_campaigns:,} campaign records but no order linkage")
        
        print("  ⚠ No campaign response data found. Skipping fact population.")
        cur.close()
        conn.close()
        return
    else:
        # Get orders with campaign_id and calculate amounts from line_items
        campaign_df = pd.read_sql("""
            SELECT 
                o.order_id,
                o.campaign_id,
                o.user_id,
                o.merchant_id,
                o.transaction_date,
                o.availed,
                COALESCE(li.order_total, 0) as order_total_amount,
                c.discount as campaign_discount_pct
            FROM ods.core_orders o
            LEFT JOIN (
                SELECT order_id, SUM(quantity * price) as order_total
                FROM ods.core_line_items
                GROUP BY order_id
            ) li ON o.order_id = li.order_id
            LEFT JOIN ods.core_campaigns c ON o.campaign_id = c.campaign_id
            WHERE o.campaign_id IS NOT NULL
        """, engine)
        
        # Calculate discount and net amounts
        campaign_df['order_discount_amount'] = campaign_df.apply(
            lambda row: row['order_total_amount'] * (row['campaign_discount_pct'] / 100) 
            if pd.notna(row['campaign_discount_pct']) and row['availed'] == True else 0,
            axis=1
        )
        campaign_df['order_net_amount'] = campaign_df['order_total_amount'] - campaign_df['order_discount_amount']

    if campaign_df.empty:
        print("  ⚠ No campaign response data found. Skipping fact population.")
        cur.close()
        conn.close()
        return

    print(f"\nProcessing {len(campaign_df):,} campaign response records...")

    records = []
    for _, row in campaign_df.iterrows():
        # Get surrogate keys
        user_key = user_lookup.get(row.get('user_id'))
        merchant_key = merchant_lookup.get(row.get('merchant_id')) if pd.notna(row.get('merchant_id')) else None
        campaign_key = campaign_lookup.get(row.get('campaign_id')) if pd.notna(row.get('campaign_id')) else None

        # Get date key
        transaction_date_key = None
        if pd.notna(row.get('transaction_date')):
            try:
                dt = pd.to_datetime(row['transaction_date'])
                transaction_date_key = int(dt.strftime('%Y%m%d'))
            except:
                pass

        # Get amounts
        order_total = float(row['order_total_amount']) if pd.notna(row.get('order_total_amount')) else 0.0
        order_discount = float(row['order_discount_amount']) if pd.notna(row.get('order_discount_amount')) else 0.0
        order_net = float(row['order_net_amount']) if pd.notna(row.get('order_net_amount')) else order_total - order_discount

        records.append((
            row['order_id'],
            transaction_date_key,
            user_key,
            merchant_key,
            campaign_key,
            order_total,
            order_discount,
            order_net
        ))

    # Bulk insert
    if records:
        insert_sql = """
            INSERT INTO dw.fact_campaign_response 
            (order_id, transaction_date_key, user_key, merchant_key, campaign_key, 
             order_total_amount, order_discount_amount, order_net_amount)
            VALUES %s
        """
        try:
            psycopg2.extras.execute_values(cur, insert_sql, records, page_size=5000)
            conn.commit()
            print(f"  ✓ Inserted {len(records):,} campaign response records")
        except Exception as e:
            conn.rollback()
            print(f"  ✗ Error: {e}")

    cur.close()
    conn.close()
    print("=" * 70)


def verify_fact_campaign_response(**context):
    """Verify the fact table."""
    engine = get_db_engine()
    conn = engine.raw_connection()
    cur = conn.cursor()

    print("=" * 70)
    print("VERIFY DW.FACT_CAMPAIGN_RESPONSE")
    print("=" * 70)

    cur.execute("SELECT COUNT(*) FROM dw.fact_campaign_response;")
    count = cur.fetchone()[0]
    print(f"dw.fact_campaign_response row count: {count:,}")

    if count > 0:
        cur.execute("""
            SELECT 
                COUNT(*) as total_responses,
                COUNT(DISTINCT campaign_key) as unique_campaigns,
                SUM(order_total_amount) as total_revenue,
                SUM(order_discount_amount) as total_discounts,
                SUM(order_net_amount) as total_net_revenue
            FROM dw.fact_campaign_response;
        """)
        stats = cur.fetchone()
        print(f"Total responses: {stats[0]:,}")
        print(f"Unique campaigns: {stats[1]:,}" if stats[1] else "Unique campaigns: N/A")
        print(f"Total revenue: ${stats[2]:,.2f}" if stats[2] else "Total revenue: $0")
        print(f"Total discounts: ${stats[3]:,.2f}" if stats[3] else "Total discounts: $0")
        print(f"Net revenue: ${stats[4]:,.2f}" if stats[4] else "Net revenue: $0")

        cur.execute("""
            SELECT response_key, order_id, transaction_date_key, user_key, campaign_key, 
                   order_total_amount, order_discount_amount
            FROM dw.fact_campaign_response
            ORDER BY response_key
            LIMIT 5;
        """)
        print("\nSample rows:")
        for r in cur.fetchall():
            print(f"  {r}")

    cur.close()
    conn.close()
    print("=" * 70)


with DAG(
    'populate_fact_campaign_response',
    default_args=default_args,
    description='Populate dw.fact_campaign_response from ODS (Kimball fact table)',
    schedule_interval=None,
    catchup=False,
    tags=['dw', 'kimball', 'fact', 'campaign'],
) as dag:

    t_dim_date = PythonOperator(task_id='ensure_dim_date', python_callable=ensure_dim_date)
    t_populate = PythonOperator(task_id='populate_fact_campaign_response', python_callable=populate_fact_campaign_response)
    t_verify = PythonOperator(task_id='verify_fact_campaign_response', python_callable=verify_fact_campaign_response)

    t_dim_date >> t_populate >> t_verify
