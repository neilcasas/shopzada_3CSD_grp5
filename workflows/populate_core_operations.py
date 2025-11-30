from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os
import uuid

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
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER', 'airflow')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'airflow')}@"
        f"{os.getenv('POSTGRES_HOST', 'postgres')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/shopzada"
    )

def process_and_load_orders(**context):
    """Process orders directly from staging to ODS without XCom."""
    engine = get_db_engine()
    
    print("=" * 70)
    print("PROCESSING ORDERS FROM STAGING TO ODS")
    print("=" * 70)
    
    conn = engine.raw_connection()
    cursor = conn.cursor()
    
    # Truncate target tables first
    print("\nTruncating ODS tables...")
    cursor.execute("TRUNCATE TABLE ods.core_orders CASCADE;")
    cursor.execute("TRUNCATE TABLE ods.core_line_items CASCADE;")
    conn.commit()
    print("  ✓ Tables truncated")
    
    # ========== PROCESS ORDERS IN CHUNKS ==========
    print("\n--- Processing Orders ---")
    
    # Get total count
    cursor.execute("SELECT COUNT(*) FROM staging.ops_orders_raw;")
    total_orders = cursor.fetchone()[0]
    print(f"Total orders to process: {total_orders:,}")
    
    chunk_size = 50000
    orders_inserted = 0
    orders_skipped = 0
    
    for offset in range(0, total_orders, chunk_size):
        print(f"\nProcessing orders chunk: {offset:,} to {min(offset+chunk_size, total_orders):,}")
        
        # Read chunk from staging
        orders_chunk = pd.read_sql(f"""
            SELECT * FROM staging.ops_orders_raw 
            ORDER BY order_id
            LIMIT {chunk_size} OFFSET {offset}
        """, engine)
        
        # Clean order_id and user_id
        orders_chunk = orders_chunk[orders_chunk['order_id'].notna()]
        orders_chunk = orders_chunk[orders_chunk['order_id'].astype(str).str.strip() != '']
        orders_chunk = orders_chunk[orders_chunk['order_id'].astype(str).str.lower() != 'nan']
        orders_chunk['order_id'] = orders_chunk['order_id'].astype(str).str.strip()
        
        orders_chunk = orders_chunk[orders_chunk['user_id'].notna()]
        orders_chunk = orders_chunk[orders_chunk['user_id'].astype(str).str.strip() != '']
        orders_chunk = orders_chunk[orders_chunk['user_id'].astype(str).str.lower() != 'nan']
        orders_chunk['user_id'] = orders_chunk['user_id'].astype(str).str.strip()
        
        # Remove duplicates within chunk
        orders_chunk = orders_chunk.drop_duplicates(subset=['order_id'], keep='first')
        
        # Parse dates
        orders_chunk['transaction_date'] = pd.to_datetime(orders_chunk['transaction_date'], errors='coerce')
        orders_chunk['estimated_arrival'] = pd.to_datetime(orders_chunk['estimated_arrival'], errors='coerce')
        
        # Remove invalid dates
        orders_chunk = orders_chunk[orders_chunk['transaction_date'].notna()]
        
        # Get delays for these orders
        order_ids = tuple(orders_chunk['order_id'].unique())
        if len(order_ids) == 1:
            delays_query = f"SELECT * FROM staging.ops_order_delays_raw WHERE order_id = '{order_ids[0]}'"
        else:
            delays_query = f"SELECT * FROM staging.ops_order_delays_raw WHERE order_id IN {order_ids}"
        
        delays = pd.read_sql(delays_query, engine)
        
        # Clean delays
        if len(delays) > 0:
            delays = delays[delays['order_id'].notna()]
            delays['order_id'] = delays['order_id'].astype(str).str.strip()
            delays['delay_in_days'] = pd.to_numeric(delays['delay_in_days'], errors='coerce')
            delays = delays[delays['delay_in_days'].notna()]
            delays['delay_in_days'] = delays['delay_in_days'].astype(int)
            delays = delays.drop_duplicates(subset=['order_id'], keep='first')
        
        # Merge with delays
        orders_merged = orders_chunk.merge(
            delays[['order_id', 'delay_in_days']] if len(delays) > 0 else pd.DataFrame(columns=['order_id', 'delay_in_days']),
            on='order_id', 
            how='left'
        )
        
        # Calculate is_delayed and actual_arrival
        orders_merged['is_delayed'] = orders_merged['delay_in_days'].notna() & (orders_merged['delay_in_days'] > 0)
        orders_merged['actual_arrival'] = orders_merged.apply(
            lambda row: row['estimated_arrival'] + timedelta(days=int(row['delay_in_days'])) 
            if pd.notna(row['delay_in_days']) and pd.notna(row['estimated_arrival'])
            else None,
            axis=1
        )
        orders_merged['delay_in_days'] = orders_merged['delay_in_days'].fillna(0).astype(int)
        
        # Insert orders
        for _, row in orders_merged.iterrows():
            try:
                cursor.execute("""
                    INSERT INTO ods.core_orders 
                    (order_id, user_id, transaction_date, estimated_arrival, actual_arrival, delay_in_days, is_delayed, merchant_id, staff_id, campaign_id, availed)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NULL, NULL, NULL, NULL)
                    ON CONFLICT (order_id) DO NOTHING
                """, (
                    row['order_id'],
                    row['user_id'],
                    row['transaction_date'],
                    row['estimated_arrival'],
                    row.get('actual_arrival'),
                    row['delay_in_days'],
                    row['is_delayed']
                ))
                orders_inserted += 1
            except Exception as e:
                orders_skipped += 1
                if orders_skipped <= 5:
                    print(f"  ⚠ Error inserting order {row['order_id']}: {str(e)}")
        
        conn.commit()
        print(f"  ✓ Chunk processed: {len(orders_merged)} orders")
    
    print(f"\n✓ Total orders inserted: {orders_inserted:,}")
    if orders_skipped > 0:
        print(f"  ⚠ Skipped: {orders_skipped:,}")
    
    cursor.close()
    conn.close()

def process_and_load_line_items(**context):
    """Process line items directly from staging to ODS without XCom."""
    engine = get_db_engine()
    
    print("=" * 70)
    print("PROCESSING LINE ITEMS FROM STAGING TO ODS")
    print("=" * 70)
    
    conn = engine.raw_connection()
    cursor = conn.cursor()
    
    # Get total count
    cursor.execute("SELECT COUNT(*) FROM staging.ops_order_item_prices_raw;")
    total_items = cursor.fetchone()[0]
    print(f"Total line items to process: {total_items:,}")
    
    chunk_size = 50000
    items_inserted = 0
    items_skipped = 0
    
    for offset in range(0, total_items, chunk_size):
        print(f"\nProcessing line items chunk: {offset:,} to {min(offset+chunk_size, total_items):,}")
        
        # Read prices chunk
        prices_chunk = pd.read_sql(f"""
            SELECT * FROM staging.ops_order_item_prices_raw 
            ORDER BY order_id
            LIMIT {chunk_size} OFFSET {offset}
        """, engine)
        
        # Clean prices
        prices_chunk = prices_chunk[prices_chunk['order_id'].notna()]
        prices_chunk['order_id'] = prices_chunk['order_id'].astype(str).str.strip()
        
        prices_chunk['price'] = prices_chunk['price'].astype(str).str.strip()
        prices_chunk['price'] = prices_chunk['price'].str.replace('$', '', regex=False)
        prices_chunk['price'] = prices_chunk['price'].str.replace(',', '', regex=False)
        prices_chunk['price'] = prices_chunk['price'].str.replace('₱', '', regex=False)
        prices_chunk['price'] = pd.to_numeric(prices_chunk['price'], errors='coerce')
        
        prices_chunk['quantity'] = pd.to_numeric(prices_chunk['quantity'], errors='coerce')
        
        prices_chunk = prices_chunk[prices_chunk['price'].notna() & (prices_chunk['price'] > 0)]
        prices_chunk = prices_chunk[prices_chunk['quantity'].notna() & (prices_chunk['quantity'] > 0)]
        prices_chunk['quantity'] = prices_chunk['quantity'].astype(int)
        
        # Add item numbers
        prices_chunk['item_num'] = prices_chunk.groupby('order_id').cumcount()
        
        # Get corresponding products
        order_ids = tuple(prices_chunk['order_id'].unique())
        if len(order_ids) == 0:
            continue
            
        if len(order_ids) == 1:
            products_query = f"SELECT * FROM staging.ops_order_item_products_raw WHERE order_id = '{order_ids[0]}'"
        else:
            products_query = f"SELECT * FROM staging.ops_order_item_products_raw WHERE order_id IN {order_ids}"
        
        products_chunk = pd.read_sql(products_query, engine)
        
        # Clean products
        if len(products_chunk) > 0:
            products_chunk = products_chunk[products_chunk['order_id'].notna()]
            products_chunk['order_id'] = products_chunk['order_id'].astype(str).str.strip()
            
            products_chunk = products_chunk[products_chunk['product_id'].notna()]
            products_chunk['product_id'] = products_chunk['product_id'].astype(str).str.strip()
            products_chunk = products_chunk[products_chunk['product_id'].astype(str).str.lower() != 'nan']
            
            products_chunk['item_num'] = products_chunk.groupby('order_id').cumcount()
            
            # Merge
            line_items = prices_chunk.merge(
                products_chunk[['order_id', 'item_num', 'product_id']], 
                on=['order_id', 'item_num'], 
                how='inner'
            )
            
            # Insert line items
            for _, row in line_items.iterrows():
                try:
                    line_item_id = str(uuid.uuid4())
                    cursor.execute("""
                        INSERT INTO ods.core_line_items 
                        (line_item_id, order_id, product_id, quantity, price)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                        line_item_id,
                        row['order_id'],
                        row['product_id'],
                        int(row['quantity']),
                        float(row['price'])
                    ))
                    items_inserted += 1
                except Exception as e:
                    items_skipped += 1
                    if items_skipped <= 5:
                        print(f"  ⚠ Error inserting line item: {str(e)}")
            
            conn.commit()
            print(f"  ✓ Chunk processed: {len(line_items)} line items")
        else:
            print(f"  ⚠ No matching products found for this chunk")
    
    print(f"\n✓ Total line items inserted: {items_inserted:,}")
    if items_skipped > 0:
        print(f"  ⚠ Skipped: {items_skipped:,}")
    
    cursor.close()
    conn.close()

def verify_load(**context):
    """Verify the data loaded into ODS."""
    engine = get_db_engine()
    conn = engine.raw_connection()
    cursor = conn.cursor()
    
    print("=" * 70)
    print("VERIFICATION")
    print("=" * 70)
    
    # Orders stats
    cursor.execute("SELECT COUNT(*) FROM ods.core_orders;")
    order_count = cursor.fetchone()[0]
    
    cursor.execute("""
        SELECT 
            COUNT(*) as total_orders,
            COUNT(DISTINCT user_id) as unique_users,
            MIN(transaction_date) as earliest_order,
            MAX(transaction_date) as latest_order,
            SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) as delayed_orders,
            AVG(delay_in_days) as avg_delay_days
        FROM ods.core_orders;
    """)
    stats = cursor.fetchone()
    
    # Line items stats
    cursor.execute("SELECT COUNT(*) FROM ods.core_line_items;")
    line_item_count = cursor.fetchone()[0]
    
    cursor.execute("""
        SELECT 
            SUM(quantity) as total_items_sold,
            AVG(price) as avg_item_price,
            MIN(price) as min_item_price,
            MAX(price) as max_item_price
        FROM ods.core_line_items;
    """)
    item_stats = cursor.fetchone()
    
    print(f"\nODS Table Statistics:")
    print(f"  Orders: {order_count:,}")
    print(f"    Unique users: {stats[1]:,}")
    print(f"    Date range: {stats[2]} to {stats[3]}")
    print(f"    Delayed orders: {stats[4]:,} ({stats[4]/stats[0]*100:.1f}%)" if stats[0] > 0 else "    Delayed orders: 0")
    print(f"    Avg delay: {stats[5]:.1f} days" if stats[5] else "    Avg delay: 0 days")
    print(f"\n  Line Items: {line_item_count:,}")
    print(f"    Total items sold: {int(item_stats[0]):,}" if item_stats[0] else "    Total items sold: 0")
    print(f"    Avg item price: ${item_stats[1]:.2f}" if item_stats[1] else "    Avg item price: $0.00")
    print(f"    Price range: ${item_stats[2]:.2f} - ${item_stats[3]:.2f}" if item_stats[2] and item_stats[3] else "    Price range: N/A")
    
    cursor.close()
    conn.close()
    
    print("=" * 70)

with DAG(
    'populate_core_operations',
    default_args=default_args,
    description='Extract operations data from staging, transform, and load to ods.core_orders and ods.core_line_items',
    schedule_interval=None,
    catchup=False,
    tags=['etl', 'ods', 'operations', 'orders', 'line-items', 'staging-to-ods'],
) as dag:
    
    process_orders_task = PythonOperator(
        task_id='process_and_load_orders',
        python_callable=process_and_load_orders,
    )
    
    process_line_items_task = PythonOperator(
        task_id='process_and_load_line_items',
        python_callable=process_and_load_line_items,
    )
    
    verify_task = PythonOperator(
        task_id='verify_load',
        python_callable=verify_load,
    )
    
    # Set dependencies
    process_orders_task >> process_line_items_task >> verify_task