"""
Streaming Operations Pipeline DAG
=================================

This DAG implements a streaming/pipelining approach for the operations department ETL.
Instead of waiting for each stage to complete fully, each stage starts processing
as soon as there's enough data available from the previous stage.

Key Features:
1. Processes latest data first (by transaction_date DESC)
2. Each layer runs in parallel once there's enough data
3. Uses a tracking table to know what's been processed at each layer
4. Chunks flow through the pipeline without waiting for previous stages to complete

Pipeline Flow:
  Source Files → Staging (chunks) → ODS (streaming) → DW Dims → Fact Tables
                     ↓                    ↓               ↓           ↓
                 (parallel)          (parallel)      (parallel)  (parallel)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import os
import psycopg2
import psycopg2.extras
import re

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Configuration - OPTIMIZED
MIN_CHUNKS_TO_START_NEXT_LAYER = 1  # Start next layer after this many chunks are ready
CHUNK_SIZE = 50000  # Increased: Process 50k records at a time for faster streaming
BULK_INSERT_BATCH = 10000  # Optimal batch size for execute_values


def get_db_engine():
    """Create SQLAlchemy engine with connection pooling for better performance."""
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER', 'airflow')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'airflow')}@"
        f"{os.getenv('POSTGRES_HOST', 'postgres')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/shopzada",
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True
    )


def get_db_connection():
    """Get raw database connection."""
    engine = get_db_engine()
    return engine.raw_connection()


# =============================================================================
# SETUP: Create tracking table for processed chunks
# =============================================================================

def setup_tracking_table(**context):
    """Create a tracking table to track what chunks have been processed at each layer."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    print("=" * 70)
    print("SETTING UP STREAMING PIPELINE TRACKING")
    print("=" * 70)
    
    # Create tracking schema if not exists
    cur.execute("CREATE SCHEMA IF NOT EXISTS pipeline;")
    
    # Create tracking table for chunk processing status
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pipeline.ops_chunk_tracker (
            chunk_id SERIAL PRIMARY KEY,
            date_range_start DATE,
            date_range_end DATE,
            staging_loaded_at TIMESTAMP,
            ods_loaded_at TIMESTAMP,
            dw_dims_loaded_at TIMESTAMP,
            facts_loaded_at TIMESTAMP,
            order_count INTEGER,
            status VARCHAR(50) DEFAULT 'pending'
        );
    """)
    
    # Create index for faster lookups
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_ops_chunk_status 
        ON pipeline.ops_chunk_tracker(status, date_range_start DESC);
    """)
    
    conn.commit()
    print("  ✓ Tracking table created/verified")
    
    # Clear previous run's tracking data
    cur.execute("TRUNCATE TABLE pipeline.ops_chunk_tracker;")
    conn.commit()
    print("  ✓ Tracking table cleared for fresh run")
    
    # OPTIMIZATION: Create indexes on staging tables for faster joins
    print("  Creating staging indexes for faster queries...")
    indexes = [
        ("staging.ops_orders_raw", "order_id", "idx_stg_orders_oid"),
        ("staging.ops_orders_raw", "user_id", "idx_stg_orders_uid"),
        ("staging.ops_order_delays_raw", "order_id", "idx_stg_delays_oid"),
        ("staging.ops_order_item_prices_raw", "order_id", "idx_stg_prices_oid"),
        ("staging.ops_order_item_products_raw", "order_id", "idx_stg_products_oid"),
        ("staging.ent_order_merchants_raw", "order_id", "idx_stg_merchants_oid"),
        ("staging.mkt_campaign_transactions_raw", "order_id", "idx_stg_campaigns_oid"),
    ]
    for table, column, idx_name in indexes:
        try:
            cur.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}({column});")
        except:
            pass
    conn.commit()
    
    # Analyze tables for better query planning
    for table, _, _ in indexes:
        try:
            cur.execute(f"ANALYZE {table};")
        except:
            pass
    conn.commit()
    print("  ✓ Staging indexes created")
    
    cur.close()
    conn.close()
    print("=" * 70)


def analyze_source_data_and_create_chunks(**context):
    """
    Analyze source data and create chunk definitions based on date ranges.
    Chunks are ordered by date DESC (latest first).
    """
    engine = get_db_engine()
    conn = get_db_connection()
    cur = conn.cursor()
    
    print("=" * 70)
    print("ANALYZING SOURCE DATA & CREATING CHUNK PLAN")
    print("=" * 70)
    
    # Check staging table for date ranges - use pandas for more flexible date parsing
    try:
        # Read transaction dates and parse with pandas (more flexible than SQL)
        orders_dates = pd.read_sql("""
            SELECT transaction_date
            FROM staging.ops_orders_raw
            WHERE transaction_date IS NOT NULL
              AND transaction_date != ''
        """, engine)
        
        if len(orders_dates) == 0:
            print("  ⚠ No orders found in staging. Loading all data as single chunk.")
            cur.execute("""
                INSERT INTO pipeline.ops_chunk_tracker 
                (date_range_start, date_range_end, status, order_count)
                VALUES (NULL, NULL, 'pending_staging', 0)
            """)
            conn.commit()
            cur.close()
            conn.close()
            return
        
        # Parse dates using pandas with mixed format support
        orders_dates['parsed_date'] = pd.to_datetime(
            orders_dates['transaction_date'], 
            format='mixed', 
            errors='coerce'
        )
        
        # Filter out invalid dates
        valid_dates = orders_dates[orders_dates['parsed_date'].notna()]['parsed_date']
        
        if len(valid_dates) == 0:
            print("  ⚠ No valid dates found in staging. Loading all data as single chunk.")
            cur.execute("""
                INSERT INTO pipeline.ops_chunk_tracker 
                (date_range_start, date_range_end, status, order_count)
                VALUES (NULL, NULL, 'pending_staging', 0)
            """)
            conn.commit()
            cur.close()
            conn.close()
            return
        
        min_date = valid_dates.min().date()
        max_date = valid_dates.max().date()
        total_orders = len(valid_dates)
        
        print(f"  Date range: {min_date} to {max_date}")
        print(f"  Total orders with valid dates: {total_orders:,}")
        
        # Create weekly chunks, ordered by date DESC (latest first)
        # This ensures dashboard shows latest data first
        current_end = pd.to_datetime(max_date)
        current_start = current_end - timedelta(days=6)
        min_dt = pd.to_datetime(min_date)
        
        chunks_created = 0
        while current_end >= min_dt:
            chunk_start = max(current_start, min_dt)
            
            # Count orders in this chunk using pandas-parsed dates
            chunk_mask = (valid_dates >= pd.Timestamp(chunk_start)) & (valid_dates <= pd.Timestamp(current_end))
            chunk_count = chunk_mask.sum()
            
            if chunk_count > 0:
                start_dt = chunk_start.date() if hasattr(chunk_start, 'date') else chunk_start
                end_dt = current_end.date() if hasattr(current_end, 'date') else current_end
                cur.execute("""
                    INSERT INTO pipeline.ops_chunk_tracker 
                    (date_range_start, date_range_end, status, order_count)
                    VALUES (%s, %s, 'pending_staging', %s)
                """, (start_dt, end_dt, int(chunk_count)))
                chunks_created += 1
                print(f"    Chunk {chunks_created}: {start_dt} to {end_dt} ({chunk_count:,} orders)")
            
            # Move to previous week
            current_end = current_start - timedelta(days=1)
            current_start = current_end - timedelta(days=6)
        
        conn.commit()
        print(f"\n  ✓ Created {chunks_created} chunks (latest dates first)")
        
    except Exception as e:
        print(f"  ⚠ Error analyzing data: {e}")
        # Fallback: create single chunk
        cur.execute("""
            INSERT INTO pipeline.ops_chunk_tracker 
            (date_range_start, date_range_end, status, order_count)
            VALUES (NULL, NULL, 'pending_staging', 0)
        """)
        conn.commit()
    
    cur.close()
    conn.close()
    print("=" * 70)


# =============================================================================
# SENSORS: Check if enough data is ready for next layer
# =============================================================================

def check_staging_ready(**context):
    """Check if there are chunks ready to process from staging to ODS."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT COUNT(*) FROM pipeline.ops_chunk_tracker
        WHERE staging_loaded_at IS NOT NULL AND ods_loaded_at IS NULL
    """)
    ready_chunks = cur.fetchone()[0]
    
    cur.close()
    conn.close()
    
    return ready_chunks >= MIN_CHUNKS_TO_START_NEXT_LAYER


def check_ods_ready(**context):
    """Check if there are chunks ready to process from ODS to DW."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT COUNT(*) FROM pipeline.ops_chunk_tracker
        WHERE ods_loaded_at IS NOT NULL AND dw_dims_loaded_at IS NULL
    """)
    ready_chunks = cur.fetchone()[0]
    
    cur.close()
    conn.close()
    
    return ready_chunks >= MIN_CHUNKS_TO_START_NEXT_LAYER


def check_dims_ready(**context):
    """Check if dimensions are populated and chunks are ready for facts."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Check if we have dimension data
    cur.execute("SELECT COUNT(*) FROM dw.dim_user;")
    user_count = cur.fetchone()[0]
    
    cur.execute("""
        SELECT COUNT(*) FROM pipeline.ops_chunk_tracker
        WHERE dw_dims_loaded_at IS NOT NULL AND facts_loaded_at IS NULL
    """)
    ready_chunks = cur.fetchone()[0]
    
    cur.close()
    conn.close()
    
    return user_count > 0 and ready_chunks >= MIN_CHUNKS_TO_START_NEXT_LAYER


# =============================================================================
# LAYER 1: Staging Processing (mark chunks as ready for ODS)
# =============================================================================

def process_staging_chunks(**context):
    """
    Process staging data and mark chunks as ready for ODS.
    Since data is already in staging, we just validate and mark as processed.
    """
    engine = get_db_engine()
    conn = get_db_connection()
    cur = conn.cursor()
    
    print("=" * 70)
    print("PROCESSING STAGING CHUNKS")
    print("=" * 70)
    
    # Get pending staging chunks (ordered by date DESC - latest first)
    cur.execute("""
        SELECT chunk_id, date_range_start, date_range_end, order_count
        FROM pipeline.ops_chunk_tracker
        WHERE status = 'pending_staging'
        ORDER BY date_range_start DESC NULLS LAST
    """)
    chunks = cur.fetchall()
    
    print(f"Found {len(chunks)} chunks to process")
    
    for chunk_id, start_date, end_date, order_count in chunks:
        print(f"\n  Processing chunk {chunk_id}: {start_date} to {end_date}")
        
        # Validate data exists in staging - use CAST with error handling
        if start_date and end_date:
            # Use a more flexible date casting approach
            cur.execute("""
                SELECT COUNT(*) FROM staging.ops_orders_raw
                WHERE transaction_date IS NOT NULL 
                  AND transaction_date != ''
                  AND CAST(
                      CASE 
                          WHEN transaction_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' 
                          THEN SUBSTRING(transaction_date FROM 1 FOR 10)
                          ELSE NULL 
                      END AS DATE
                  ) BETWEEN %s AND %s
            """, (start_date, end_date))
        else:
            cur.execute("SELECT COUNT(*) FROM staging.ops_orders_raw WHERE order_id IS NOT NULL")
        
        actual_count = cur.fetchone()[0]
        
        if actual_count > 0:
            # Mark chunk as ready for ODS processing
            cur.execute("""
                UPDATE pipeline.ops_chunk_tracker
                SET staging_loaded_at = NOW(), 
                    status = 'staging_complete',
                    order_count = %s
                WHERE chunk_id = %s
            """, (actual_count, chunk_id))
            conn.commit()
            print(f"    ✓ Chunk {chunk_id} ready for ODS ({actual_count:,} orders)")
        else:
            print(f"    ⚠ Chunk {chunk_id} has no data, skipping")
            cur.execute("""
                UPDATE pipeline.ops_chunk_tracker
                SET status = 'skipped'
                WHERE chunk_id = %s
            """, (chunk_id,))
            conn.commit()
    
    cur.close()
    conn.close()
    print("=" * 70)


# =============================================================================
# LAYER 2: ODS Processing (Staging → ODS)
# =============================================================================

def process_ods_chunks(**context):
    """
    Process chunks from staging to ODS.
    Runs in parallel with staging processing.
    """
    engine = get_db_engine()
    conn = get_db_connection()
    cur = conn.cursor()
    
    print("=" * 70)
    print("PROCESSING ODS CHUNKS (STREAMING)")
    print("=" * 70)
    
    # Get chunks ready for ODS (staging complete, ods not done)
    cur.execute("""
        SELECT chunk_id, date_range_start, date_range_end
        FROM pipeline.ops_chunk_tracker
        WHERE staging_loaded_at IS NOT NULL AND ods_loaded_at IS NULL
        ORDER BY date_range_start DESC NULLS LAST
    """)
    chunks = cur.fetchall()
    
    print(f"Found {len(chunks)} chunks ready for ODS processing")
    
    # Disable foreign keys for faster inserts
    cur.execute("SET session_replication_role = 'replica';")
    conn.commit()
    
    for chunk_id, start_date, end_date in chunks:
        print(f"\n  Processing chunk {chunk_id}: {start_date} to {end_date} → ODS")
        
        try:
            # Build date filter - use substring extraction for ISO format dates
            if start_date and end_date:
                date_filter = f"""AND CAST(
                    CASE 
                        WHEN o.transaction_date ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}' 
                        THEN SUBSTRING(o.transaction_date FROM 1 FOR 10)
                        ELSE NULL 
                    END AS DATE
                ) BETWEEN '{start_date}' AND '{end_date}'"""
            else:
                date_filter = ""
            
            # Read orders for this chunk
            orders_df = pd.read_sql(f"""
                SELECT DISTINCT ON (o.order_id)
                    o.order_id,
                    o.user_id,
                    o.transaction_date,
                    o.estimated_arrival,
                    d.delay_in_days,
                    m.merchant_id,
                    m.staff_id,
                    c.campaign_id,
                    c.availed
                FROM staging.ops_orders_raw o
                LEFT JOIN staging.ops_order_delays_raw d ON o.order_id = d.order_id
                LEFT JOIN staging.ent_order_merchants_raw m ON o.order_id = m.order_id
                LEFT JOIN staging.mkt_campaign_transactions_raw c ON o.order_id = c.order_id
                WHERE o.order_id IS NOT NULL 
                  AND o.order_id != ''
                  AND o.user_id IS NOT NULL
                  AND o.user_id != ''
                  {date_filter}
                ORDER BY o.order_id, o.transaction_date DESC
            """, engine)
            
            if len(orders_df) == 0:
                print(f"    ⚠ No orders found for chunk {chunk_id}")
                cur.execute("""
                    UPDATE pipeline.ops_chunk_tracker
                    SET ods_loaded_at = NOW(), status = 'ods_complete'
                    WHERE chunk_id = %s
                """, (chunk_id,))
                conn.commit()
                continue
            
            # Clean and transform data
            orders_df['order_id'] = orders_df['order_id'].astype(str).str.strip()
            orders_df['user_id'] = orders_df['user_id'].astype(str).str.strip()
            orders_df['transaction_date'] = pd.to_datetime(orders_df['transaction_date'], format='mixed', errors='coerce')
            orders_df['delay_in_days'] = pd.to_numeric(orders_df['delay_in_days'], errors='coerce').fillna(0).astype(int)
            orders_df['is_delayed'] = orders_df['delay_in_days'] > 0
            
            # Parse estimated_arrival from duration string (e.g., "13days" → 13)
            # Then calculate actual estimated_arrival date = transaction_date + days
            def parse_estimated_arrival(row):
                if pd.isna(row['transaction_date']):
                    return None
                est_str = str(row['estimated_arrival']) if pd.notna(row['estimated_arrival']) else ''
                # Extract numeric days from strings like "13days", "15 days", etc.
                match = re.search(r'(\d+)', est_str)
                if match:
                    days = int(match.group(1))
                    return row['transaction_date'] + timedelta(days=days)
                return None
            
            orders_df['estimated_arrival'] = orders_df.apply(parse_estimated_arrival, axis=1)
            
            # Calculate actual arrival = estimated_arrival + delay_in_days
            orders_df['actual_arrival'] = orders_df.apply(
                lambda row: row['estimated_arrival'] + timedelta(days=int(row['delay_in_days'])) 
                if pd.notna(row['estimated_arrival']) and row['delay_in_days'] > 0
                else None,
                axis=1
            )
            
            # Clean string columns
            for col in ['merchant_id', 'staff_id', 'campaign_id']:
                if col in orders_df.columns:
                    orders_df[col] = orders_df[col].apply(
                        lambda x: str(x).strip() if pd.notna(x) and str(x).strip().lower() not in ['nan', 'none', ''] else None
                    )
            
            # Handle availed boolean
            if 'availed' in orders_df.columns:
                orders_df['availed'] = pd.to_numeric(orders_df['availed'], errors='coerce').fillna(0).astype(bool)
            else:
                orders_df['availed'] = False
            
            # OPTIMIZED: Bulk insert using execute_values (10-50x faster than row-by-row)
            def to_val(x):
                """Convert pandas/numpy values to Python native types."""
                if pd.isna(x):
                    return None
                if isinstance(x, pd.Timestamp):
                    return x.to_pydatetime()
                if isinstance(x, (np.integer, np.floating)):
                    return x.item()
                return x
            
            # Prepare records as list of tuples
            orders_records = [
                (
                    row['order_id'],
                    row['user_id'],
                    to_val(row['transaction_date']),
                    to_val(row['estimated_arrival']),
                    to_val(row.get('actual_arrival')),
                    int(row['delay_in_days']),
                    bool(row['is_delayed']),
                    to_val(row.get('merchant_id')),
                    to_val(row.get('staff_id')),
                    to_val(row.get('campaign_id')),
                    bool(row.get('availed', False))
                )
                for _, row in orders_df.iterrows()
            ]
            
            # Bulk insert in batches
            inserted = 0
            for i in range(0, len(orders_records), BULK_INSERT_BATCH):
                batch = orders_records[i:i + BULK_INSERT_BATCH]
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO ods.core_orders 
                    (order_id, user_id, transaction_date, estimated_arrival, actual_arrival,
                     delay_in_days, is_delayed, merchant_id, staff_id, campaign_id, availed)
                    VALUES %s
                    ON CONFLICT (order_id) DO NOTHING
                    """,
                    batch,
                    page_size=BULK_INSERT_BATCH
                )
                inserted += len(batch)
            
            conn.commit()
            
            # Also process line items for this chunk
            line_items_inserted = process_line_items_for_chunk(engine, conn, cur, start_date, end_date)
            
            # Mark chunk as ODS complete
            cur.execute("""
                UPDATE pipeline.ops_chunk_tracker
                SET ods_loaded_at = NOW(), status = 'ods_complete'
                WHERE chunk_id = %s
            """, (chunk_id,))
            conn.commit()
            
            print(f"    ✓ Chunk {chunk_id}: {inserted:,} orders, {line_items_inserted:,} line items → ODS")
            
        except Exception as e:
            print(f"    ✗ Error processing chunk {chunk_id}: {e}")
            conn.rollback()
    
    # Re-enable foreign keys
    cur.execute("SET session_replication_role = 'origin';")
    conn.commit()
    
    cur.close()
    conn.close()
    print("=" * 70)


def process_line_items_for_chunk(engine, conn, cur, start_date, end_date):
    """Process line items for a specific date range chunk."""
    try:
        if start_date and end_date:
            # Get order_ids for this date range using flexible date parsing
            order_ids_df = pd.read_sql(f"""
                SELECT DISTINCT order_id FROM staging.ops_orders_raw
                WHERE CAST(
                    CASE 
                        WHEN transaction_date ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}' 
                        THEN SUBSTRING(transaction_date FROM 1 FOR 10)
                        ELSE NULL 
                    END AS DATE
                ) BETWEEN '{start_date}' AND '{end_date}'
                  AND order_id IS NOT NULL AND order_id != ''
            """, engine)
            
            if len(order_ids_df) == 0:
                return 0
            
            order_ids = tuple(order_ids_df['order_id'].tolist())
            if len(order_ids) == 1:
                order_filter = f"WHERE p.order_id = '{order_ids[0]}'"
            else:
                order_filter = f"WHERE p.order_id IN {order_ids}"
        else:
            order_filter = "WHERE p.order_id IS NOT NULL"
        
        # Read line items - Use ROW_NUMBER to match prices and products when raw_index is NULL
        # This handles parquet files that don't have raw_index
        line_items_df = pd.read_sql(f"""
            WITH prices_numbered AS (
                SELECT 
                    order_id, price, quantity,
                    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _ingested_at, price) as row_num
                FROM staging.ops_order_item_prices_raw p
                {order_filter.replace('p.order_id', 'order_id')}
            ),
            products_numbered AS (
                SELECT 
                    order_id, product_id, product_name,
                    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _ingested_at, product_id) as row_num
                FROM staging.ops_order_item_products_raw
                {order_filter.replace('p.order_id', 'order_id')}
            )
            SELECT 
                p.order_id,
                p.price,
                p.quantity,
                pr.product_id,
                pr.product_name
            FROM prices_numbered p
            LEFT JOIN products_numbered pr 
                ON p.order_id = pr.order_id AND p.row_num = pr.row_num
        """, engine)
        
        if len(line_items_df) == 0:
            return 0
        
        # Clean data
        line_items_df['order_id'] = line_items_df['order_id'].astype(str).str.strip()
        line_items_df['price'] = line_items_df['price'].astype(str).str.replace('[$,₱]', '', regex=True)
        line_items_df['price'] = pd.to_numeric(line_items_df['price'], errors='coerce')
        
        # Extract quantity from strings like "4PC", "2pcs"
        def extract_qty(val):
            if pd.isna(val):
                return 1
            match = re.search(r'(\d+)', str(val))
            return int(match.group(1)) if match else 1
        
        line_items_df['quantity'] = line_items_df['quantity'].apply(extract_qty)
        
        # Generate unique line_item_ids (vectorized - much faster than apply)
        line_items_df['item_num'] = line_items_df.groupby('order_id').cumcount()
        line_items_df['line_item_id'] = line_items_df['order_id'] + '_item_' + line_items_df['item_num'].astype(str)
        
        # Filter valid rows (vectorized)
        valid_mask = (line_items_df['price'].notna()) & (line_items_df['price'] > 0)
        valid_items = line_items_df[valid_mask]
        
        # Clean product_id (vectorized)
        valid_items = valid_items.copy()
        valid_items['product_id'] = valid_items['product_id'].astype(str).str.strip()
        valid_items['product_id'] = valid_items['product_id'].replace(['nan', 'None', 'none', ''], np.nan)
        
        # OPTIMIZED: Bulk insert using execute_values
        line_items_records = [
            (
                row['line_item_id'],
                row['order_id'],
                row['product_id'] if pd.notna(row['product_id']) else None,
                int(row['quantity']),
                float(row['price'])
            )
            for _, row in valid_items.iterrows()
        ]
        
        # Bulk insert in batches
        inserted = 0
        for i in range(0, len(line_items_records), BULK_INSERT_BATCH):
            batch = line_items_records[i:i + BULK_INSERT_BATCH]
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO ods.core_line_items 
                (line_item_id, order_id, product_id, quantity, price)
                VALUES %s
                ON CONFLICT (line_item_id) DO NOTHING
                """,
                batch,
                page_size=BULK_INSERT_BATCH
            )
            inserted += len(batch)
        
        conn.commit()
        return inserted
        
    except Exception as e:
        print(f"      ⚠ Line items error: {e}")
        return 0


# =============================================================================
# LAYER 3: DW Processing (ODS → Dimensions)
# =============================================================================

def ensure_dim_date(**context):
    """Ensure dim_date is populated."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT COUNT(*) FROM dw.dim_date;")
    existing_count = cur.fetchone()[0]
    
    # Define the date range we need (wider range to handle future estimated arrivals)
    start_date = datetime(2019, 1, 1)
    end_date = datetime(2035, 12, 31)  # Extended to 2035 to handle future dates
    
    if existing_count == 0:
        print("Populating dim_date...")
    else:
        # Check if we need to extend the range
        cur.execute("SELECT MAX(date) FROM dw.dim_date;")
        max_date = cur.fetchone()[0]
        if max_date and max_date >= end_date.date():
            print(f"  dim_date already populated with {existing_count} dates (up to {max_date})")
            cur.close()
            conn.close()
            return
        print(f"Extending dim_date range to {end_date.date()}...")
    
    current = start_date
    records = []
    while current <= end_date:
        date_key = int(current.strftime('%Y%m%d'))
        records.append((
            date_key, current.date(), current.weekday() + 1,
            current.month, current.strftime('%B'),
            (current.month - 1) // 3 + 1, current.year,
            current.weekday() >= 5
        ))
        current += timedelta(days=1)
    
    psycopg2.extras.execute_values(cur, """
        INSERT INTO dw.dim_date (date_key, date, day_of_week, month, month_name, quarter, year, is_weekend)
        VALUES %s ON CONFLICT DO NOTHING
    """, records)
    conn.commit()
    print(f"  ✓ Inserted/updated {len(records)} dates (2019-2035)")
    
    cur.close()
    conn.close()


def process_dw_dims_for_chunks(**context):
    """Mark chunks as dimension-ready (dims are populated by other DAGs)."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    print("=" * 70)
    print("MARKING CHUNKS READY FOR FACTS")
    print("=" * 70)
    
    # Update all ODS-complete chunks to dims-complete
    cur.execute("""
        UPDATE pipeline.ops_chunk_tracker
        SET dw_dims_loaded_at = NOW(), status = 'dims_complete'
        WHERE ods_loaded_at IS NOT NULL AND dw_dims_loaded_at IS NULL
        RETURNING chunk_id
    """)
    updated = cur.fetchall()
    conn.commit()
    
    print(f"  ✓ {len(updated)} chunks marked ready for fact loading")
    
    cur.close()
    conn.close()
    print("=" * 70)


# =============================================================================
# LAYER 4: Fact Tables Processing (Dims → Facts)
# =============================================================================

def process_fact_tables_streaming(**context):
    """
    Process fact tables for chunks that are ready.
    This runs in parallel with other layers.
    """
    engine = get_db_engine()
    conn = get_db_connection()
    cur = conn.cursor()
    
    print("=" * 70)
    print("PROCESSING FACT TABLES (STREAMING)")
    print("=" * 70)
    
    # Load dimension lookups
    cur.execute("SELECT user_id, user_key FROM dw.dim_user;")
    user_lookup = {row[0]: row[1] for row in cur.fetchall()}
    
    cur.execute("SELECT product_id, product_key FROM dw.dim_product;")
    product_lookup = {row[0]: row[1] for row in cur.fetchall()}
    
    cur.execute("SELECT merchant_id, merchant_key FROM dw.dim_merchant;")
    merchant_lookup = {row[0]: row[1] for row in cur.fetchall()}
    
    cur.execute("SELECT staff_id, staff_key FROM dw.dim_staff;")
    staff_lookup = {row[0]: row[1] for row in cur.fetchall()}
    
    cur.execute("SELECT campaign_id, campaign_key FROM dw.dim_campaign;")
    campaign_lookup = {row[0]: row[1] for row in cur.fetchall()}
    
    # Load valid date keys to handle FK constraints
    cur.execute("SELECT date_key FROM dw.dim_date;")
    valid_date_keys = set(row[0] for row in cur.fetchall())
    
    print(f"  Loaded lookups: users={len(user_lookup)}, products={len(product_lookup)}, merchants={len(merchant_lookup)}")
    print(f"  Valid date keys: {len(valid_date_keys)} (range: {min(valid_date_keys)} - {max(valid_date_keys)})")
    
    # Get chunks ready for facts
    cur.execute("""
        SELECT chunk_id, date_range_start, date_range_end
        FROM pipeline.ops_chunk_tracker
        WHERE dw_dims_loaded_at IS NOT NULL AND facts_loaded_at IS NULL
        ORDER BY date_range_start DESC NULLS LAST
    """)
    chunks = cur.fetchall()
    
    print(f"  Found {len(chunks)} chunks ready for fact loading")
    
    for chunk_id, start_date, end_date in chunks:
        print(f"\n  Processing facts for chunk {chunk_id}: {start_date} to {end_date}")
        
        try:
            if start_date and end_date:
                # ODS table has proper TIMESTAMP columns, so ::DATE cast works
                date_filter = f"WHERE o.transaction_date::DATE BETWEEN '{start_date}' AND '{end_date}'"
            else:
                date_filter = "WHERE o.transaction_date IS NOT NULL"
            
            # Process fact_orders
            orders_df = pd.read_sql(f"""
                SELECT order_id, user_id, merchant_id, staff_id,
                       transaction_date, estimated_arrival, delay_in_days, is_delayed
                FROM ods.core_orders o
                {date_filter}
            """, engine)
            
            fact_orders_records = []
            skipped_orders = 0
            for _, row in orders_df.iterrows():
                user_key = user_lookup.get(row['user_id'])
                merchant_key = merchant_lookup.get(row['merchant_id']) if pd.notna(row.get('merchant_id')) else None
                staff_key = staff_lookup.get(row['staff_id']) if pd.notna(row.get('staff_id')) else None
                
                order_date_key = None
                est_arrival_key = None
                
                if pd.notna(row.get('transaction_date')):
                    try:
                        order_date_key = int(pd.to_datetime(row['transaction_date']).strftime('%Y%m%d'))
                        # Validate date key exists in dim_date
                        if order_date_key not in valid_date_keys:
                            order_date_key = None
                    except:
                        pass
                
                if pd.notna(row.get('estimated_arrival')):
                    try:
                        est_arrival_key = int(pd.to_datetime(row['estimated_arrival']).strftime('%Y%m%d'))
                        # Validate date key exists in dim_date
                        if est_arrival_key not in valid_date_keys:
                            est_arrival_key = None
                    except:
                        pass
                
                fact_orders_records.append((
                    row['order_id'], order_date_key, est_arrival_key,
                    user_key, merchant_key, staff_key,
                    int(row['delay_in_days']) if pd.notna(row.get('delay_in_days')) else 0,
                    bool(row['is_delayed']) if pd.notna(row.get('is_delayed')) else False
                ))
            
            # Insert fact_orders
            if fact_orders_records:
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO dw.fact_orders 
                    (order_id, order_date_key, estimated_arrival_date_key, user_key, merchant_key, staff_key, delay_in_days, is_delayed)
                    VALUES %s
                    ON CONFLICT DO NOTHING
                """, fact_orders_records)
                conn.commit()
            
            # Process fact_sales (line items)
            if start_date and end_date:
                sales_filter = f"""
                    WHERE o.transaction_date::DATE BETWEEN '{start_date}' AND '{end_date}'
                """
            else:
                sales_filter = "WHERE o.transaction_date IS NOT NULL"
            
            line_items_df = pd.read_sql(f"""
                SELECT li.order_id, li.product_id, li.quantity, li.price,
                       o.user_id, o.transaction_date, o.merchant_id, o.staff_id, o.campaign_id
                FROM ods.core_line_items li
                JOIN ods.core_orders o ON li.order_id = o.order_id
                {sales_filter}
            """, engine)
            
            fact_sales_records = []
            for _, row in line_items_df.iterrows():
                user_key = user_lookup.get(row['user_id'])
                product_key = product_lookup.get(row['product_id'])
                merchant_key = merchant_lookup.get(row['merchant_id']) if pd.notna(row.get('merchant_id')) else None
                staff_key = staff_lookup.get(row['staff_id']) if pd.notna(row.get('staff_id')) else None
                campaign_key = campaign_lookup.get(row['campaign_id']) if pd.notna(row.get('campaign_id')) else None
                
                order_date_key = None
                if pd.notna(row.get('transaction_date')):
                    try:
                        order_date_key = int(pd.to_datetime(row['transaction_date']).strftime('%Y%m%d'))
                        # Validate date key exists in dim_date
                        if order_date_key not in valid_date_keys:
                            order_date_key = None
                    except:
                        pass
                
                qty = int(row['quantity']) if pd.notna(row['quantity']) else 0
                price = float(row['price']) if pd.notna(row['price']) else 0.0
                
                fact_sales_records.append((
                    row['order_id'], order_date_key, user_key, product_key,
                    merchant_key, staff_key, campaign_key,
                    qty, price, qty * price
                ))
            
            # Insert fact_sales
            if fact_sales_records:
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO dw.fact_sales 
                    (order_id, order_date_key, user_key, product_key, merchant_key, staff_key, campaign_key,
                     quantity_sold, unit_price, sale_amount)
                    VALUES %s
                    ON CONFLICT DO NOTHING
                """, fact_sales_records)
                conn.commit()
            
            # Mark chunk as facts complete
            cur.execute("""
                UPDATE pipeline.ops_chunk_tracker
                SET facts_loaded_at = NOW(), status = 'complete'
                WHERE chunk_id = %s
            """, (chunk_id,))
            conn.commit()
            
            print(f"    ✓ Chunk {chunk_id}: {len(fact_orders_records)} orders, {len(fact_sales_records)} sales → Facts")
            
        except Exception as e:
            print(f"    ✗ Error: {e}")
            conn.rollback()
    
    cur.close()
    conn.close()
    print("=" * 70)


def verify_streaming_results(**context):
    """Verify the streaming pipeline results."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    print("=" * 70)
    print("STREAMING PIPELINE VERIFICATION")
    print("=" * 70)
    
    # Chunk status summary
    cur.execute("""
        SELECT status, COUNT(*), SUM(order_count)
        FROM pipeline.ops_chunk_tracker
        GROUP BY status
        ORDER BY status
    """)
    print("\nChunk Processing Summary:")
    for status, count, orders in cur.fetchall():
        print(f"  {status}: {count} chunks, {orders or 0:,} orders")
    
    # Table counts
    tables = [
        ('staging.ops_orders_raw', 'Staging Orders'),
        ('ods.core_orders', 'ODS Orders'),
        ('ods.core_line_items', 'ODS Line Items'),
        ('dw.fact_orders', 'Fact Orders'),
        ('dw.fact_sales', 'Fact Sales')
    ]
    
    print("\nTable Row Counts:")
    for table, label in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table};")
        count = cur.fetchone()[0]
        print(f"  {label}: {count:,}")
    
    cur.close()
    conn.close()
    print("=" * 70)


# =============================================================================
# DAG Definition
# =============================================================================

with DAG(
    'stream_operations_pipeline',
    default_args=default_args,
    description='OPTIMIZED: Streaming operations pipeline with bulk inserts and vectorized transforms',
    schedule_interval=None,
    catchup=False,
    tags=['operations', 'streaming', 'parallel', 'pipeline', 'optimized'],
    max_active_tasks=6,  # Increased for better parallel processing
) as dag:
    
    # Setup
    t_setup = PythonOperator(
        task_id='setup_tracking_table',
        python_callable=setup_tracking_table,
    )
    
    t_analyze = PythonOperator(
        task_id='analyze_and_create_chunks',
        python_callable=analyze_source_data_and_create_chunks,
    )
    
    # Layer 1: Process staging
    t_staging = PythonOperator(
        task_id='process_staging_chunks',
        python_callable=process_staging_chunks,
    )
    
    # Sensor: Wait for staging data (reduced polling for less overhead)
    sensor_staging = PythonSensor(
        task_id='wait_for_staging_data',
        python_callable=check_staging_ready,
        poke_interval=2,
        timeout=30,
        soft_fail=True,
        mode='reschedule',  # Use reschedule mode to free worker slots
    )
    
    # Layer 2: Process ODS
    t_ods = PythonOperator(
        task_id='process_ods_chunks',
        python_callable=process_ods_chunks,
    )
    
    # Sensor: Wait for ODS data (reduced polling for less overhead)
    sensor_ods = PythonSensor(
        task_id='wait_for_ods_data',
        python_callable=check_ods_ready,
        poke_interval=2,
        timeout=30,
        soft_fail=True,
        mode='reschedule',  # Use reschedule mode to free worker slots
    )
    
    # Layer 3: Prepare DW dims
    t_dim_date = PythonOperator(
        task_id='ensure_dim_date',
        python_callable=ensure_dim_date,
    )
    
    t_dims = PythonOperator(
        task_id='process_dw_dims',
        python_callable=process_dw_dims_for_chunks,
    )
    
    # Sensor: Wait for dims (reduced polling for less overhead)
    sensor_dims = PythonSensor(
        task_id='wait_for_dims_ready',
        python_callable=check_dims_ready,
        poke_interval=2,
        timeout=30,
        soft_fail=True,
        mode='reschedule',  # Use reschedule mode to free worker slots
    )
    
    # Layer 4: Process facts
    t_facts = PythonOperator(
        task_id='process_fact_tables',
        python_callable=process_fact_tables_streaming,
    )
    
    # Verification
    t_verify = PythonOperator(
        task_id='verify_results',
        python_callable=verify_streaming_results,
    )
    
    # Dependencies - parallel streams
    t_setup >> t_analyze >> t_staging
    
    # Stream 1: Staging → ODS
    t_staging >> sensor_staging >> t_ods
    
    # Stream 2: ODS → DW Dims
    t_ods >> sensor_ods >> t_dim_date >> t_dims
    
    # Stream 3: Dims → Facts
    t_dims >> sensor_dims >> t_facts >> t_verify
