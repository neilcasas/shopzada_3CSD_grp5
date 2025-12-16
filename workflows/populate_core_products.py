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
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER', 'airflow')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'airflow')}@"
        f"{os.getenv('POSTGRES_HOST', 'postgres')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/shopzada"
    )

def extract_from_staging(**context):
    """Extract product data from staging table."""
    engine = get_db_engine()
    
    print("=" * 70)
    print("EXTRACTING DATA FROM STAGING SCHEMA")
    print("=" * 70)
    
    try:
        # Extract from staging table
        print("Extracting products...")
        products = pd.read_sql(
            "SELECT * FROM staging.biz_products_raw",
            engine
        )
        print(f"  ✓ Loaded {len(products)} products from staging")
        
        # Store in XCom
        context['ti'].xcom_push(key='products', value=products.to_json(orient='split'))
        
        print("\n✓ Staging data extracted successfully")
        
    except Exception as e:
        print(f"✗ Error extracting from staging: {str(e)}")
        raise

def transform_and_clean(**context):
    """Transform and clean product data from staging."""
    ti = context['ti']
    
    print("=" * 70)
    print("TRANSFORMING AND CLEANING DATA")
    print("=" * 70)
    
    # Retrieve data from XCom
    products = pd.read_json(
        ti.xcom_pull(key='products', task_ids='extract_from_staging'),
        orient='split'
    )
    
    print(f"Products shape: {products.shape}")
    print(f"Columns: {products.columns.tolist()}")
    
    # Remove duplicates based on product_id
    initial_count = len(products)
    products = products.drop_duplicates(subset=['product_id'], keep='first')
    print(f"After deduplication: {len(products)} products (removed {initial_count - len(products)} duplicates)")
    
    # Clean anomalies
    print("\nCleaning data...")
    
    # Remove records with missing critical fields
    initial_count = len(products)
    products = products[products['product_id'].notna()]
    products = products[products['product_id'].astype(str).str.strip() != '']
    products = products[products['product_id'].astype(str).str.lower() != 'nan']
    
    if 'product_name' in products.columns:
        products = products[products['product_name'].notna()]
        products = products[products['product_name'].astype(str).str.strip() != '']
        products = products[products['product_name'].astype(str).str.lower() != 'nan']
    
    print(f"  → Removed {initial_count - len(products)} records with missing critical fields")
    
    # Clean price field (convert to float, handle invalid values)
    if 'price' in products.columns:
        print("  → Cleaning price values...")
        
        # Convert to string first, remove any currency symbols, commas
        products['price'] = products['price'].astype(str).str.strip()
        products['price'] = products['price'].str.replace('$', '', regex=False)
        products['price'] = products['price'].str.replace(',', '', regex=False)
        products['price'] = products['price'].str.replace('₱', '', regex=False)
        
        # Convert to numeric, coerce errors to NaN
        products['price'] = pd.to_numeric(products['price'], errors='coerce')
        
        # Remove products with invalid prices (null, negative, or zero)
        initial_count = len(products)
        products = products[products['price'].notna()]
        products = products[products['price'] > 0]
        print(f"  → Removed {initial_count - len(products)} products with invalid prices")
    
    # Standardize product_type (title case)
    if 'product_type' in products.columns:
        products['product_type'] = products['product_type'].astype(str).str.strip().str.title()
        products['product_type'] = products['product_type'].replace('Nan', None)
    
    # Standardize product_name (title case)
    if 'product_name' in products.columns:
        products['product_name'] = products['product_name'].astype(str).str.strip()
    
    # Ensure product_id is clean string
    products['product_id'] = products['product_id'].astype(str).str.strip()
    
    # Remove any remaining duplicates after cleaning
    products = products.drop_duplicates(subset=['product_id'], keep='first')
    
    # Drop metadata columns from staging
    metadata_cols = ['_source_file', '_ingested_at', 'raw_index']
    products = products.drop(columns=[col for col in metadata_cols if col in products.columns], errors='ignore')
    
    print(f"\n✓ Final cleaned data shape: {products.shape}")
    print(f"  Columns: {products.columns.tolist()}")
    if not products.empty:
        print(f"  Price range: ${products['price'].min():.2f} - ${products['price'].max():.2f}")
    else:
        print("  No products to process.")
    
    # Store in XCom
    context['ti'].xcom_push(key='cleaned_data', value=products.to_json(orient='split'))

def load_to_ods(**context):
    """Load cleaned product data into ods.core_products table."""
    ti = context['ti']
    engine = get_db_engine()
    
    print("=" * 70)
    print("LOADING DATA TO ODS.CORE_PRODUCTS")
    print("=" * 70)
    
    # Retrieve cleaned data
    cleaned_data = pd.read_json(
        ti.xcom_pull(key='cleaned_data', task_ids='transform_and_clean'),
        orient='split'
    )
    
    print(f"Loading {len(cleaned_data)} records to ods.core_products")
    
    conn = engine.raw_connection()
    cursor = conn.cursor()
    
    # Truncate table first
    print("Truncating ods.core_products table...")
    cursor.execute("TRUNCATE TABLE ods.core_products CASCADE;")
    conn.commit()
    
    # Get the columns available in the table
    cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'ods' AND table_name = 'core_products'
        ORDER BY ordinal_position;
    """)
    db_columns = [row[0] for row in cursor.fetchall()]
    print(f"Database columns: {db_columns}")
    
    # Map dataframe columns to database columns
    available_columns = [col for col in db_columns if col in cleaned_data.columns]
    print(f"Columns to insert: {available_columns}")
    
    # Insert data
    insert_count = 0
    error_count = 0
    
    for _, row in cleaned_data.iterrows():
        try:
            # Build dynamic INSERT statement
            placeholders = ', '.join(['%s'] * len(available_columns))
            columns_str = ', '.join(available_columns)
            
            values = tuple(row.get(col) for col in available_columns)
            
            cursor.execute(
                f"INSERT INTO ods.core_products ({columns_str}) VALUES ({placeholders})",
                values
            )
            insert_count += 1
        except Exception as e:
            error_count += 1
            if error_count <= 5:  # Only print first 5 errors
                print(f"  ✗ Error inserting product {row.get('product_id')}: {str(e)}")
    
    conn.commit()
    
    # Verify the load
    cursor.execute("SELECT COUNT(*) FROM ods.core_products;")
    final_count = cursor.fetchone()[0]
    
    # Get some statistics
    cursor.execute("""
        SELECT 
            COUNT(*) as total_products,
            COUNT(DISTINCT product_type) as product_types,
            MIN(price) as min_price,
            MAX(price) as max_price,
            AVG(price) as avg_price
        FROM ods.core_products;
    """)
    stats = cursor.fetchone()
    
    print(f"\n✓ Successfully inserted {insert_count} records")
    if error_count > 0:
        print(f"  ⚠ Failed inserts: {error_count}")
    print(f"\nFinal statistics:")
    print(f"  Total products: {stats[0]}")
    print(f"  Product types: {stats[1]}")
    if stats[0] > 0:
        print(f"  Price range: ${stats[2]:.2f} - ${stats[3]:.2f}")
        print(f"  Average price: ${stats[4]:.2f}")
    else:
        print("  No products loaded.")
    
    cursor.close()
    conn.close()
    
    print("=" * 70)

with DAG(
    'populate_core_products',
    default_args=default_args,
    description='Extract product data from staging, transform, and load to ods.core_products',
    schedule_interval=None,
    catchup=False,
    tags=['etl', 'ods', 'business', 'products', 'staging-to-ods'],
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_from_staging',
        python_callable=extract_from_staging,
    )
    
    transform_task = PythonOperator(
        task_id='transform_and_clean',
        python_callable=transform_and_clean,
    )
    
    load_task = PythonOperator(
        task_id='load_to_ods',
        python_callable=load_to_ods,
    )
    
    # Set dependencies
    extract_task >> transform_task >> load_task
