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
    """Extract campaign data from staging table."""
    engine = get_db_engine()
    
    print("=" * 70)
    print("EXTRACTING DATA FROM STAGING SCHEMA")
    print("=" * 70)
    
    try:
        # Extract from staging table
        print("Extracting campaigns...")
        campaigns = pd.read_sql(
            "SELECT * FROM staging.mkt_campaigns_raw",
            engine
        )
        print(f"  ✓ Loaded {len(campaigns)} campaigns from staging")
        
        # Store in XCom
        context['ti'].xcom_push(key='campaigns', value=campaigns.to_json(orient='split'))
        
        print("\n✓ Staging data extracted successfully")
        
    except Exception as e:
        print(f"✗ Error extracting from staging: {str(e)}")
        raise

def transform_and_clean(**context):
    """Transform and clean campaign data from staging."""
    ti = context['ti']
    
    print("=" * 70)
    print("TRANSFORMING AND CLEANING DATA")
    print("=" * 70)
    
    # Retrieve data from XCom
    campaigns = pd.read_json(
        ti.xcom_pull(key='campaigns', task_ids='extract_from_staging'),
        orient='split'
    )
    
    print(f"Campaigns shape: {campaigns.shape}")
    print(f"Columns: {campaigns.columns.tolist()}")
    
    # Remove duplicates based on campaign_id
    initial_count = len(campaigns)
    campaigns = campaigns.drop_duplicates(subset=['campaign_id'], keep='first')
    print(f"After deduplication: {len(campaigns)} campaigns (removed {initial_count - len(campaigns)} duplicates)")
    
    # Clean anomalies
    print("\nCleaning data...")
    
    # Remove records with missing critical fields
    initial_count = len(campaigns)
    campaigns = campaigns[campaigns['campaign_id'].notna()]
    campaigns = campaigns[campaigns['campaign_id'].astype(str).str.strip() != '']
    campaigns = campaigns[campaigns['campaign_id'].astype(str).str.lower() != 'nan']
    
    if 'campaign_name' in campaigns.columns:
        campaigns = campaigns[campaigns['campaign_name'].notna()]
        campaigns = campaigns[campaigns['campaign_name'].astype(str).str.strip() != '']
        campaigns = campaigns[campaigns['campaign_name'].astype(str).str.lower() != 'nan']
    
    print(f"  → Removed {initial_count - len(campaigns)} records with missing critical fields")
    
    # Clean discount field (convert to float, handle invalid values)
    if 'discount' in campaigns.columns:
        print("  → Cleaning discount values...")
        
        # Convert to string first, remove any % symbols
        campaigns['discount'] = campaigns['discount'].astype(str).str.strip()
        campaigns['discount'] = campaigns['discount'].str.replace('%', '', regex=False)
        
        # Convert to numeric, coerce errors to NaN
        campaigns['discount'] = pd.to_numeric(campaigns['discount'], errors='coerce')
        
        # Set discount to 0 if null
        campaigns['discount'] = campaigns['discount'].fillna(0)
        
        # Ensure discount is between 0 and 100
        campaigns.loc[campaigns['discount'] < 0, 'discount'] = 0
        campaigns.loc[campaigns['discount'] > 100, 'discount'] = 100
        
        if not campaigns.empty:
            print(f"  → Discount range: {campaigns['discount'].min():.2f}% - {campaigns['discount'].max():.2f}%")
        else:
            print("  → No campaigns to process.")
    
    # Standardize campaign_name (strip whitespace)
    if 'campaign_name' in campaigns.columns:
        campaigns['campaign_name'] = campaigns['campaign_name'].astype(str).str.strip()
    
    # Clean campaign_description
    if 'campaign_description' in campaigns.columns:
        campaigns['campaign_description'] = campaigns['campaign_description'].astype(str).str.strip()
        campaigns['campaign_description'] = campaigns['campaign_description'].replace('nan', None)
        campaigns['campaign_description'] = campaigns['campaign_description'].replace('Nan', None)
        campaigns['campaign_description'] = campaigns['campaign_description'].replace('NaN', None)
    
    # Ensure campaign_id is clean string
    campaigns['campaign_id'] = campaigns['campaign_id'].astype(str).str.strip()
    
    # Remove any remaining duplicates after cleaning
    campaigns = campaigns.drop_duplicates(subset=['campaign_id'], keep='first')
    
    # Drop metadata columns from staging
    metadata_cols = ['_source_file', '_ingested_at', 'raw_index']
    campaigns = campaigns.drop(columns=[col for col in metadata_cols if col in campaigns.columns], errors='ignore')
    
    print(f"\n✓ Final cleaned data shape: {campaigns.shape}")
    print(f"  Columns: {campaigns.columns.tolist()}")
    print(f"  Average discount: {campaigns['discount'].mean():.2f}%")
    
    # Store in XCom
    context['ti'].xcom_push(key='cleaned_data', value=campaigns.to_json(orient='split'))

def load_to_ods(**context):
    """Load cleaned campaign data into ods.core_campaigns table."""
    ti = context['ti']
    engine = get_db_engine()
    
    print("=" * 70)
    print("LOADING DATA TO ODS.CORE_CAMPAIGNS")
    print("=" * 70)
    
    # Retrieve cleaned data
    cleaned_data = pd.read_json(
        ti.xcom_pull(key='cleaned_data', task_ids='transform_and_clean'),
        orient='split'
    )
    
    print(f"Loading {len(cleaned_data)} records to ods.core_campaigns")
    
    conn = engine.raw_connection()
    cursor = conn.cursor()
    
    # Truncate table first
    print("Truncating ods.core_campaigns table...")
    cursor.execute("TRUNCATE TABLE ods.core_campaigns CASCADE;")
    conn.commit()
    
    # Get the columns available in the table
    cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'ods' AND table_name = 'core_campaigns'
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
                f"INSERT INTO ods.core_campaigns ({columns_str}) VALUES ({placeholders})",
                values
            )
            insert_count += 1
        except Exception as e:
            error_count += 1
            if error_count <= 5:  # Only print first 5 errors
                print(f"  ✗ Error inserting campaign {row.get('campaign_id')}: {str(e)}")
    
    conn.commit()
    
    # Verify the load
    cursor.execute("SELECT COUNT(*) FROM ods.core_campaigns;")
    final_count = cursor.fetchone()[0]
    
    # Get some statistics
    cursor.execute("""
        SELECT 
            COUNT(*) as total_campaigns,
            MIN(discount) as min_discount,
            MAX(discount) as max_discount,
            AVG(discount) as avg_discount
        FROM ods.core_campaigns;
    """)
    stats = cursor.fetchone()
    
    print(f"\n✓ Successfully inserted {insert_count} records")
    if error_count > 0:
        print(f"  ⚠ Failed inserts: {error_count}")
    print(f"\nFinal statistics:")
    print(f"  Total campaigns: {stats[0]}")
    if stats[0] > 0:
        print(f"  Discount range: {stats[1]:.2f}% - {stats[2]:.2f}%")
        print(f"  Average discount: {stats[3]:.2f}%")
    else:
        print("  No campaigns loaded.")
    
    cursor.close()
    conn.close()
    
    print("=" * 70)

with DAG(
    'populate_core_campaigns',
    default_args=default_args,
    description='Extract campaign data from staging, transform, and load to ods.core_campaigns',
    schedule_interval=None,
    catchup=False,
    tags=['etl', 'ods', 'marketing', 'campaigns', 'staging-to-ods'],
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
