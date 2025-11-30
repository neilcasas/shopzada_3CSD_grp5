from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
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

def test_postgres_connection():
    """Test connection to PostgreSQL and print all schemas."""
    try:
        # Database connection parameters
        conn_params = {
            'host': os.getenv('POSTGRES_HOST', 'postgres'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'database': 'shopzada',
            'user': os.getenv('POSTGRES_USER', 'airflow'),
            'password': os.getenv('POSTGRES_PASSWORD', 'airflow')
        }
        
        print(f"Attempting to connect to database: {conn_params['database']} at {conn_params['host']}:{conn_params['port']}")
        
        # Establish connection
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        print("✓ Successfully connected to PostgreSQL database!")
        
        # Get PostgreSQL version
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"\nPostgreSQL Version:\n{version}\n")
        
        # Get all schemas
        cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            ORDER BY schema_name;
        """)
        
        schemas = cursor.fetchall()
        
        print(f"Total Schemas Found: {len(schemas)}\n")
        print("=" * 50)
        print("Available Schemas in 'shopzada' Database:")
        print("=" * 50)
        
        for idx, (schema,) in enumerate(schemas, 1):
            print(f"{idx}. {schema}")
            
            # Get table count for each schema
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = %s AND table_type = 'BASE TABLE';
            """, (schema,))
            
            table_count = cursor.fetchone()[0]
            print(f"   └─ Tables: {table_count}")
        
        print("=" * 50)
        
        # Close connection
        cursor.close()
        conn.close()
        
        print("\n✓ Connection test completed successfully!")
        
    except Exception as e:
        print(f"✗ Error connecting to PostgreSQL: {str(e)}")
        raise

with DAG(
    'test_postgres_connection',
    default_args=default_args,
    description='Test PostgreSQL connection and list all schemas in shopzada database',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['test', 'postgres', 'connection'],
) as dag:
    
    test_connection = PythonOperator(
        task_id='test_postgres_connection',
        python_callable=test_postgres_connection,
    )
