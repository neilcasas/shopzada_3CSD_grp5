from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'shopzada_main_dag',
    default_args=default_args,
    description='Main orchestration DAG for the complete Shopzada ETL pipeline',
    schedule_interval=None,
    catchup=False,
    tags=['master', 'orchestration', 'pipeline', 'shopzada'],
) as dag:

    """
    Shopzada Main ETL Pipeline
    ==========================
    
    This DAG orchestrates the complete ETL pipeline:
    
    1. test_postgres_connection - Verify database connectivity
    2. load_all_staging - Load raw data from source files into staging tables
    3. load_all_ods - Transform and load data from staging to ODS (typed, cleaned)
    4. load_all_dw - Populate Kimball star schema (dimensions and facts)
    
    Data Flow:
    Source Files → Staging (raw VARCHAR) → ODS (typed/cleaned) → DW (star schema)
    """

    start = EmptyOperator(task_id='start')

    # Step 1: Test database connection
    test_connection = TriggerDagRunOperator(
        task_id='test_postgres_connection',
        trigger_dag_id='test_postgres_connection',
        wait_for_completion=True,
        poke_interval=10,
    )

    # Step 2: Load all staging tables
    load_staging = TriggerDagRunOperator(
        task_id='load_all_staging',
        trigger_dag_id='load_all_staging',
        wait_for_completion=True,
        poke_interval=15,
    )

    # Step 3: Populate ODS layer
    load_ods = TriggerDagRunOperator(
        task_id='load_all_ods',
        trigger_dag_id='load_all_ods',
        wait_for_completion=True,
        poke_interval=15,
    )

    # Step 4: Populate DW layer (Kimball star schema)
    load_dw = TriggerDagRunOperator(
        task_id='load_all_dw',
        trigger_dag_id='load_all_dw',
        wait_for_completion=True,
        poke_interval=15,
    )

    end = EmptyOperator(task_id='end')

    # Sequential pipeline execution
    start >> test_connection >> load_staging >> load_ods >> load_dw >> end
