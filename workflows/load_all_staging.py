from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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

DEPARTMENT_DAG_IDS = [
    'load_business_staging',
    'load_customer_staging',
    'load_enterprise_staging',
    'load_marketing_staging',
    'load_operations_staging',
]

with DAG(
    'load_all_staging',
    default_args=default_args,
    description='Trigger all department-specific staging DAGs in sequence',
    schedule_interval=None,
    catchup=False,
    tags=['staging', 'all-departments', 'raw', 'master'],
) as dag:
    start = EmptyOperator(task_id='start')
    finish = EmptyOperator(task_id='finish')

    for dag_id in DEPARTMENT_DAG_IDS:
        trigger = TriggerDagRunOperator(
            task_id=f'trigger_{dag_id}',
            trigger_dag_id=dag_id,
            reset_dag_run=True,
            wait_for_completion=True,
            poke_interval=30,
        )
        start >> trigger >> finish
