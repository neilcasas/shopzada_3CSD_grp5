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

ODS_DAG_IDS = [
    'populate_core_users',
    'populate_core_products',
    'populate_core_campaigns',
    'populate_core_enterprise',
    'populate_core_operations',
]

with DAG(
    'load_all_ods',
    default_args=default_args,
    description='Trigger all ODS population DAGs in parallel',
    schedule_interval=None,
    catchup=False,
    tags=['ods', 'all-departments', 'master'],
) as dag:
    start = EmptyOperator(task_id='start')
    finish = EmptyOperator(task_id='finish')

    for dag_id in ODS_DAG_IDS:
        trigger = TriggerDagRunOperator(
            task_id=f'trigger_{dag_id}',
            trigger_dag_id=dag_id,
            reset_dag_run=True,
            wait_for_completion=True,
            poke_interval=30,
        )
        start >> trigger >> finish
