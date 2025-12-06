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
    'load_all_dw',
    default_args=default_args,
    description='Master DAG to orchestrate all DW (Kimball) layer DAGs',
    schedule_interval=None,
    catchup=False,
    tags=['dw', 'kimball', 'master', 'orchestration'],
) as dag:

    start = EmptyOperator(task_id='start')
    
    # Dimension DAGs - can run in parallel
    trigger_dim_staff = TriggerDagRunOperator(
        task_id='trigger_dim_staff',
        trigger_dag_id='populate_dim_staff',
        wait_for_completion=True,
        poke_interval=10,
    )

    trigger_dim_campaign = TriggerDagRunOperator(
        task_id='trigger_dim_campaign',
        trigger_dag_id='populate_dim_campaign',
        wait_for_completion=True,
        poke_interval=10,
    )

    trigger_dim_merchant = TriggerDagRunOperator(
        task_id='trigger_dim_merchant',
        trigger_dag_id='populate_dim_merchant',
        wait_for_completion=True,
        poke_interval=10,
    )

    dimensions_complete = EmptyOperator(task_id='dimensions_complete')

    # Fact DAGs - depend on dimensions being complete
    trigger_fact_sales = TriggerDagRunOperator(
        task_id='trigger_fact_sales',
        trigger_dag_id='populate_fact_sales',
        wait_for_completion=True,
        poke_interval=10,
    )

    trigger_fact_orders = TriggerDagRunOperator(
        task_id='trigger_fact_orders',
        trigger_dag_id='populate_fact_orders',
        wait_for_completion=True,
        poke_interval=10,
    )

    trigger_fact_campaign_response = TriggerDagRunOperator(
        task_id='trigger_fact_campaign_response',
        trigger_dag_id='populate_fact_campaign_response',
        wait_for_completion=True,
        poke_interval=10,
    )

    end = EmptyOperator(task_id='end')

    # Dependencies
    # Start -> Dimensions (parallel) -> dimensions_complete -> Facts (parallel) -> end
    start >> [trigger_dim_staff, trigger_dim_campaign, trigger_dim_merchant] >> dimensions_complete
    dimensions_complete >> [trigger_fact_sales, trigger_fact_orders, trigger_fact_campaign_response] >> end
