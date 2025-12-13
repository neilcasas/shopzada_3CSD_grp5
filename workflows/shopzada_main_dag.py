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
    
    This DAG orchestrates the complete ETL pipeline BY DEPARTMENT in parallel:
    
    Each department runs its full pipeline (staging → ODS → DW) independently:
    - Business: Products pipeline
    - Customer: Users pipeline  
    - Enterprise: Merchants & Staff pipeline
    - Marketing: Campaigns pipeline
    - Operations: Orders & Sales pipeline (the slowest, now runs in parallel)
    
    This significantly improves performance since Operations doesn't block other departments.
    """

    start = EmptyOperator(task_id='start')

    # Step 1: Test database connection
    test_connection = TriggerDagRunOperator(
        task_id='test_postgres_connection',
        trigger_dag_id='test_postgres_connection',
        wait_for_completion=True,
        poke_interval=10,
    )

    # =========================================================================
    # BUSINESS DEPARTMENT PIPELINE (Products)
    # staging → ODS → DW (dim_product)
    # =========================================================================
    biz_staging = TriggerDagRunOperator(
        task_id='biz_load_staging',
        trigger_dag_id='load_business_staging',
        wait_for_completion=True,
        poke_interval=10,
    )
    biz_ods = TriggerDagRunOperator(
        task_id='biz_load_ods',
        trigger_dag_id='populate_core_products',
        wait_for_completion=True,
        poke_interval=10,
    )
    biz_dw = TriggerDagRunOperator(
        task_id='biz_load_dw',
        trigger_dag_id='populate_dim_product',
        wait_for_completion=True,
        poke_interval=10,
    )
    biz_complete = EmptyOperator(task_id='biz_complete')

    # =========================================================================
    # CUSTOMER DEPARTMENT PIPELINE (Users)
    # staging → ODS → DW (dim_user)
    # =========================================================================
    cust_staging = TriggerDagRunOperator(
        task_id='cust_load_staging',
        trigger_dag_id='load_customer_staging',
        wait_for_completion=True,
        poke_interval=10,
    )
    cust_ods = TriggerDagRunOperator(
        task_id='cust_load_ods',
        trigger_dag_id='populate_core_users',
        wait_for_completion=True,
        poke_interval=10,
    )
    cust_dw = TriggerDagRunOperator(
        task_id='cust_load_dw',
        trigger_dag_id='populate_dim_user',
        wait_for_completion=True,
        poke_interval=10,
    )
    cust_complete = EmptyOperator(task_id='cust_complete')

    # =========================================================================
    # ENTERPRISE DEPARTMENT PIPELINE (Merchants & Staff)
    # staging → ODS → DW (dim_merchant, dim_staff)
    # =========================================================================
    ent_staging = TriggerDagRunOperator(
        task_id='ent_load_staging',
        trigger_dag_id='load_enterprise_staging',
        wait_for_completion=True,
        poke_interval=10,
    )
    ent_ods = TriggerDagRunOperator(
        task_id='ent_load_ods',
        trigger_dag_id='populate_core_enterprise',
        wait_for_completion=True,
        poke_interval=10,
    )
    ent_dw_merchant = TriggerDagRunOperator(
        task_id='ent_load_dw_merchant',
        trigger_dag_id='populate_dim_merchant',
        wait_for_completion=True,
        poke_interval=10,
    )
    ent_dw_staff = TriggerDagRunOperator(
        task_id='ent_load_dw_staff',
        trigger_dag_id='populate_dim_staff',
        wait_for_completion=True,
        poke_interval=10,
    )
    ent_complete = EmptyOperator(task_id='ent_complete')

    # =========================================================================
    # MARKETING DEPARTMENT PIPELINE (Campaigns)
    # staging → ODS → DW (dim_campaign)
    # =========================================================================
    mkt_staging = TriggerDagRunOperator(
        task_id='mkt_load_staging',
        trigger_dag_id='load_marketing_staging',
        wait_for_completion=True,
        poke_interval=10,
    )
    mkt_ods = TriggerDagRunOperator(
        task_id='mkt_load_ods',
        trigger_dag_id='populate_core_campaigns',
        wait_for_completion=True,
        poke_interval=10,
    )
    mkt_dw = TriggerDagRunOperator(
        task_id='mkt_load_dw',
        trigger_dag_id='populate_dim_campaign',
        wait_for_completion=True,
        poke_interval=10,
    )
    mkt_complete = EmptyOperator(task_id='mkt_complete')

    # =========================================================================
    # OPERATIONS DEPARTMENT PIPELINE (Orders, Line Items, Sales)
    # staging → ODS → DW (fact_sales, fact_orders)
    # This is the slowest pipeline - now runs in parallel with others!
    # =========================================================================
    ops_staging = TriggerDagRunOperator(
        task_id='ops_load_staging',
        trigger_dag_id='load_operations_staging',
        wait_for_completion=True,
        poke_interval=10,
    )
    ops_ods = TriggerDagRunOperator(
        task_id='ops_load_ods',
        trigger_dag_id='populate_core_operations',
        wait_for_completion=True,
        poke_interval=10,
    )
    ops_complete = EmptyOperator(task_id='ops_complete')

    # =========================================================================
    # FACT TABLES (depend on ALL dimensions being complete)
    # =========================================================================
    all_dims_complete = EmptyOperator(task_id='all_dims_complete')

    fact_sales = TriggerDagRunOperator(
        task_id='load_fact_sales',
        trigger_dag_id='populate_fact_sales',
        wait_for_completion=True,
        poke_interval=10,
    )
    fact_orders = TriggerDagRunOperator(
        task_id='load_fact_orders',
        trigger_dag_id='populate_fact_orders',
        wait_for_completion=True,
        poke_interval=10,
    )
    fact_campaign_response = TriggerDagRunOperator(
        task_id='load_fact_campaign_response',
        trigger_dag_id='populate_fact_campaign_response',
        wait_for_completion=True,
        poke_interval=10,
    )

    end = EmptyOperator(task_id='end')

    # =========================================================================
    # DEPENDENCIES - Each department pipeline runs in parallel
    # =========================================================================
    
    # After connection test, all department staging loads start in parallel
    test_connection >> [biz_staging, cust_staging, ent_staging, mkt_staging, ops_staging]

    # Business pipeline: staging → ODS → DW
    start >> test_connection
    biz_staging >> biz_ods >> biz_dw >> biz_complete

    # Customer pipeline: staging → ODS → DW
    cust_staging >> cust_ods >> cust_dw >> cust_complete

    # Enterprise pipeline: staging → ODS → DW (merchant & staff in parallel)
    ent_staging >> ent_ods >> [ent_dw_merchant, ent_dw_staff] >> ent_complete

    # Marketing pipeline: staging → ODS → DW
    mkt_staging >> mkt_ods >> mkt_dw >> mkt_complete

    # Operations pipeline: staging → ODS (facts depend on all dims)
    ops_staging >> ops_ods >> ops_complete

    # Facts can only run after ALL dimensions are complete AND operations ODS is done
    [biz_complete, cust_complete, ent_complete, mkt_complete, ops_complete] >> all_dims_complete
    all_dims_complete >> [fact_sales, fact_orders, fact_campaign_response] >> end
