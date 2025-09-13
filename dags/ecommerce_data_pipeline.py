"""
E-Commerce Data Pipeline DAG

This DAG orchestrates the ETL process for the e-commerce data warehouse.
It follows the dependency structure:
1. Dimension tables (dim_date, dim_customer, dim_product, dim_seller)
2. dim_campaign (depends on customer, product, seller)
3. fact_sales (depends on all dimensions)
4. fact_inventory, fact_cart, fact_marketing (depend on fact_sales)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
import sys
import os

# Add project root and common module to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.extend([
    project_root,
    os.path.join(project_root, 'common'),
])

# Import pipeline modules
from pipelines.dim_date import run as run_dim_date
from pipelines.dim_customer import run as run_dim_customer
from pipelines.dim_product import run as run_dim_product
from pipelines.dim_seller import run as run_dim_seller
from pipelines.dim_campaign import run as run_dim_campaign
from pipelines.fact_sales import run as run_fact_sales
from pipelines.fact_inventory import run as run_fact_inventory
from pipelines.fact_cart import run as run_fact_cart
from pipelines.fact_marketing import run as run_fact_marketing

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

# Define the DAG
dag = DAG(
    'ecommerce_data_pipeline',
    default_args=default_args,
    description='Orchestrates the ETL process for the e-commerce data warehouse',
    schedule_interval='@daily',
    catchup=False,
    tags=['ecommerce', 'etl'],
    max_active_runs=1,
)

# Define task functions
def run_pipeline(pipeline_func):
    """Wrapper function to run a pipeline with error handling."""
    try:
        pipeline_func()
        return True
    except Exception as e:
        print(f"Error running pipeline: {str(e)}")
        raise

# Create tasks
start_pipeline = DummyOperator(
    task_id='start_pipeline',
    dag=dag,
)

# Dimension tables
dim_date_task = PythonOperator(
    task_id='run_dim_date',
    python_callable=run_pipeline,
    op_kwargs={'pipeline_func': run_dim_date},
    dag=dag,
)

dim_customer_task = PythonOperator(
    task_id='run_dim_customer',
    python_callable=run_pipeline,
    op_kwargs={'pipeline_func': run_dim_customer},
    dag=dag,
)

dim_product_task = PythonOperator(
    task_id='run_dim_product',
    python_callable=run_pipeline,
    op_kwargs={'pipeline_func': run_dim_product},
    dag=dag,
)

dim_seller_task = PythonOperator(
    task_id='run_dim_seller',
    python_callable=run_pipeline,
    op_kwargs={'pipeline_func': run_dim_seller},
    dag=dag,
)

# dim_campaign depends on customer, product, and seller
dim_campaign_task = PythonOperator(
    task_id='run_dim_campaign',
    python_callable=run_pipeline,
    op_kwargs={'pipeline_func': run_dim_campaign},
    dag=dag,
)

# fact_sales depends on all dimensions
fact_sales_task = PythonOperator(
    task_id='run_fact_sales',
    python_callable=run_pipeline,
    op_kwargs={'pipeline_func': run_fact_sales},
    dag=dag,
)

# Fact tables that depend on fact_sales
fact_inventory_task = PythonOperator(
    task_id='run_fact_inventory',
    python_callable=run_pipeline,
    op_kwargs={'pipeline_func': run_fact_inventory},
    dag=dag,
)

fact_cart_task = PythonOperator(
    task_id='run_fact_cart',
    python_callable=run_pipeline,
    op_kwargs={'pipeline_func': run_fact_cart},
    dag=dag,
)

fact_marketing_task = PythonOperator(
    task_id='run_fact_marketing',
    python_callable=run_pipeline,
    op_kwargs={'pipeline_func': run_fact_marketing},
    dag=dag,
)

end_pipeline = DummyOperator(
    task_id='end_pipeline',
    dag=dag,
)

# Set up task dependencies
start_pipeline >> dim_date_task

# Set task dependencies
# dim_date must run first
dim_date_task >> [dim_customer_task, dim_product_task, dim_seller_task]

# dim_campaign depends on dim_customer, dim_product, and dim_seller
[dim_customer_task, dim_product_task, dim_seller_task] >> dim_campaign_task

# fact_sales depends on dim_campaign
dim_campaign_task >> fact_sales_task

# fact_inventory, fact_cart, fact_marketing can run in parallel after fact_sales
fact_sales_task >> [fact_inventory_task, fact_cart_task, fact_marketing_task]

# All fact tasks must complete before end_pipeline
[fact_inventory_task, fact_cart_task, fact_marketing_task] >> end_pipeline
