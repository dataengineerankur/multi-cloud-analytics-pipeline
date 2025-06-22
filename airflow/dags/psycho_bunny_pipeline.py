"""
Psycho Bunny Data Pipeline DAG - Minimal Version
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Create DAG
dag = DAG(
    'psycho_bunny_data_pipeline',
    default_args=default_args,
    description='Data pipeline for Psycho Bunny analytics',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['psycho-bunny', 'data-pipeline']
)

# Pipeline base path
PIPELINE_PATH = "/opt/airflow/dags/datapipeline"

# Task 1: Data Ingestion
data_ingestion = BashOperator(
    task_id='data_ingestion',
    bash_command=f'cd {PIPELINE_PATH} && python scripts/01_data_ingestion.py',
    dag=dag
)

# Task 2: Data Quality Validation
data_quality = BashOperator(
    task_id='data_quality_validation',
    bash_command=f'cd {PIPELINE_PATH} && python scripts/02_data_quality_validation.py',
    dag=dag
)

# Task 3: Data Processing
data_processing = BashOperator(
    task_id='data_processing',
    bash_command=f'cd {PIPELINE_PATH} && python scripts/03_data_processing.py',
    dag=dag
)

# Task 4: Load to Redshift
load_redshift = BashOperator(
    task_id='load_to_redshift',
    bash_command=f'cd {PIPELINE_PATH} && python scripts/04_load_to_redshift.py',
    dag=dag
)

# Task 5: Generate Analytics
generate_analytics = BashOperator(
    task_id='generate_analytics',
    bash_command=f'cd {PIPELINE_PATH} && python scripts/06_generate_analytics.py',
    dag=dag
)

# Define task dependencies
data_ingestion >> data_quality >> data_processing >> load_redshift >> generate_analytics 