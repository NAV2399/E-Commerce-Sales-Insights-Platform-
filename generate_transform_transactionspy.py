from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import subprocess

# Default settings
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Python callable to run the ETL script
def run_etl_script():
    script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../generate_transform_transactions.py'))
    subprocess.run(['python', script_path], check=True)

# DAG definition
with DAG(
    'daily_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Run daily ETL job for E-Commerce Sales Pipeline',
    tags=['ecommerce', 'etl', 'daily']
) as dag:

    run_etl = PythonOperator(
        task_id='run_etl_script',
        python_callable=run_etl_script
    )

    run_etl
