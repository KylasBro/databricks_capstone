from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow'
}

with DAG(
    dag_id='education_etl_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    bronze_task = BashOperator(
        task_id='run_bronze',
        bash_command='python notebooks/01_bronze_ingestion.py'
    )

    silver_task = BashOperator(
        task_id='run_silver',
        bash_command='python notebooks/02_silver_transformation.py'
    )

    gold_task = BashOperator(
        task_id='run_gold',
        bash_command='python notebooks/03_gold_aggregation.py'
    )

    bronze_task >> silver_task >> gold_task