"""
Education ETL Pipeline DAG

This Airflow DAG orchestrates the school enrollment ETL pipeline
using the medallion architecture (Bronze -> Silver -> Gold).

Pipeline Flow:
    1. Bronze Ingestion: Load raw CSV data into Delta tables
    2. Silver Transformation: Clean and standardize data
    3. Gold Aggregation: Create business-level aggregations

Author: Data Engineering Team
Created: 2024
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Default arguments applied to all tasks in the DAG
default_args = {
    'owner': 'airflow'  # Owner of the DAG for tracking and notifications
}

# Define the DAG context
with DAG(
    dag_id='education_etl_pipeline',  # Unique identifier for the DAG
    # start_date=datetime(2024, 1, 1),  # Uncomment to set a specific start date
    schedule_interval='@once',  # Run once when triggered (manual execution)
    catchup=False  # Don't backfill missed runs
) as dag:

    # Task 1: Bronze Layer - Raw data ingestion
    # Reads CSV files and stores them as Delta tables in the Bronze layer
    bronze_task = BashOperator(
        task_id='run_bronze',
        bash_command='python notebooks/01_bronze_ingestion.py'
    )

    # Task 2: Silver Layer - Data cleaning and transformation
    # Removes nulls, casts data types, and applies business rules
    silver_task = BashOperator(
        task_id='run_silver',
        bash_command='python notebooks/02_silver_transformation.py'
    )

    # Task 3: Gold Layer - Business aggregations
    # Creates yearly enrollment summaries for reporting
    gold_task = BashOperator(
        task_id='run_gold',
        bash_command='python notebooks/03_gold_aggregation.py'
    )

    # Define task dependencies: Bronze -> Silver -> Gold (sequential execution)
    bronze_task >> silver_task >> gold_task