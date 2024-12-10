import sys
from pathlib import Path
import airflow
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
from scripts.query_duckdb import query_duckdb

# Add the scripts folder to the Python path
base_path = Path(__file__).resolve().parent.parent
scripts_path = base_path / 'scripts'
sys.path.append(str(scripts_path))

# Import the function from the scripts folder
from db_utils import load_to_duckdb

# Default args for the DAG
default_args_dict = {
    'start_date': datetime(2024, 12, 10),  # Set a specific start date
    'concurrency': 1,
    'schedule_interval': None,  # Adjust as needed for a schedule
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'cinema_stocks_pipeline',
    default_args=default_args_dict,
    schedule_interval=None,  # Adjust if needed for a schedule
    catchup=False,
)

# Define the task for loading data to DuckDB
load_to_duckdb_task = PythonOperator(
    task_id='load_to_duckdb',
    python_callable=load_to_duckdb,
    dag=dag,
)

# Define the task for querying DuckDB
query_duckdb_task = PythonOperator(
    task_id='query_duckdb',
    python_callable=query_duckdb,
    dag=dag,
)

# Define task dependencies
load_to_duckdb_task >> query_duckdb_task

