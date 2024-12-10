import sys
from pathlib import Path
import airflow
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime

# Add the scripts folder to the Python path
base_path = Path(__file__).resolve().parent.parent
scripts_path = base_path / 'scripts'
sys.path.append(str(scripts_path))

# Import the function from the scripts folder
from db_utils import load_to_duckdb

# DAG default arguments
default_args = {
    'start_date': datetime(2024, 12, 1),
}

# Define the DAG
dag = DAG(
    'cinema_stocks_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

# Define the task
load_to_duckdb_task = PythonOperator(
    task_id='load_to_duckdb',
    python_callable=load_to_duckdb,
    dag=dag,
)
