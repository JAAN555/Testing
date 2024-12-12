import duckdb
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define the function to load data into DuckDB
def load_to_duckdb():
    # File path for the source data
    csv_file_path = 'C:/Users/PC/Documents/Testing/airflow/data/movies/cleaned_mymoviedb.csv'

    # DuckDB database file (creates a new one if it doesn't exist)
    duckdb_file_path = 'C:/Users/PC/Documents/Testing/airflow/data/movies/database.duckdb'

    # Table name in DuckDB
    table_name = 'movies'

    # Read the data from CSV using pandas
    df = pd.read_csv(csv_file_path)

    # Connect to DuckDB and load the data
    conn = duckdb.connect(duckdb_file_path)
    conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df LIMIT 0")
    conn.register('data_frame', df)
    conn.execute(f"INSERT INTO {table_name} SELECT * FROM data_frame")

    # Print success message
    print(f"Data successfully loaded into DuckDB table: {table_name}")

# Define the function to query data from DuckDB
def query_duckdb():
    # DuckDB database file
    duckdb_file_path = '/opt/airflow/data/movies/database.duckdb'

    # Query to fetch data
    query = "SELECT * FROM movies LIMIT 10"

    # Connect to DuckDB and execute the query
    conn = duckdb.connect(duckdb_file_path)
    result = conn.execute(query).fetchall()

    # Print query result
    print("Query Results:")
    for row in result:
        print(row)

# Define the Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 12),
    'retries': 1,
}

dag = DAG(
    'load_to_duckdb_dag',
    default_args=default_args,
    description='A DAG to load data into DuckDB',
    schedule_interval=None,  # Adjust as needed
)

load_to_duckdb_task = PythonOperator(
    task_id='load_to_duckdb',
    python_callable=load_to_duckdb,
    dag=dag,
)


query_duckdb_task = PythonOperator(
    task_id='query_duckdb',
    python_callable=query_duckdb,
    dag=dag,
)

# Set task dependencies
load_to_duckdb_task >> query_duckdb_task
