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