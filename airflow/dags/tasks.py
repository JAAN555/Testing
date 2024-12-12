import duckdb
import sys
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
#!pip install kagglehub
import shutil
import mlcroissant as mlc

#from scripts.ingest_from_kaggle import ingest_data_from_kagglehub
#from scripts.data_filtering import filter_files

import os
import kagglehub

def ingest_data_from_croissant(dataset_id: str, destination_path: str = "/opt/airflow/dags/datasets/stocks") -> str:
    """
    Ingest a dataset from Kaggle using mlcroissant and move it to the desired destination folder.

    Args:
        dataset_id (str): The Kaggle dataset identifier (e.g., 'jacksoncrow/stock-market-dataset').
        destination_path (str): The local path where the dataset should be moved. Defaults to '/opt/airflow/dags/datasets/stocks'.

    Returns:
        str: Path to the moved dataset files.
    """
    try:
        print(f"Attempting to download the dataset: {dataset_id}")
        
        # Fetch the Croissant JSON-LD dataset
        croissant_dataset = mlc.Dataset(f'www.kaggle.com/datasets/{dataset_id}/croissant/download')

        # Check the record sets in the dataset
        record_sets = croissant_dataset.metadata.record_sets
        print(f"Record sets found: {record_sets}")

        # Fetch the records and put them into a DataFrame
        record_set_df = pd.DataFrame(croissant_dataset.records(record_set=record_sets[0].uuid))
        
        # Ensure the destination directory exists
        if not os.path.exists(destination_path):
            os.makedirs(destination_path)
        
        # Define the file path where the records will be saved (CSV or other format)
        destination_file_path = os.path.join(destination_path, 'stock_market_data.csv')

        # Save the DataFrame to the destination file path
        record_set_df.to_csv(destination_file_path, index=False)
        print(f"Dataset successfully moved to: {destination_file_path}")

        # Verify that the file is saved in the destination folder
        if not os.path.exists(destination_file_path):
            print(f"Failed to move dataset to: {destination_file_path}")
            return None
        else:
            print(f"File successfully moved to: {destination_file_path}")
            return destination_file_path
    except Exception as e:
        print(f"Failed to download or move the dataset: {dataset_id}")
        print(f"Error: {e}")
        return None
    

#Test

import pandas as pd
import sys
import csv
import os
# Increase CSV field size limit
max_int = sys.maxsize
while True:
    try:
        csv.field_size_limit(max_int)
        break
    except OverflowError:
        max_int = int(max_int / 10)

def clean_and_split_genres(input_file, output_file):
    # Load the dataset
    df = pd.read_csv(
        input_file, 
        encoding='utf-8',
        delimiter=',',
        on_bad_lines='skip',  # Skips rows that cause parsing errors
        engine='python',      # Use the Python parser for more flexibility
    )

    # Ensure 'Genre' column exists
    if 'Genre' not in df.columns:
        raise ValueError("The dataset does not have a 'Genre' column.")

    # Split the 'Genre' column into individual genres and find unique genres
    genres = df['Genre'].dropna().str.split(', ').explode().unique()

    # Create a new binary column for each genre
    for genre in genres:
        df[genre] = df['Genre'].apply(
            lambda x: 1 if isinstance(x, str) and genre in x.split(', ') else 0
        )

    # Drop the original 'Genre' column
    df.drop(columns=['Genre'], inplace=True)

    # Save the cleaned dataset to a new CSV file
    df.to_csv(output_file, index=False)
    print(f"Cleaned dataset saved to {output_file}")

script_dir = os.path.dirname(os.path.abspath(__file__))

# Test ends


# Define the Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 12),
    'retries': 1,
}

dag = DAG(
    'download_stock_market_dataset',
    default_args=default_args,
    description='A DAG to load data into DuckDB',
    schedule_interval=None,  # Adjust as needed
)


# Task to download the 9000 movies dataset
'''download_movies_task = PythonOperator(
    task_id='download_9000_movies_dataset',
    python_callable=ingest_data_from_kagglehub,
    op_args=['disham993/9000-movies-dataset'],  # Dataset ID for 9000 movies
    op_kwargs={'destination_path': './data/movies'},
    dag = dag,
)'''

    # Task to download the stock market dataset
download_stock_market_task = PythonOperator(
    task_id='download_stock_market_dataset',
    python_callable=ingest_data_from_croissant,
    op_args=['jacksoncrow/stock-market-dataset'],  # Dataset ID for stock market
    op_kwargs={'destination_path': './data/stocks'},  # Specify the destination path
    dag=dag
)

# Task to filter stock market dataset
'''filter_task = PythonOperator(
    task_id='filter_files_task',
    python_callable=filter_files,
    op_args=['./data/stocks'],  # Asenda oma kausta teega
    dag=dag,
)'''
'''load_to_duckdb_task = PythonOperator(
    task_id='load_to_duckdb',
    python_callable=load_to_duckdb,
    dag=dag,
)


query_duckdb_task = PythonOperator(
    task_id='query_duckdb',
    python_callable=query_duckdb,
    dag=dag,
)
'''


script_dir = os.path.dirname(os.path.abspath(__file__))
input_file = os.path.join(script_dir, '../data/movies/mymoviedb_raw.csv')
output_file = os.path.join(script_dir, '../data/movies/cleaned_mymoviedb2.csv')

# Define the PythonOperator task
clean_genres_task = PythonOperator(
    task_id='clean_and_split_genres',
    python_callable=clean_and_split_genres,
    op_args=[input_file, output_file],  # Pass input and output file paths
    dag=dag,
)


# Set task dependencies

# Will change a lot


#load_to_duckdb_task >> query_duckdb_task

##download_stock_market_task #>> filter_task

download_stock_market_task >> clean_genres_task ##>> clean_genres_task
