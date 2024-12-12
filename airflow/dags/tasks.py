import duckdb
import sys
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import kagglehub
#!pip install kagglehub
import shutil
import mlcroissant as mlc

#from scripts.ingest_from_kaggle import ingest_data_from_kagglehub
#from scripts.data_filtering import filter_files


def ingest_data_from_kagglehub(dataset_id: str, destination_path: str = "/opt/airflow/data_raw/stocks_raw") -> str:
    """
    Ingest a dataset from Kaggle using kagglehub and move it to the desired destination folder.

    Args:
        dataset_id (str): The Kaggle dataset identifier (e.g., 'jacksoncrow/stock-market-dataset').
        location_path (str): The local path where the dataset should be moved. Defaults to '/opt/airflow/dags/datasets/stocks'.

    Returns:
        str: Path to the downloaded dataset folder.
    """
    try:
        print(f"Attempting to download the dataset: {dataset_id}")
        
        # Ensure the destination path exists
        if not os.path.exists(destination_path):
            os.makedirs(destination_path)

        # Download the dataset from Kaggle using kagglehub
        dataset_path = kagglehub.dataset_download(dataset_id)
        
        print(f"Dataset downloaded to: {dataset_path}")
        
        # Now move the dataset to the desired destination folder
        destination_full_path = os.path.join(destination_path, os.path.basename(dataset_path))
        
        # Move files from the downloaded folder to the destination
        if os.path.exists(dataset_path):
            shutil.move(dataset_path, destination_full_path)
            print(f"Dataset successfully moved to: {destination_full_path}")
        else:
            print(f"Failed to find dataset at {dataset_path}")
            return None
        
        # Verify that the dataset files were moved successfully
        if not os.path.exists(destination_full_path):
            print(f"Failed to move the dataset to: {destination_full_path}")
            return None
        else:
            print(f"Dataset successfully moved to: {destination_full_path}")
            return destination_full_path

    except Exception as e:
        print(f"Failed to download or move the dataset: {dataset_id}")
        print(f"Error: {e}")
        return None
    

def rename_prn_file(stocks_folder: str):
    try:
        old_file = os.path.join(stocks_folder, 'PRN.csv')
        new_file = os.path.join(stocks_folder, 'PRN_1.csv')
        
        if os.path.exists(old_file):
            # Rename the file
            os.rename(old_file, new_file)
            print(f"Renamed {old_file} to {new_file}")
        else:
            print(f"{old_file} not found!")
    
    except Exception as e:
        print(f"Failed to rename PRN file: {e}")

def modify_symbols_valid_meta(downloaded_folder: str):
    try:
        symbols_file = os.path.join(downloaded_folder, 'symbols_valid_meta.csv')

        # Check if the file exists
        if os.path.exists(symbols_file):
            # Read the CSV file
            df = pd.read_csv(symbols_file, header=None)
            
            old_value = "Y,PRN,Invesco DWA Industrials Momentum ETF,Q,G,Y,100.0,N,N,,PRN,N"
            new_value = "Y,PRN_1,Invesco DWA Industrials Momentum ETF,Q,G,Y,100.0,N,N,,PRN,N"
            
            df[0] = df[0].replace(old_value, new_value)
            

            df.to_csv(symbols_file, index=False, header=False)
            print(f"Modified {symbols_file} successfully.")
        else:
            print(f"{symbols_file} not found!")

    except Exception as e:
        print(f"Failed to modify {symbols_file}: {e}")

def process_files(stocks_folder: str, downloaded_folder: str):
    rename_prn_file(stocks_folder)
    modify_symbols_valid_meta(downloaded_folder)
    
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
    python_callable=ingest_data_from_kagglehub,
    op_args=['jacksoncrow/stock-market-dataset'],  # Dataset ID for stock market
    op_kwargs={'destination_path': './data_raw/stocks_raw'},  # Specify the destination path
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


# Will change a lot
input_file_namechange = os.path.join(script_dir, '../data_raw/stocks_raw/stocks/')
output_file_namechange = os.path.join(script_dir, '../data_raw/stocks_raw/stocks/')

stocks_folder = "C:/Users/molsi/datasets/stocks"  
downloaded_folder = "C:/Users/molsi/datasets"

fix_the_filename_repitition_error_in_stocks = PythonOperator(
    task_id='process_files',
    python_callable=process_files,
    op_args=[input_file_namechange, output_file_namechange], 
    dag=dag,
)

download_stock_market_task >> clean_genres_task >> fix_the_filename_repitition_error_in_stocks
