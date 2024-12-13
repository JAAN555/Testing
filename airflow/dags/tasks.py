import duckdb
import sys
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from pathlib import Path
from datetime import datetime
import os
import kagglehub
#!pip install kagglehub
import shutil
import subprocess
import zipfile
import mlcroissant as mlc

#from scripts.ingest_from_kaggle import ingest_data_from_kagglehub
#from scripts.data_filtering import filter_files


def ingest_data_from_kagglehub(dataset_id: str) -> str:
    try:
        # Define the destination directory where data will be saved
        script_dir = os.path.dirname(os.path.abspath(__file__))
        input_file_namechange = os.path.join(script_dir, '../data_raw/stocks_raw/')
        #destination_directory = r"C:/Users/molsi/Documents/Testing/airflow/data_raw/stocks_raw"
        
        # Make sure the destination directory exists
        if not os.path.exists(input_file_namechange):
            print(f"Directory does not exist. Creating: {input_file_namechange}")
            os.makedirs(input_file_namechange, exist_ok=True)

        print(f"Destination directory: {input_file_namechange}")
        
        # Define the URL for the Kaggle dataset download (using the dataset_id)
        dataset_url = f"https://www.kaggle.com/api/v1/datasets/download/{dataset_id}"
        
        # Define the path where the zip file will be saved
        zip_file_path = os.path.join(input_file_namechange, "stock-market-dataset.zip")
        print(f"ZIP file will be saved to: {zip_file_path}")

        # Use curl to download the dataset zip file
        print(f"Downloading dataset from {dataset_url}...")
        curl_command = f"curl -L -o {zip_file_path} {dataset_url}"

        # Execute the curl command
        subprocess.run(curl_command, shell=True, check=True)
        print(f"Dataset downloaded to {zip_file_path}")

        # Extract the zip file
        print(f"Extracting the zip file {zip_file_path}...")
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(input_file_namechange)
        print(f"Dataset extracted to {input_file_namechange}")

        # Remove the zip file after extraction
        os.remove(zip_file_path)
        print(f"Removed the zip file {zip_file_path}")

        # List the contents of the destination directory to confirm extraction
        extracted_files = os.listdir(input_file_namechange)
        print(f"Files in the destination directory: {extracted_files}")

        return input_file_namechange

    except subprocess.CalledProcessError as e:
        print(f"Error during dataset download using curl: {e}")
        return None
    except Exception as e:
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
