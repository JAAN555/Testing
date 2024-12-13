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


def ingest_data_from_kagglehub(dataset_id: str, zip_file_path: str, location: str) -> str:
    try:
        # Define the destination directory where data will be saved
        script_dir = os.path.dirname(os.path.abspath(__file__))
        input_file_namechange = os.path.join(script_dir, f'../data_raw/{location}/')
        #destination_directory = r"C:/Users/molsi/Documents/Testing/airflow/data_raw/stocks_raw"
        
        # Make sure the destination directory exists
        if not os.path.exists(input_file_namechange):
            print(f"Directory does not exist. Creating: {input_file_namechange}")
            os.makedirs(input_file_namechange, exist_ok=True)

        check_file = os.path.join(input_file_namechange, "symbols_valid_meta.csv")
        if os.path.exists(check_file):
            print(f"Data already exists in {input_file_namechange}. Skipping download.")
            return input_file_namechange
        else:
            print("check_file stocks_raw.csv does not exist")

        print(f"Destination directory: {input_file_namechange}")
        
        # Define the URL for the Kaggle dataset download (using the dataset_id)
        dataset_url = f"https://www.kaggle.com/api/v1/datasets/download/{dataset_id}"
        
        # Define the path where the zip file will be saved
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

def clean_and_split_genres(input_file: str, output_file: str):
    """
    Cleans the movie dataset by splitting the 'Genre' column into binary columns 
    for each unique genre and saves the updated dataset to a new CSV file.
    
    Args:
        input_file (str): Path to the input CSV file.
        output_file (str): Path to save the cleaned CSV file.
    """
    try:
        # Load the dataset
        df = pd.read_csv(
            input_file,
            encoding='utf-8',
            delimiter=',',
            on_bad_lines='skip',  # Skips problematic rows
            engine='python'       # Flexible parsing for complex CSVs
        )

        # Ensure 'Genre' column exists
        if 'Genre' not in df.columns:
            raise ValueError("The dataset does not have a 'Genre' column.")

        # Generate unique genres
        genres = df['Genre'].dropna().str.split(', ').explode().unique()

        # Add binary columns for each genre
        for genre in genres:
            df[genre] = df['Genre'].apply(
                lambda x: 1 if isinstance(x, str) and genre in x.split(', ') else 0
            )

        # Drop the original 'Genre' column
        df.drop(columns=['Genre'], inplace=True)

        # Save the cleaned dataset
        df.to_csv(output_file, index=False)
        print(f"Cleaned dataset saved to {output_file}")

    except Exception as e:
        print(f"Error while cleaning and splitting genres: {e}")
        raise

#script_dir = os.path.dirname(os.path.abspath(__file__))

# Test ends

def filter_files(input_path, output_path, meta_file_path):
    """
    Filters stock data files from the input_path based on a predefined list
    and writes the valid files into output_path.
    """
     # File list, mida soovime säilitada
    file_list = [
        "A", "AA", "AACG", "AAL", "AAN", "AAOI", "AAON", "AAP", "AAU", "B", "DIS",
        "BWA", "SNE", "PGRE", "UVV", "LGF.B", "NFLX", "REG", "SPOT", "ROKU", "AMZN",
        "TME", "IQ", "BILI", "ZNGA", "ATVI", "EA", "NTES", "TTWO", "MAT", "HAS", "FNKO",
        "CZR", "SIX", "ORCL", "HPQ", "DELL", "AAPL", "MSFT", "TSLA", "NVDA", "AMD",
        "LPL", "BAX", "JNJ", "PFE", "NVS", "AZN", "MRK", "MDT", "BSX", "NKE", "PBYI",
        "UAA", "PG", "PLNT", "PTON", "LULU", "FSLR", "WMT", "COST", "HD", "UNH", "CVS",
        "GOOG", "GOOGL", "BAC", "C", "EAD", "GBIL", "CVX", "MPC", "PSX", "PSXP", "CCZ",
        "VZ", "CHTR", "DIS", "ALL", "AIG", "MCD", "SBUX", "DPZ", "F", "GM"
    ]

       # Create the output directory if it doesn't exist
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    # Process files in the input path
    for filename in os.listdir(input_path):
        # Check if the file is a CSV and if it is in the file_list (consider the first part of the filename)
        if filename.endswith('.csv') and filename[:-4] in file_list:
            print(f"Matching file: {filename[:-4]}")  # Debugging line
            src_file = os.path.join(input_path, filename)  
            dest_file = os.path.join(output_path, filename)
            shutil.copy(src_file, dest_file)  # Move the file

    # Preserve "symbols_valid_meta.csv" if present
    if os.path.exists(meta_file_path):
        shutil.copy(meta_file_path, os.path.join(output_path, 'symbols_valid_meta.csv'))
    else:
        print("symbols_valid_meta.csv not found, but it should be kept.")

    print(f"Filtering completed. Valid files are in '{output_path}'.")


# Done


# Define the Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 12),
    'retries': 1,
}

dag = DAG(
    'download_stock_market_and_movies_datasets',
    default_args=default_args,
    description='A DAG to load data into DuckDB',
    schedule_interval=None,  # Adjust as needed
)


# Task to download the 9000 movies dataset
download_movies_task = PythonOperator(
    task_id='download_9000_movies_dataset',
    python_callable=ingest_data_from_kagglehub,
    op_args=['disham993/9000-movies-dataset'],  # Dataset ID for 9000 movies
    op_kwargs={'zip_file_path': 'movies_9000.zip', 'location':'movies_raw'},
    dag = dag,
)

    # Task to download the stock market dataset
download_stock_market_task = PythonOperator(
    task_id='download_stock_market_dataset',
    python_callable=ingest_data_from_kagglehub,
    op_args=['jacksoncrow/stock-market-dataset'],  # Dataset ID for stock market
    op_kwargs={'zip_file_path': 'stock-market-dataset.zip', 'location':'stocks_raw'},
    dag=dag
)
'''   # Define paths
base_path = './data/'
input_path = os.path.join(base_path, 'stocks_raw')
output_path = os.path.join(base_path, 'stocks')

# Set the meta_file_path to the correct location inside 'stocks_raw'
meta_file_path = os.path.join(input_path, 'symbols_valid_meta.csv')

# Task: Filter files
filter_task = PythonOperator(
    task_id='filter_files_task',
    python_callable=filter_files,
    op_args=[input_path, output_path, meta_file_path],  # Pass paths as args
)
'''

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


# Define the task for cleaning and splitting genres
script_dir = os.path.dirname(os.path.abspath(__file__))
movies_raw_dir = os.path.join(script_dir, '../data/movies_raw/')
movies_cleaned_dir = os.path.join(script_dir, '../data/movies/')

# Ensure directories exist
os.makedirs(movies_raw_dir, exist_ok=True)
os.makedirs(movies_cleaned_dir, exist_ok=True)

input_file = os.path.join(movies_raw_dir, 'mymoviedb.csv')
output_file = os.path.join(movies_cleaned_dir, 'cleaned_mymoviedb.csv')

clean_genres_task = PythonOperator(
    task_id='clean_and_split_genres',
    python_callable=clean_and_split_genres,
    op_args=[input_file, output_file],
    dag=dag,
)

# Will change a lot
#base_path = './data/'
#input_path = os.path.join(base_path, 'stocks_raw/stocks')  # Correct input path to 'stocks_raw'
#output_path = os.path.join(base_path, 'stocks')  # Correct output path to 'stocks'

# Set the meta_file_path to the correct location inside 'stocks_raw'
#meta_file_path = os.path.join(os.path.join(base_path, 'stocks_raw'), 'stocks', 'symbols_valid_meta.csv')

input_path = os.path.abspath('./data/stocks_raw/stocks')  # Absolute path to the 'stocks' directory
output_path = os.path.abspath('./data/stocks')  # Absolute path to the destination directory
meta_file_path = os.path.abspath('./data/stocks_raw/symbols_valid_meta.csv')  # Path to the meta file



# Task: Filter files
filter_task = PythonOperator(
    task_id='filter_files_task',
    python_callable=filter_files,
    op_args=[input_path, output_path, meta_file_path],  # Pass paths as args
)


input_file_namechange = os.path.join(script_dir, '../data_raw/stocks_raw/stocks/')
output_file_namechange = os.path.join(script_dir, '../data_raw/stocks_raw/stocks/')


fix_the_filename_repitition_error_in_stocks = PythonOperator(
    task_id='process_files',
    python_callable=process_files,
    op_args=[input_file_namechange, output_file_namechange], 
    dag=dag,
)

#download_stock_market_task >> download_movies_task >> clean_genres_task >> fix_the_filename_repitition_error_in_stocks
clean_genres_task >> filter_task
#filter_task