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
import glob

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


'''
def load_csv_to_duckdb(con, file_paths, table_prefix):
    """
    Load CSV files into DuckDB tables with specific handling for the problematic file LGF.B.csv.
    
    :param con: DuckDB connection object
    :param file_paths: List of file paths to CSV files
    :param table_prefix: Prefix for the table names
    """
    for file in file_paths:
        # Extract table name from file name
        table_name = os.path.basename(file).replace('.csv', '')

        try:
            # Special handling for LGF.B.csv
            if 'LGF.B.csv' in file:
                print(f"Handling special case for {file}...")
                # Load with additional options to handle the file correctly
                con.execute(f"""
                    CREATE OR REPLACE TABLE {table_prefix}_{table_name} AS
                    SELECT * FROM read_csv_auto('{file}', NULL, ',', true, true)
                """)
                print(f"Special case table {table_prefix}_{table_name} created and data loaded successfully.")
            else:
                # General case for other CSV files
                con.execute(f"""
                    CREATE OR REPLACE TABLE {table_prefix}_{table_name} AS
                    SELECT * FROM read_csv_auto('{file}')
                """)
                print(f"Table {table_prefix}_{table_name} created and data loaded successfully.")
        
        except Exception as e:
            # Log the error without skipping the file
            print(f"Error loading file {file} into DuckDB table {table_prefix}_{table_name}: {e}")
            # Store error information for later troubleshooting (could be written to a log file or database)
            with open("load_errors.log", "a") as log_file:
                log_file.write(f"Error loading {file}: {e}\n")


def export_duckdb_to_csv(con, table_name, output_dir):
    """
    Export data from DuckDB table to CSV for human-readable access.
    
    :param con: DuckDB connection object
    :param table_name: The DuckDB table name to export
    :param output_dir: The directory to save the CSV file
    """
    try:
        output_file = os.path.join(output_dir, f"{table_name}.csv")
        con.execute(f"EXPORT TO CSV '{output_file}' FROM SELECT * FROM {table_name}")
        print(f"Table {table_name} exported to {output_file} successfully.")
    except Exception as e:
        print(f"Error exporting {table_name} to CSV: {e}")
        with open("export_errors.log", "a") as log_file:
            log_file.write(f"Error exporting {table_name}: {e}\n")


def load_data_task():
    """
    Task to load CSV files into DuckDB and export them to CSV for human-readable access.
    """
    # Paths for stock and movie data
    stocks_path = 'data/stocks/'
    movies_path = 'data/movies/'

    # Get list of all CSV files in the directories
    stocks_files = glob.glob(os.path.join(stocks_path, '*.csv'))
    movies_files = glob.glob(os.path.join(movies_path, '*.csv'))

    # Step 1: Connect to DuckDB (connection will be reused for all tasks)
    con = duckdb.connect('data/combined.duckdb')  # Path for the DuckDB database file
    
    # Load stock and movie files into DuckDB
    load_csv_to_duckdb(con, stocks_files, 'stock')
    load_csv_to_duckdb(con, movies_files, 'movie')

    # Optionally, export the tables to CSV for human readability
    output_dir = 'data/output_csv'
    os.makedirs(output_dir, exist_ok=True)
    
    # Get list of all table names dynamically (instead of hardcoding)
    # This assumes you have a method of tracking the tables you've created, like querying the DuckDB system tables
    tables_to_export = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()
    
    for table in tables_to_export:
        table_name = table[0]  # Extract table name from result
        # Export each loaded table to CSV
        export_duckdb_to_csv(con, table_name, output_dir)

    # Do not close the connection, as it may be reused in later tasks
    return con
'''


# Testing again

# Define paths
MOVIES_FOLDER = "data/movies"
STOCKS_FOLDER = "data/stocks"
DUCKDB_PATH = "data/warehouse.duckdb"

# Shared DuckDB connection (file-backed for persistence)
duckdb_connection = duckdb.connect(DUCKDB_PATH)

def load_movies_to_duckdb():
    # Load movies data
    movies_files = glob.glob(os.path.join(MOVIES_FOLDER, "*.csv"))
    all_movies = []

    for file in movies_files:
        df = pd.read_csv(file)
        all_movies.append(df)

    combined_movies = pd.concat(all_movies)
    combined_movies['Movie_ID'] = range(1, len(combined_movies) + 1)  # Add unique Movie_ID
        
    # Store in DuckDB
    duckdb_connection.execute("CREATE OR REPLACE TABLE movies AS SELECT * FROM combined_movies")
    print("Movies table loaded into DuckDB.")



# Verification Functions
def view_movies():
    result = duckdb_connection.execute("SELECT * FROM movies LIMIT 10").fetchdf()
    print(result)

def count_movies():
    row_count = duckdb_connection.execute("SELECT COUNT(*) AS total_rows FROM movies").fetchone()
    print(f"Total rows in movies table: {row_count[0]}")

def describe_movies():
    schema = duckdb_connection.execute("DESCRIBE movies").fetchdf()
    print(schema)

def check_table_exists():
    tables = duckdb_connection.execute("SHOW TABLES").fetchdf()
    print(tables)


# Now we load stocks within DuckDB database

# Paths to data folders
STOCKS_FOLDER = "data/stocks"
DUCKDB_PATH = "data/warehouse.duckdb"

# Shared DuckDB connection (file-backed for persistence)
duckdb_connection = duckdb.connect(DUCKDB_PATH)

def load_stocks_to_duckdb():
    # Load stocks data, excluding symbols_valid_meta.csv
    stocks_files = glob.glob(os.path.join(STOCKS_FOLDER, "*.csv"))
    all_stocks = []
    # Assuming STOCKS_FOLDER is defined elsewhere
    meta_file_path = os.path.join(STOCKS_FOLDER, "symbols_valid_meta.csv")

    # Load the CSV file into a Pandas DataFrame
    meta_df = pd.read_csv(meta_file_path)

    # Use DuckDB's from_df method to directly create a table from the DataFrame
    duckdb_connection.execute("CREATE OR REPLACE TABLE symbols_valid_meta AS SELECT * FROM meta_df")

    print("symbols_valid_meta table loaded into DuckDB.")   

    # Process all other CSV files and add Stock_ID based on filename
    for file in stocks_files:
        if os.path.basename(file) == "symbols_valid_meta.csv":
            continue  # Skip the meta file, already loaded separately
        
        # Load CSV data
        df = pd.read_csv(file)
        
        # Create the Stock_ID based on the filename (e.g., Stock_A, Stock_AA, Stock_LGF.B)
        stock_id = "Stock_" + os.path.splitext(os.path.basename(file))[0]
        df['Stock_ID'] = stock_id  # Add the Stock_ID feature
        
        all_stocks.append(df)

    # Combine all stock data into one DataFrame
    combined_stocks = pd.concat(all_stocks, ignore_index=True)

    # Store in DuckDB
    duckdb_connection.execute("CREATE OR REPLACE TABLE stocks AS SELECT * FROM combined_stocks")
    print("Stocks table loaded into DuckDB.")

    # Optionally, print out the first few rows of the loaded data for verification
    print(duckdb_connection.execute("SELECT * FROM stocks LIMIT 5").fetchall())

# Verification Functions for Stocks
def view_stocks():
    result = duckdb_connection.execute("SELECT * FROM stocks LIMIT 10").fetchdf()
    print(result)

def count_stocks():
    row_count = duckdb_connection.execute("SELECT COUNT(*) AS total_rows FROM stocks").fetchone()
    print(f"Total rows in stocks table: {row_count[0]}")

def describe_stocks():
    schema = duckdb_connection.execute("DESCRIBE stocks").fetchdf()
    print(schema)

def check_stocks_table_exists():
    tables = duckdb_connection.execute("SHOW TABLES").fetchdf()
    if 'stocks' in tables.values:
        print("The 'stocks' table exists.")
    else:
        print("The 'stocks' table does not exist.")



# Define the Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 12),
    'retries': 5,
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
    dag=dag,
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



# Set the paths for the stocks and movies data
stocks_path = os.path.abspath('./data/stocks')  # Path to the stocks data directory
movies_path = os.path.abspath('./data/movies')  # Path to the movies data directory
db_path = os.path.abspath('./data/duckdb.db')  # Path to the DuckDB database
'''
load_and_export_task = PythonOperator(
    task_id='load_and_export_duckdb',
    python_callable=load_data_task,
    dag=dag,
)
'''
load_movies_task = PythonOperator(
    task_id="load_movies",
    python_callable=load_movies_to_duckdb,
    dag = dag,
)

def verify_movies():
    view_movies()
    check_table_exists()
    count_movies()
    describe_movies()

verify_task = PythonOperator(
    task_id="verify_movies",
    python_callable=verify_movies,
    dag=dag
)




load_stocks_task = PythonOperator(
    task_id="load_stocks",
    python_callable=load_stocks_to_duckdb,
    dag = dag,
)


def verify_stocks():
    view_stocks()
    check_stocks_table_exists()
    count_stocks()
    describe_stocks()

verify_task2 = PythonOperator(
    task_id="verify_stocks",
    python_callable=verify_stocks,
    dag=dag
)

#download_stock_market_task >> download_movies_task >> clean_genres_task >> fix_the_filename_repitition_error_in_stocks
#clean_genres_task >> filter_task >> load_movies_task
#filter_task

download_stock_market_task >> fix_the_filename_repitition_error_in_stocks >> filter_task >> download_movies_task >> clean_genres_task >> load_movies_task >> verify_task >> load_stocks_task >> verify_task2
