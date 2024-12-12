'''

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import duckdb
import pandas as pd
from pymongo import MongoClient  # For MongoDB connection
import os  # For file path operations
from datetime import timedelta  # For scheduling

# Connection details
MONGO_URI = "mongodb://mongodb:27017"
DB_NAME = "data_pipeline"

STOCKS_FOLDER = '/data/stocks/'
MOVIES_FILE = '/data/movies/cleaned_mymoviedb.csv'

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

def load_to_mongo():
    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    # Read movies file
    #movies = pd.read_csv(MOVIES_FILE)
    #db.movies.insert_many(movies.to_dict('records'))

    # Read all stock files
    all_stocks = []  # Collect all stocks data into one DataFrame
    for file_name in file_list:
        file_path = os.path.join(STOCKS_FOLDER, f"{file_name}.csv")
        if os.path.exists(file_path):
            stock_data = pd.read_csv(file_path)
            all_stocks.append(stock_data)
        else:
            print(f"Warning: {file_path} does not exist.")

    # Combine all stock data into a single DataFrame
    if all_stocks:
        combined_stocks = pd.concat(all_stocks, ignore_index=True)
        db.stocks.insert_many(combined_stocks.to_dict('records'))

    print("Data successfully loaded into MongoDB.")


# Function to create DuckDB database
def create_duckdb_db():
    conn = duckdb.connect('sample_database.duckdb')
    conn.execute('CREATE TABLE IF NOT EXISTS users (id INTEGER, name STRING, age INTEGER)')
    conn.close()

# Function to load data into DuckDB

def load_data_to_duckdb():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    movies = pd.DataFrame(list(db.movies.find()))
    stocks = pd.DataFrame(list(db.stocks.find()))

    # Perform join and save to DuckDB
    merged = pd.merge(movies, stocks, left_on='movie_id', right_on='stock_id')
    conn = duckdb.connect('/data/final_data.duckdb')
    conn.execute("""CREATE TABLE IF NOT EXISTS fact_table AS SELECT * FROM movies LIMIT 0;""")  # Create the table with no data, just the schema

# Insert the merged DataFrame
conn.from_df(merged).to_sql('fact_table', conn, if_exists='replace')

def load_to_duckdb():
    import duckdb
    import pandas as pd

    base_path = Path(__file__).resolve().parent.parent
    csv_file = base_path / 'data/A.csv'
    database_file = base_path / 'data/stocks.db'

    conn = duckdb.connect(str(database_file))
    df = pd.read_csv(csv_file)
    conn.execute("CREATE TABLE IF NOT EXISTS movies AS SELECT * FROM df")
    conn.close()

# Define DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_pipeline",
    default_args=default_args,
    description="ETL pipeline for movie and stock data",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    task1 = PythonOperator(task_id="create_duckdb_db", python_callable=create_duckdb_db)
    task3 = PythonOperator(task_id="load_to_mongo", python_callable=load_to_mongo)
    #task4 = PythonOperator(task_id="transform_and_load_to_duckdb", python_callable=transform_and_load_to_duckdb)
    #task4 = PythonOperator(task_id="transform_and_load_to_duckdb", python_callable=load_data_to_duckdb)

   # task1 >> task3 #>> task4


   '''