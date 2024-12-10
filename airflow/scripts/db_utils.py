import duckdb
import pandas as pd
from pathlib import Path

def load_to_duckdb():
    """
    Load cleaned movie data from a CSV file into a DuckDB database.
    """
    # Base path relative to the project structure
    base_path = Path(__file__).resolve().parent.parent.parent

    # File paths
    csv_file = base_path / 'data/movies/cleaned_mymoviedb.csv'
    database_file = base_path / 'data/movies/movies.db'

    # Load data into DuckDB
    conn = duckdb.connect(str(database_file))
    
    # Load CSV into a Pandas DataFrame
    df = pd.read_csv(csv_file)
    
    # Store the DataFrame directly into DuckDB
    conn.execute("CREATE TABLE IF NOT EXISTS movies AS SELECT * FROM df")  # Create table if it doesn't exist
    conn.execute("INSERT INTO movies SELECT * FROM df")  # Insert data into the table
    
    conn.close()

    print(f"Data loaded into DuckDB database: {database_file}")
