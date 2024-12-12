import duckdb
from pathlib import Path

def query_duckdb():
    # Define base path and database file path
    base_path = Path(__file__).resolve().parent.parent.parent
    database_file = base_path / 'data/movies/movies.db'
    
    # Connect to DuckDB
    conn = duckdb.connect(str(database_file))
    
    # Query data from DuckDB
    result = conn.execute("SELECT * FROM movies LIMIT 5").fetchdf()
    print(result)
    
    # Close connection
    conn.close()

if __name__ == '__main__':
    query_duckdb()
