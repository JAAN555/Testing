import duckdb

def query_duckdb():
    database_file = 'airflow/data/movies/movies.db'
    conn = duckdb.connect(database_file)
    result = conn.execute("SELECT * FROM movies LIMIT 5").fetchdf()
    print(result)
    conn.close()

if __name__ == '__main__':
    query_duckdb()