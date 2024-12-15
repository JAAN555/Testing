import streamlit as st
import pandas as pd
import duckdb
import logging
import os

logging.basicConfig(
    filename="/app/logs/app.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def log_directory_contents(directory):
    try:
        current_working_directory = os.getcwd()
        absolute_directory_path = os.path.join(current_working_directory, directory)
        logging.info(f"Absolute path of the directory: {absolute_directory_path}")

        files = os.listdir(directory)
        logging.info(f"Contents of {directory}:")
        for file in files:
            logging.info(file)
    except Exception as e:
        logging.error(f"Error listing contents of {directory}: {e}")

def log_table_columns(table_name):
    """
    Logs the columns of a specific table by querying its schema.
    """
    try:
        conn = get_db_connection()
        logging.info(f"Fetching column details for table: {table_name}")
        # Query to get the column details (DuckDB uses PRAGMA table_info for this)
        query = f"PRAGMA table_info({table_name});"
        columns = conn.execute(query).fetchdf()
        
        # Log the column details
        logging.info(f"Columns in table {table_name}:")
        logging.info(columns)
        
        conn.close()
    except Exception as e:
        logging.error(f"Error fetching columns for {table_name}: {e}")

def check_genre_columns():
    conn = get_db_connection()
    query = "PRAGMA table_info(Dim_Movie);"
    columns = conn.execute(query).fetchdf()
    required_columns = [
        'Genre_Adventure', 'Genre_Comedy', 'Genre_Drama', 'Genre_Science_Fiction',
        'Genre_Crime', 'Genre_Mystery', 'Genre_Thriller', 'Genre_Animation',
        'Genre_Family', 'Genre_Fantasy', 'Genre_War', 'Genre_Horror', 'Genre_Music',
        'Genre_Romance', 'Genre_Western', 'Genre_History', 'Genre_TV_Movie', 'Genre_Documentary'
    ]
    
    for col in required_columns:
        if col not in columns['name'].values:
            logging.error(f"Column '{col}' not found in Dim_Movie.")
        else:
            logging.info(f"Column '{col}' is present.")
    
    conn.close()

def get_db_connection():
    db_path = '/airflow/data/warehouse.duckdb'
    logging.info(f"Attempting to connect to database at: {db_path}")
    try:
        conn = duckdb.connect(db_path)
        logging.info("Database connection successful.")
        tables = conn.execute("SHOW TABLES").fetchdf()
        logging.info(f"Available tables: {tables}")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to the database: {e}")
        raise

def log_all_table_columns():
    try:
        # i do not know which table is the problem
        table_names = [
            'Fact_Sector_Stock_Performance', 
            'Dim_Company_Sector', 
            'Dim_Movie', 
            'Fact_Movie_Impact', 
            'Dim_Time', 
            'movies', 
            'stocks', 
            'symbols_valid_meta'
        ]
        
        for table in table_names:
            log_table_columns(table)
    
    except Exception as e:
        logging.error(f"Error logging columns for all tables: {e}")

@st.cache_data
def load_data(query):
    logging.debug(f"Executing query: {query}")
    try:
        conn = get_db_connection()
        df = conn.execute(query).fetchdf()
        conn.close()
        logging.info(f"Query executed successfully. Rows fetched: {len(df)}")
        return df
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        st.error("An error occurred while fetching data. Check logs for details.")
        return pd.DataFrame()

# Corrected Queries
query_largest_companies_overall = """
SELECT 
    Symbol, 
    "Security Name" AS Security_Name, 
    Sector_Name, 
    MAX(Average_Close_Price) AS Max_Close_Price 
FROM 
    Fact_Sector_Stock_Performance fssp
JOIN 
    Dim_Company_Sector dcs 
ON 
    fssp.Sector_ID = dcs.Sector_ID 
GROUP BY 
    Symbol, "Security Name", Sector_Name 
ORDER BY 
    Max_Close_Price DESC 
LIMIT 10;
"""

query_largest_companies_by_sector = """
SELECT 
    Sector_Name, 
    Symbol, 
    "Security Name" AS Security_Name, 
    MAX(Average_Close_Price) AS Max_Close_Price 
FROM 
    Fact_Sector_Stock_Performance fssp
JOIN 
    Dim_Company_Sector dcs 
ON 
    fssp.Sector_ID = dcs.Sector_ID 
GROUP BY 
    Sector_Name, Symbol, "Security Name" 
ORDER BY 
    Sector_Name, Max_Close_Price DESC;
"""

#This was a bad thing to do - not worth it by a long shot
def get_query_largest_movies_overall():
    conn = duckdb.connect('/airflow/data/warehouse.duckdb')
    try:
        genre_columns = conn.execute("""
            PRAGMA table_info(Dim_Movie);
        """).fetchdf()


        genres = [
            col for col in genre_columns['name'].to_list()
            if col.startswith('Genre_')
        ]

        # This makes the SELECT clause dynamically
        select_clause = ",\n    ".join(genres)

        # This makes the query dynamically - i will change it often enough that there is no point in having it set
        query = f"""
        SELECT 
            Title, 
            Release_Date, 
            {select_clause}
        FROM 
            Dim_Movie 
        ORDER BY 
            Release_Date DESC
        LIMIT 10;
        """
        return query
    finally:
        conn.close()


def get_query_largest_movies_by_tag():
    conn = duckdb.connect('/airflow/data/warehouse.duckdb')
    try:
        genre_columns = conn.execute("""
            PRAGMA table_info(Dim_Movie);
        """).fetchdf()

        genres = [
            col for col in genre_columns['name'].to_list()
            if col.startswith('Genre_')
        ]

        # This makes the SELECT clause dynamically
        select_clause = ",\n    ".join(genres)

        # This makes the query dynamically - i will change it often enough that there is no point in having it set
        query = f"""
        SELECT 
            Title, 
            Release_Date,
            {select_clause}
        FROM 
            Dim_Movie 
        ORDER BY 
            Release_Date DESC;
        """
        return query
    finally:
        conn.close()


query_sector_impact_by_movie = """
SELECT 
    dm.Title AS Movie_Title,
    dm.Release_Date,
    dcs.Sector_Name,
    AVG(fssp.Average_Close_Price) AS Average_Impact,
    MAX(fssp.Average_Close_Price) AS Max_Impact,
    MIN(fssp.Average_Close_Price) AS Min_Impact
FROM 
    Dim_Movie dm
JOIN 
    Fact_Sector_Stock_Performance fssp 
ON 
    dm.Release_Date = fssp.Date
JOIN 
    Dim_Company_Sector dcs 
ON 
    fssp.Sector_ID = dcs.Sector_ID 
GROUP BY 
    dm.Title, dm.Release_Date, dcs.Sector_Name
ORDER BY 
    Average_Impact DESC;
"""

# Streamlit Application
st.title("Cinema-Stocks---Data-Engineering: Impact of movie releases on the stockmarket")
st.write("Explore the largest companies by sector and overall, the largest movies overall and by tags, and analyze the sector impact of movie releases.")

st.header("Largest Companies")
tab1, tab2 = st.tabs(["Overall", "By Sector"])

with tab1:
    st.subheader("Largest Companies Overall")
    data = load_data(query_largest_companies_overall)
    if not data.empty:
        st.write(data)
        st.bar_chart(data, x='Symbol', y='Max_Close_Price')
    else:
        st.warning("No data available.")

with tab2:
    st.subheader("Largest Companies by Sector")
    data = load_data(query_largest_companies_by_sector)
    if not data.empty:
        st.write(data)
    else:
        st.warning("No data available.")

st.header("Largest Movies")
tab1, tab2 = st.tabs(["Overall", "By Tags"])


with tab1:
    st.subheader("Largest Movies Overall")
    query = get_query_largest_movies_overall()
    data = load_data(query)
    if not data.empty:
        st.write(data)
    else:
        st.warning("No data available.")

with tab2:
    st.subheader("Largest Movies by Tags")
    query = get_query_largest_movies_by_tag()
    data = load_data(query)
    if not data.empty:
        st.write(data)
    else:
        st.warning("No data available.")

st.header("Sector Impact by Movie")
data = load_data(query_sector_impact_by_movie)
if not data.empty:
    st.write(data)
    st.bar_chart(data, x='Movie_Title', y='Average_Impact')
else:
    st.warning("No data available.")

#log_all_table_columns() #not needed right now!
check_genre_columns()

logging.info("Streamlit app loaded successfully.")
