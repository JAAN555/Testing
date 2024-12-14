import streamlit as st
import pandas as pd
import sqlite3  
import logging

logging.basicConfig(
    filename="/app/logs/app.log",  
    level=logging.DEBUG,  
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def get_db_connection():
    db_path = '/airflow/data/warehouse.duckdb'
    logging.info(f"Attempting to connect to database at: {db_path}")
    try:
        conn = sqlite3.connect(db_path)  
        logging.info("Database connection successful.")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to the database: {e}")
        st.error("Could not connect to the database. Check logs for details.")
        raise

@st.cache_data
def load_data(query):
    logging.debug(f"Executing query: {query}")
    try:
        conn = get_db_connection()
        df = pd.read_sql_query(query, conn)
        conn.close()
        logging.info(f"Query executed successfully. Rows fetched: {len(df)}")
        return df
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        st.error("An error occurred while fetching data. Check logs for details.")
        return pd.DataFrame()

query_largest_companies_overall = """
SELECT 
    Symbol, 
    Security_Name, 
    Sector_Name, 
    MAX(Average_Close_Price) AS Max_Close_Price 
FROM 
    Fact_Sector_Stock_Performance fssp
JOIN 
    Dim_Company_Sector dcs 
ON 
    fssp.Sector_ID = dcs.Sector_ID 
GROUP BY 
    Symbol, Security_Name, Sector_Name 
ORDER BY 
    Max_Close_Price DESC 
LIMIT 10;
"""

query_largest_companies_by_sector = """
SELECT 
    Sector_Name, 
    Symbol, 
    Security_Name, 
    MAX(Average_Close_Price) AS Max_Close_Price 
FROM 
    Fact_Sector_Stock_Performance fssp
JOIN 
    Dim_Company_Sector dcs 
ON 
    fssp.Sector_ID = dcs.Sector_ID 
GROUP BY 
    Sector_Name, Symbol, Security_Name 
ORDER BY 
    Sector_Name, Max_Close_Price DESC;
"""

query_largest_movies_overall = """
SELECT 
    Title, 
    Release_Date, 
    Popularity_Score, 
    Vote_Average 
FROM 
    Dim_Movie 
ORDER BY 
    Popularity_Score DESC, Vote_Average DESC 
LIMIT 10;
"""

query_largest_movies_by_tag = """
SELECT 
    Genre, 
    Title, 
    Release_Date, 
    Popularity_Score, 
    Vote_Average 
FROM 
    Dim_Movie_Genre dmg
JOIN 
    Dim_Movie dm 
ON 
    dmg.Movie_ID = dm.Movie_ID 
ORDER BY 
    Genre, Popularity_Score DESC, Vote_Average DESC;
"""

query_sector_impact_by_movie = """
SELECT 
    dm.Title AS Movie_Title,
    dm.Release_Date,
    dcs.Sector_Name,
    AVG(fssp.Price_Change_Percent) AS Average_Impact,
    MAX(fssp.Price_Change_Percent) AS Max_Impact,
    MIN(fssp.Price_Change_Percent) AS Min_Impact
FROM 
    Dim_Movie dm
JOIN 
    Fact_Sector_Stock_Performance fssp 
ON 
    dm.Sector_ID = fssp.Sector_ID 
AND 
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
st.title("Exploration Dashboard: Largest Companies, Movies, and Sector Impact")
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
    data = load_data(query_largest_movies_overall)
    if not data.empty:
        st.write(data)
        st.bar_chart(data, x='Title', y='Popularity_Score')
    else:
        st.warning("No data available.")

with tab2:
    st.subheader("Largest Movies by Tags")
    data = load_data(query_largest_movies_by_tag)
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

logging.info("Streamlit app loaded successfully.")