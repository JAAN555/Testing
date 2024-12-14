import streamlit as st
import pandas as pd
import sqlite3  # Replace with your database connector if not SQLite

# Database connection (replace with your DB connection)
def get_db_connection():
    return sqlite3.connect('airflow/data/warehouse.duckdb')  # Update with your database file or connection string

# Load data from the database
@st.cache_data
def load_data(query):
    conn = get_db_connection()
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# Queries
query_correlation = """
SELECT 
    dm.Title AS Movie_Title,
    dm.Release_Date,
    dcs.Symbol AS Company_Symbol,
    dcs.Sector_Name,
    fssp.Average_Close_Price,
    fssp.Volatility,
    fssp.Price_Change_Percent
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
    dcs.Sector_ID = fssp.Sector_ID;
"""

query_most_expensive_stocks = """
SELECT 
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
    Symbol, Security_Name
ORDER BY 
    Max_Close_Price DESC
LIMIT 10;
"""

query_largest_movie_releases = """
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

# Load data
st.title("Company Stocks and Movie Releases Correlation")
st.write("Analyze correlations between stock performance and movie releases, explore the most expensive stocks, and largest movie releases.")

# Section 1: Correlation between Stock Prices/Volatility and Movie Releases
st.header("Correlation: Stock Performance & Movie Releases")
correlation_data = load_data(query_correlation)

if not correlation_data.empty:
    st.write("The table below shows the correlation data:")
    st.dataframe(correlation_data)

    st.write("**Visualization: Average Close Price vs. Volatility (Movies)**")
    st.scatter_chart(correlation_data, x='Volatility', y='Average_Close_Price', color='Movie_Title')

else:
    st.write("No data available for correlation analysis.")

# Section 2: Most Expensive Company Stocks
st.header("Top 10 Most Expensive Company Stocks")
most_expensive_stocks = load_data(query_most_expensive_stocks)

if not most_expensive_stocks.empty:
    st.write("The table below shows the most expensive company stocks:")
    st.table(most_expensive_stocks)

    st.write("**Visualization: Top 10 Most Expensive Stocks**")
    st.bar_chart(most_expensive_stocks, x='Symbol', y='Max_Close_Price')

else:
    st.write("No data available for most expensive stocks.")

# Section 3: Largest Movie Releases
st.header("Top 10 Largest Movie Releases")
largest_movie_releases = load_data(query_largest_movie_releases)

if not largest_movie_releases.empty:
    st.write("The table below shows the largest movie releases:")
    st.table(largest_movie_releases)

    st.write("**Visualization: Popularity vs. Vote Average (Top Movies)**")
    st.scatter_chart(largest_movie_releases, x='Vote_Average', y='Popularity_Score', color='Title')

else:
    st.write("No data available for largest movie releases.")

st.write("End of the application.")