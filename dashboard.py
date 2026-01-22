import streamlit as st
import pandas as pd
import psycopg2
import os 
from dotenv import load_dotenv

st.set_page_config(page_title="EGX Insight", layout="wide")
st.title("🇪🇬 EGX Market Overview")

load_dotenv()

conn_string = os.getenv('DATABASE_URL')

conn = psycopg2.connect(conn_string) 

query = """
    SELECT 
        s.code, 
        s.company_name, 
        f.market_cap, 
        f.pe_ratio, 
        f.one_year_change_pct,
        f.revenue
    FROM stocks s
    JOIN quarterly_financials f ON s.code = f.code
    WHERE f.record_date = (
        SELECT MAX(record_date) FROM quarterly_financials WHERE code = s.code
    )
    ORDER BY f.market_cap DESC;
"""

df = pd.read_sql(query, conn)

col1, col2, col3 = st.columns(3)
col1.metric("Total Stocks Tracked", len(df))
col2.metric("Top Gainer (1Y)", f"{df.iloc[0]['code']}", f"{df.iloc[0]['one_year_change_pct']}%")
col3.metric("Largest Market Cap", f"{df.iloc[0]['code']}", f"{df.iloc[0]['market_cap']:,.0f}")

st.subheader("Market Data")
st.dataframe(
    df,
    column_config={
        "market_cap": st.column_config.NumberColumn("Market Cap", format="EGP %.2f"),
        "revenue": st.column_config.NumberColumn("Revenue", format="EGP %.2f"),
        "one_year_change_pct": st.column_config.NumberColumn("1Y Change", format="%.2f%%"),
    },
    use_container_width=True,
    hide_index=True
)