import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import os
from dotenv import load_dotenv

st.set_page_config(page_title="Stock Analysis", layout="wide")

load_dotenv()
conn_string = os.getenv('DATABASE_URL')

conn = psycopg2.connect(conn_string)

stocks_list = pd.read_sql("SELECT code FROM stocks ORDER BY code", conn)['code'].tolist()

selected_stock = st.sidebar.selectbox("Select a Stock", stocks_list)

st.title(f"📊 Analysis: {selected_stock}")

price_query = f"""
    SELECT record_date, close_price 
    FROM daily_prices 
    WHERE code = '{selected_stock}' 
    ORDER BY record_date ASC
"""
df_prices = pd.read_sql(price_query, conn)

fund_query = f"""
    SELECT * FROM quarterly_financials 
    WHERE code = '{selected_stock}' 
    ORDER BY record_date DESC 
    LIMIT 1
"""
df_funds = pd.read_sql(fund_query, conn)

col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Price History")
    if not df_prices.empty:
        fig = px.line(df_prices, x='record_date', y='close_price', title=f"{selected_stock} Daily Close Price")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No price data available yet (Run fetch_prices.py!)")

with col2:
    st.subheader("Fundamentals")
    if not df_funds.empty:
        data = df_funds.iloc[0]
        st.metric("P/E Ratio", f"{data['pe_ratio']}")
        st.metric("EPS", f"{data['eps']}")
        st.metric("Dividend Yield", f"{data['dividend_yield_pct']}%")
        st.metric("Market Cap", f"{data['market_cap']:,.0f}")
        st.metric("Revenue", f"{data['revenue']:,.0f}")
    else:
        st.info("No fundamental data scraped yet.")