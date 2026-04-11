import streamlit as st
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

st.set_page_config(page_title="Market OverView",page_icon="🌍", layout="wide")

load_dotenv()

@st.cache_resource 
def init_connection():
    return psycopg2.connect(os.getenv("DATABASE_URL"))

conn = init_connection()


try:
    # Try to ping the database to see if the connection is still alive
    conn.rollback()
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
except Exception: 
    st.cache_resource.clear()
    conn = init_connection()



# --- FETCH DATA ---
@st.cache_data(ttl=3600) 
def get_dashboard_data():
    query = """
        SELECT 
            p.record_date, p.code, s.company_name, p.close_price, 
            i.rsi_14, i.sma_50, i.stoch_k, i.stoch_d,
            f.graham_number
        FROM daily_prices p
        JOIN calculated_indicators i ON p.code = i.code AND p.record_date = i.record_date
        LEFT JOIN quarterly_financials f ON p.code = f.code 
            AND f.record_date = (SELECT MAX(record_date) FROM quarterly_financials WHERE code = p.code)
        LEFT JOIN stocks s ON p.code = s.code
        WHERE p.record_date = (SELECT MAX(record_date) FROM daily_prices)
    """
    try:
        df = pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Database Error: Ensure you have a 'stocks' table with 'code' and 'company_name'. Detail: {e}")
        return pd.DataFrame()

    if not df.empty:
        df['company_name'] = df['company_name'].fillna('')
        df['Stock'] = df.apply(
            lambda row: f"{row['code']} - {row['company_name']}" if row['company_name'] else row['code'], 
            axis=1
        )
    return df

df = get_dashboard_data()

if not df.empty:
    latest_date = pd.to_datetime(df['record_date'].iloc[0]).strftime("%A, %B %d, %Y")
else:
    latest_date = "No Data Available"

st.title("🌍 Market OverView")
st.caption(f"🔄 **Data Last Updated:** {latest_date}")
st.markdown("### Market Overview (Latest Close)")

col1, col2, col3 = st.columns(3)

total_stocks = len(df)
above_50_sma = len(df[df['close_price'] > df['sma_50']]) if total_stocks > 0 else 0
breadth_pct = (above_50_sma / total_stocks) * 100 if total_stocks > 0 else 0
value_plays = len(df[df['close_price'] < df['graham_number']]) if total_stocks > 0 else 0

with col1:
    st.metric("Total Tracked", total_stocks)
with col2:
    st.metric("Market Breadth (> 50 SMA)", f"{breadth_pct:.1f}%")
with col3:
    st.metric("Value Opportunities", value_plays)

st.divider()

colA, colB = st.columns(2)

with colA:
    st.subheader("🏦 Deep Value (Price < Graham)")
    value_df = df[df['close_price'] < df['graham_number']].copy()
    
    if not value_df.empty:
        value_df['Discount (%)'] = ((value_df['graham_number'] - value_df['close_price']) / value_df['graham_number']) * 100
        
        display_df = value_df[['Stock', 'close_price', 'graham_number', 'Discount (%)']].sort_values('Discount (%)', ascending=False)
        display_df = display_df.rename(columns={
            'close_price': 'Close Price',
            'graham_number': 'Graham Number'
        })
        
        st.dataframe(
            display_df.style.format({
                'Close Price': '{:.2f}', 
                'Graham Number': '{:.2f}', 
                'Discount (%)': '{:.1f}%'
            }), 
            use_container_width=True, 
            hide_index=True
        )
    else:
        st.info("No deep value plays found today.")

with colB:
    st.subheader("📈 Oversold Momentum (RSI < 35)")
    mom_df = df[(df['rsi_14'] < 35) & (df['stoch_k'] > df['stoch_d'])].copy()
    
    if not mom_df.empty:
        display_mom = mom_df[['Stock', 'close_price', 'rsi_14', 'stoch_k']].sort_values('rsi_14')
        display_mom = display_mom.rename(columns={
            'close_price': 'Close Price',
            'rsi_14': 'RSI (14)',
            'stoch_k': 'Stochastic %K'
        })
        
        st.dataframe(
            display_mom.style.format({
                'Close Price': '{:.2f}', 
                'RSI (14)': '{:.1f}', 
                'Stochastic %K': '{:.1f}'
            }), 
            width='stretch', 
            hide_index=True
        )
    else:
        st.info("No oversold bounce setups found today.")