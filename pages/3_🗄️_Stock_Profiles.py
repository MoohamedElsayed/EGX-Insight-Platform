import streamlit as st
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

st.set_page_config(page_title="Stock Profiles", page_icon="🗄️", layout="wide")
load_dotenv()


NAME_MAPPING = {
    'pe_ratio': 'Price To Earnings',
    'price_to_book': 'Price To Book',
    'ev_ebitda': 'EV / EBITDA',
    'revenue': 'Total Revenue',
    'net_income': 'Net Income',
    'ebitda': 'EBITDA',
    'market_cap': 'Market Capitalization',
    'shares_outstanding': 'Shares Outstanding',
    'eps': 'Earnings Per Share',
    'book_value_per_share': 'Book Value Per Share',
    'dividend_yield_pct': 'Dividend Yield (%)',
    'gross_profit_margin_pct': 'Gross Profit Margin (%)',
    'roa_pct': 'Return On Assets',
    'roe_pct': 'Return On Equity',
    'beta': 'Beta',
    'one_year_change_pct': '1-Year Change (%)',
    'next_earnings_date': 'Next Earnings Date',
    'graham_number': 'Graham Number',
    
    'rsi_14': 'Relative Strength Index',
    'sma_50': 'Simple Moving Average 50',
    'sma_200': 'Simple Moving Average 200',
    'macd_line': 'MACD Line',
    'macd_signal': 'MACD Signal',
    'macd_hist': 'MACD Histogram',
    'bb_upper': 'Bollinger Band Upper',
    'bb_lower': 'Bollinger Band Lower',
    'bb_middle': 'Bollinger Band Middle',
    'atr_14': 'Average True Range',
    'stoch_k': 'Stochastic K',
    'stoch_d': 'Stochastic D'
}

# connecting to the data base
@st.cache_resource
def init_connection():
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    conn.autocommit = True  
    return conn

conn = init_connection()

try:
    # Try to ping the database to see if the connection is still alive
    conn.rollback()
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
except Exception: 
    st.cache_resource.clear()
    conn = init_connection()



@st.cache_data(ttl=3600)
def get_stock_dict():
    """Fetches both the code and the company name for a beautiful dropdown."""
    query = """
        SELECT DISTINCT p.code, s.company_name 
        FROM daily_prices p
        LEFT JOIN stocks s ON p.code = s.code
        ORDER BY p.code
    """
    try:
        df = pd.read_sql(query, conn)
        stock_dict = {}
        for _, row in df.iterrows():
            code = row['code']
            name = row['company_name']
            if pd.notna(name) and str(name).strip():
                stock_dict[code] = f"{code} - {name}"
            else:
                stock_dict[code] = code
        return stock_dict
    except:
        query = "SELECT DISTINCT code FROM daily_prices ORDER BY code"
        df = pd.read_sql(query, conn)
        return {code: code for code in df['code']}

@st.cache_data(ttl=3600)
def get_company_profile(stock_code):
    try:
        query = f"SELECT company_name, sector_name FROM stocks WHERE code = '{stock_code}'"
        df = pd.read_sql(query, conn)
        return df.iloc[0] if not df.empty else None
    except:
        return None 

@st.cache_data(ttl=3600)
def get_latest_financials(stock_code):
    query = f"""
        SELECT * FROM quarterly_financials 
        WHERE code = '{stock_code}' 
        ORDER BY record_date DESC LIMIT 1
    """
    return pd.read_sql(query, conn)

@st.cache_data(ttl=3600)
def get_latest_technicals(stock_code):
    query = f"""
        SELECT * FROM calculated_indicators 
        WHERE code = '{stock_code}' 
        ORDER BY record_date DESC LIMIT 1
    """
    return pd.read_sql(query, conn)


st.title("🗄️ Stock Profiles")

stock_names_dict = get_stock_dict()

selected_stock = st.selectbox(
    "Select a Stock to extract latest records", 
    options=list(stock_names_dict.keys()), 
    format_func=lambda x: stock_names_dict[x], 
    index=0
)

if selected_stock:
    profile = get_company_profile(selected_stock)
    if profile is not None:
        st.markdown(f"**{profile['company_name']}** | Sector: {profile['sector_name']}")
    
    st.divider()

    col_fund, col_tech = st.columns([1, 1], gap="large")

    with col_fund:
        st.subheader("📊 Fundamentals")
        fin_df = get_latest_financials(selected_stock)
        
        if not fin_df.empty:
            fin_display = fin_df.drop(columns=['id', 'code', 'record_date'], errors='ignore').T
            fin_display.columns = ['Value']
            fin_display = fin_display.dropna()
            
            fin_display = fin_display.rename(index=NAME_MAPPING)

            fin_display = fin_display.reset_index()
            fin_display.columns = ['Metric', 'Value']
            fin_display['Value'] = fin_display['Value'].astype(str)
            
            st.dataframe(fin_display, hide_index=True, width='stretch')
        else:
            st.info("No fundamental data found for this stock.")

    with col_tech:
        st.subheader("⚙️ Technical Posture")
        tech_df = get_latest_technicals(selected_stock)
        
        if not tech_df.empty:
            tech_data = tech_df.iloc[0]
            
            st.markdown("##### 🏎️ Momentum")
            m1, m2, m3 = st.columns(3)
            m1.metric(NAME_MAPPING.get('rsi_14', 'RSI'), f"{tech_data.get('rsi_14', 0):.2f}")
            m2.metric(NAME_MAPPING.get('stoch_k', 'Stoch K'), f"{tech_data.get('stoch_k', 0):.2f}")
            m3.metric(NAME_MAPPING.get('stoch_d', 'Stoch D'), f"{tech_data.get('stoch_d', 0):.2f}")
            
            st.markdown("<br>", unsafe_allow_html=True) 
            
            st.markdown("##### 📈 Trend & Volatility")
            t1, t2, t3 = st.columns(3)
            t1.metric(NAME_MAPPING.get('sma_50', 'SMA 50'), f"{tech_data.get('sma_50', 0):.2f}")
            t2.metric(NAME_MAPPING.get('sma_200', 'SMA 200'), f"{tech_data.get('sma_200', 0):.2f}")
            t3.metric(NAME_MAPPING.get('atr_14', 'ATR'), f"{tech_data.get('atr_14', 0):.2f}")

            st.markdown("<br>", unsafe_allow_html=True)

            st.markdown("##### 🎯 MACD & Bollinger")
            b1, b2, b3 = st.columns(3)
            b1.metric(NAME_MAPPING.get('macd_line', 'MACD Line'), f"{tech_data.get('macd_line', 0):.2f}")
            b2.metric(NAME_MAPPING.get('macd_signal', 'MACD Signal'), f"{tech_data.get('macd_signal', 0):.2f}")
            
            bb_u = tech_data.get('bb_upper', 0)
            bb_l = tech_data.get('bb_lower', 0)
            b3.metric("Bollinger Band Range", f"{bb_l:.2f} - {bb_u:.2f}" if pd.notna(bb_l) else "N/A")

        else:
            st.info("No technical indicators calculated yet.")