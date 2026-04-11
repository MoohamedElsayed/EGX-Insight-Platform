import streamlit as st
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
import plotly.graph_objects as go

st.set_page_config(page_title="Technical Charts", page_icon="📈", layout="wide")
load_dotenv()

@st.cache_resource
def init_connection():
    return psycopg2.connect(os.getenv("DATABASE_URL"))

conn = init_connection()

try:
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
except (psycopg2.OperationalError, psycopg2.InterfaceError):
    # If the cloud DB closed the connection, clear the cache and reconnect
    st.cache_resource.clear()
    conn = init_connection()




@st.cache_data(ttl=3600)
def get_stock_list():
    query = "SELECT DISTINCT code FROM daily_prices ORDER BY code"
    df = pd.read_sql(query, conn)
    return df['code'].tolist()

@st.cache_data(ttl=3600)
def get_stock_data(stock_code):
    """Fetches price history AND the latest indicators/fundamentals."""
    query = f"""
        SELECT 
            p.record_date, p.open_price, p.high_price, p.low_price, p.close_price,
            i.rsi_14, i.sma_50, i.sma_200, 
            i.bb_upper, i.bb_lower, i.macd_line, i.macd_signal, i.macd_hist,
            f.graham_number
        FROM daily_prices p
        LEFT JOIN calculated_indicators i ON p.code = i.code AND p.record_date = i.record_date
        LEFT JOIN quarterly_financials f ON p.code = f.code 
            AND f.record_date = (
                SELECT MAX(record_date) FROM quarterly_financials WHERE code = p.code AND record_date <= p.record_date
            )
        WHERE p.code = '{stock_code}'
        ORDER BY p.record_date DESC
    """
    df = pd.read_sql(query, conn)
    return df.sort_values('record_date')

st.title("📈 Technical Charts")

available_stocks = get_stock_list()
selected_stock = st.selectbox("Search and Select a Stock", available_stocks, index=0)

if selected_stock:
    df = get_stock_data(selected_stock)

    if not df.empty:
        latest = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else latest
        
        change = latest['close_price'] - prev['close_price']
        change_pct = (change / prev['close_price']) * 100 if prev['close_price'] > 0 else 0

        col_chart, col_data = st.columns([3, 1])

        with col_chart:
            fig = go.Figure()

            fig.add_trace(go.Candlestick(
                x=df['record_date'],
                open=df['open_price'],
                high=df['high_price'],
                low=df['low_price'],
                close=df['close_price'],
                name='Price',
                increasing_line_color='#26a69a', 
                decreasing_line_color='#ef5350'  
            ))

            fig.add_trace(go.Scatter(
                x=df['record_date'], y=df['bb_upper'], 
                line=dict(color='gray', width=1, dash='dot'), 
                showlegend=False, hoverinfo='skip', name='BB Upper'
            ))

            fig.add_trace(go.Scatter(
                x=df['record_date'], y=df['bb_lower'], 
                line=dict(color='gray', width=1, dash='dot'), 
                fill='tonexty', fillcolor='rgba(128,128,128,0.1)', 
                hoverinfo='skip', name='Bollinger Bands'
            ))

            fig.update_layout(
                title=f"{selected_stock} ",
                yaxis_title="Price (EGP)",
                xaxis_rangeslider_visible=False,
                height=600,
                margin=dict(l=0, r=0, t=40, b=0),
                hovermode="x unified",
                template="plotly_dark"
            )
            st.plotly_chart(fig, width='stretch')

        with col_data:
            st.markdown("### Key Statistics")
            
            st.metric("Current Price", f"{latest['close_price']:.2f}", f"{change:.2f} ({change_pct:.2f}%)")
            
            rsi_val = latest['rsi_14']
            if pd.notna(rsi_val):
                rsi_status = "🔴 Overbought" if rsi_val > 70 else "🟢 Oversold" if rsi_val < 30 else "⚪ Neutral"
                st.metric("RSI (14)", f"{rsi_val:.1f}", rsi_status, delta_color="off")
            else:
                st.metric("RSI (14)", "N/A")

            macd_line = latest['macd_line']
            macd_signal = latest['macd_signal']
            if pd.notna(macd_line) and pd.notna(macd_signal):
                macd_status = "🟢 Bullish Crossover" if macd_line > macd_signal else "🔴 Bearish Crossover"
                st.metric("MACD Trend", f"{macd_line:.2f}", macd_status, delta_color="off")
            else:
                st.metric("MACD Trend", "N/A")

            graham = latest['graham_number']
            if pd.notna(graham):
                val_status = "🟢 Undervalued" if latest['close_price'] < graham else "🔴 Overvalued"
                st.metric("Graham Fair Value", f"{graham:.2f}", val_status, delta_color="off")
            else:
                st.metric("Graham Fair Value", "N/A")

            st.markdown("**Moving Averages**")
            sma_50 = latest['sma_50']
            sma_200 = latest['sma_200']
            
            trend_data = {
                "Indicator": ["50-Day SMA", "200-Day SMA"],
                "Value": [
                    f"{sma_50:.2f}" if pd.notna(sma_50) else "N/A", 
                    f"{sma_200:.2f}" if pd.notna(sma_200) else "N/A"
                ],
                "Status": [
                    "🟢 Above" if pd.notna(sma_50) and latest['close_price'] > sma_50 else "🔴 Below",
                    "🟢 Above" if pd.notna(sma_200) and latest['close_price'] > sma_200 else "🔴 Below"
                ]
            }
            st.dataframe(pd.DataFrame(trend_data), hide_index=True, use_container_width=True)

    else:
        st.warning("No data found for this stock.")