import streamlit as st

st.set_page_config(
    page_title="EGX Insight Platform",
    page_icon="🦅",
    layout="centered" 
)

st.title("🦅 Welcome to EGX Insight")
st.subheader("Automated Data Pipeline & Analysis for the Egyptian Stock Exchange (EGX100)")

st.divider()

st.markdown("""
This platform is powered by an automated ETL pipeline that scrapes daily prices, technical indicators, and fundamental data for EGX100-listed companies.

### 👈 How to navigate (Use the sidebar)

* **🌍 Market Overview:** Your main command center. View market breadth, total tracked stocks, and daily scanners for Deep Value (Graham Number) and Oversold Momentum (RSI/Stochastics).
* **📈 Technical Charts:** A dedicated charting engine. Search for any stock to view its Japanese Candlestick history alongside Bollinger Bands, SMAs, and MACD.
* **🗄️ Stock Profiles:** The deep dive. Extract and view the absolute latest fundamental balance sheets and technical indicator metrics for specific companies.
""")

st.divider()

st.markdown(
    """
    <div style="text-align: center; color: gray; font-size: medium;">
        🚀 Built by <b>Mohamed Elsayed Mansour</b> <br>
        <a href="https://www.linkedin.com/in/mohamed-elsayed-de" target="_blank" style="text-decoration: none; color: #0A66C2;">LinkedIn</a> • 
        <a href="https://github.com/MoohamedElsayed" target="_blank" style="text-decoration: none; color: #0A66C2;">GitHub</a>
        <br><br>
    </div>
    """, 
    unsafe_allow_html=True
)
