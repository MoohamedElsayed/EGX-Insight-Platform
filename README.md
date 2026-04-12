# 🦅 EGX Insight Platform

**Live Dashboard:** [Click here to view the live app](https://egx-insight-platform.streamlit.app/)

<img width="2524" height="1074" alt="image" src="https://github.com/user-attachments/assets/0e07df4c-e729-46f5-9b65-e34c6c79041c" />


## Overview
The EGX Insight Platform is an automated end-to-end data pipeline and analytics dashboard built to monitor, analyze, and discover trading opportunities on the Egyptian Stock Exchange (EGX100). 

This project solves the problem of fragmented and delayed EGX market data by automatically extracting daily price action, calculating institutional-grade technical indicators, and aggregating fundamental financial data into a centralized, highly accessible interactive terminal.

---

## 🏗️ System Architecture & Data Flow

The platform is fully automated, utilizing a modern Data Engineering stack to ensure data is fresh, accurate, and instantly available.



* **1. Extraction & Automation:** GitHub Actions triggers a daily workflow post-market close, executing Python scrapers to pull live market data and fundamental balance sheets.
* **2. Transformation:** Data is cleaned and enriched using Pandas. Complex technical indicators (RSI, Bollinger Bands, MACD) and value metrics (Graham Number) are calculated mathematically before storage.
* **3. Loading:** The structured data is securely pushed into a cloud-hosted serverless PostgreSQL database (NeonDB).
* **4. Presentation:** A Streamlit web application queries the database in real-time, utilizing query caching and state management to deliver a lightning-fast UI.

![final final](https://github.com/user-attachments/assets/27a625fc-f5a8-44c6-84d6-0217ccee65f9)

  
<img width="1188" height="792" alt="EGX Insight Platform" src="https://github.com/user-attachments/assets/65549c36-1a45-46ad-889b-f96e5416b7bb" />


---

## 🛠️ Tech Stack

**Frontend & Data Visualization**
* `Streamlit` - Web framework and UI components
* `Plotly` - Interactive financial charting (Candlesticks, moving averages)

**Backend ETL & Automation**
* `GitHub Actions` - CI/CD and daily pipeline orchestration
* `yfinance`, `curl_cffi`, `beautifulsoup4` - Web scraping and market data extraction
* `pandas`, `pandas_ta` - Data manipulation and technical indicator math

**Database & Cloud Infrastructure**
* `PostgreSQL` (NeonDB) - Serverless relational database
* `psycopg2-binary` - Python-to-Postgres connection engine

---

## 📊 Core Platform Features

### 1. Market Overview & Scanners
* **Breadth Tracking:** Instant metrics on the number of tracked stocks and total market coverage.
* **Deep Value Screener:** Automatically flags stocks trading below their calculated Graham Number.
* **Momentum Screener:** Identifies mathematically oversold conditions using RSI and Stochastic oscillators.

### 2. Interactive Technical Charting
* Searchable interface for all supported EGX equities.
* Interactive Japanese Candlestick charts featuring 50-day and 200-day Simple Moving Averages.
* Integrated Volume, MACD, and Bollinger Band overlays for volatility and trend confirmation.

### 3. Fundamental Data Hub
* Extracts the absolute latest balance sheet and income statement metrics.
* Displays critical financial health markers, including P/E Ratio, Dividend Yield, and Return on Equity (ROE).
---

## 👨‍💻 Author

**Mohamed Elsayed Mansour** *Data Engineer & Data Analyst*

* **LinkedIn:** [Mohamed Elsayed Mansour](https://www.linkedin.com/in/mohamed-elsayed-de)
* **GitHub:** [@MoohamedElsayed](https://github.com/MoohamedElsayed)
