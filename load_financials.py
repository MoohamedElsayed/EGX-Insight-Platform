import pandas as pd
from dotenv import load_dotenv 
import os
import psycopg2
import numpy as np
from datetime import datetime

load_dotenv()
conn_string = os.getenv("DATABASE_URL")
CSV_FILE = os.path.join("data", "stocks_quartely_financials.csv")


def clean_nan(value):
    """
    Converts pandas NaN (Not a Number) to Python None (NULL in SQL).
    """
    if pd.isna(value) or value == "NaT":
        return None
    return value




def clean_data():

    df = pd.read_csv(CSV_FILE, na_values=['-'])
    
    local_eps = df['Prev. Close'] / df['P/E Ratio'].astype(float)

    EX_Factor = local_eps / df['EPS']

    columns_to_drop = ["Source_URL", "Bid/Ask", "Prev. Close","Open", "Day's Range", "52 wk Range",
                  "Volume", "Average Vol. (3m)", "Fair Value", "Fair Value Upside", "EPS Growth Forecast",
                    "Dividends PaymentStreak", "RSI(14)",  "ISIN"]
    df.drop(columns = columns_to_drop , inplace= True)

    df['1-Year Change'] = df['1-Year Change'].str.replace("%", "").astype(float)
    


    df['Market Cap'] = df['Market Cap'].apply(lambda x:
                                         x if not isinstance(x, str) else (
                                         float(x[:-1]) * 1e9 if x.endswith('B') else (
                                         float(x[:-1]) * 1e6 if x.endswith('M') else (
                                         float(x[:-1]) * 1e3 if x.endswith('K') else (
                                         float(x) 
                                         )))))
    

    df['Shares Outstanding'] = df['Shares Outstanding'].apply(lambda x:
                                         x if not isinstance(x, str) else (
                                         float(x[:-1]) * 1e9 if x.endswith('B') else (
                                         float(x[:-1]) * 1e6 if x.endswith('M') else (
                                         float(x[:-1]) * 1e3 if x.endswith('K') else (
                                         float(x) 
                                         )))))
    
    df['Revenue'] = df['Revenue'].apply(lambda x:
                                         x if not isinstance(x, str) else (
                                         float(x[:-1]) * 1e9 if x.endswith('B') else (
                                         float(x[:-1]) * 1e6 if x.endswith('M') else (
                                         float(x[:-1]) * 1e3 if x.endswith('K') else (
                                         float(x) 
                                         ))))) * EX_Factor
    

    df['Net Income'] = df['Net Income'].apply(lambda x:
                                         x if not isinstance(x, str) else (
                                         float(x[:-1]) * 1e9 if x.endswith('K') else (
                                         float(x) * 1e6 
                                         ))) * EX_Factor
    
    df['EPS'] = df['EPS'] * EX_Factor

    df['Next Earnings Date'] = pd.to_datetime(df['Next Earnings Date'], format='%b %d, %Y')

    df['Dividend (Yield)'] = pd.to_numeric(df['Dividend (Yield)'].str.extract(r'\((.*?)\)')[0].str.replace('%', ''), errors='coerce')

    df['P/E Ratio'] = df['P/E Ratio'].astype(float)

    df['Return on Assets'] = df['Return on Assets'].str.replace("%", "").astype(float)

    df['Return on Equity'] = df['Return on Equity'].str.replace("%", "").astype(float)

    df['Gross Profit Margin'] = df['Gross Profit Margin'].str.replace("%", "").astype(float)

    df['Price/Book'] = df['Price/Book'].astype(float)

    df['EBITDA'] = df['EBITDA'].apply(lambda x:
                                         x if not isinstance(x, str) else (
                                         float(x[:-1]) * 1e9 if x.endswith('B') else (
                                         float(x[:-1]) * 1e6 if x.endswith('M') else (
                                         float(x[:-1]) * 1e3 if x.endswith('K') else (
                                         float(x) 
                                         ))))) * EX_Factor
    
    df['EV/EBITDA'] = df['EV/EBITDA'].astype(float)

    df['Beta'] = df['Beta'].astype(float)

    df['Book Value / Share'] = df['Book Value / Share'].astype(float)

    return df


def load_data(df):
    
    column_mapping = {
        "Symbol": "code",
        "1-Year Change": "one_year_change_pct",
        "Market Cap": "market_cap",
        "Shares Outstanding": "shares_outstanding",
        "Revenue": "revenue",
        "Net Income": "net_income",
        "EPS": "eps",
        "Next Earnings Date": "next_earnings_date",
        "Dividend (Yield)": "dividend_yield_pct",
        "P/E Ratio": "pe_ratio",
        "Return on Assets": "roa_pct",
        "Return on Equity": "roe_pct",
        "Gross Profit Margin": "gross_profit_margin_pct",
        "Price/Book": "price_to_book",
        "EBITDA": "ebitda",
        "EV/EBITDA": "ev_ebitda",
        "Beta": "beta",
        "Book Value / Share": "book_value_per_share"
    }

    df.rename(columns=column_mapping, inplace=True)

    product = 22.5 * df['eps'].fillna(0) * df['book_value_per_share'].fillna(0)
    df['graham_number'] = product.apply(lambda x: np.sqrt(x) if x > 0 else None)

    if not conn_string:
        print("conn_string is missing. Check your environment variables")
        return
    
    conn = None


    try:
        conn = psycopg2.connect(conn_string)
        print("Connection established")
        
        cur = conn.cursor()

        print(f"Processing {len(df)} rows")

        for index, row in df.iterrows():

            if pd.isna(row['code']):
                continue

            data_tuple = (
            clean_nan(row.get('code')),
            clean_nan(row.get('one_year_change_pct')),
            clean_nan(row.get('market_cap')),
            clean_nan(row.get('shares_outstanding')),
            clean_nan(row.get('revenue')),
            clean_nan(row.get('net_income')),
            clean_nan(row.get('eps')),
            clean_nan(row.get('next_earnings_date')),
            clean_nan(row.get('dividend_yield_pct')),
            clean_nan(row.get('pe_ratio')),
            clean_nan(row.get('roa_pct')),
            clean_nan(row.get('roe_pct')),
            clean_nan(row.get('gross_profit_margin_pct')),
            clean_nan(row.get('price_to_book')),
            clean_nan(row.get('ebitda')),
            clean_nan(row.get('ev_ebitda')),
            clean_nan(row.get('beta')),
            clean_nan(row.get('book_value_per_share')),
            clean_nan(row.get('graham_number'))

            )
            

            sql = """
            INSERT INTO quarterly_financials (
                record_date, code, 
                one_year_change_pct, market_cap, shares_outstanding, revenue, net_income, 
                eps, next_earnings_date, dividend_yield_pct, pe_ratio, roa_pct, 
                roe_pct, gross_profit_margin_pct, price_to_book, ebitda, ev_ebitda, 
                beta, book_value_per_share, graham_number 
            ) VALUES (
                CURRENT_DATE, %s, 
                %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, 
                %s, %s,%s
            )
            ON CONFLICT (record_date, code) 
            DO UPDATE SET 
                one_year_change_pct = EXCLUDED.one_year_change_pct,
                market_cap = EXCLUDED.market_cap,
                shares_outstanding = EXCLUDED.shares_outstanding,
                revenue = EXCLUDED.revenue,
                net_income = EXCLUDED.net_income,
                eps = EXCLUDED.eps,
                next_earnings_date = EXCLUDED.next_earnings_date,
                dividend_yield_pct = EXCLUDED.dividend_yield_pct,
                pe_ratio = EXCLUDED.pe_ratio,
                roa_pct = EXCLUDED.roa_pct,
                roe_pct = EXCLUDED.roe_pct,
                gross_profit_margin_pct = EXCLUDED.gross_profit_margin_pct,
                price_to_book = EXCLUDED.price_to_book,
                ebitda = EXCLUDED.ebitda,
                ev_ebitda = EXCLUDED.ev_ebitda,
                beta = EXCLUDED.beta,
                book_value_per_share = EXCLUDED.book_value_per_share,
                graham_number = EXCLUDED.graham_number;
                """
            
            cur.execute(sql, data_tuple)

            conn.commit()

        print("Success! Financial data loaded into database.")

    except Exception as e:
        print(f"Database Error: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()


if __name__ =="__main__":
    df = clean_data()
    load_data(df)
