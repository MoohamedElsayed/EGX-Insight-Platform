import yfinance as yf
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv 

load_dotenv()

conn_string = os.getenv("DATABASE_URL")
CSV_FILE = os.path.join("data", "stocks.csv")


def fetch_daily_prices():

    if not conn_string:
        print("conn_string is missing. Check your environment variables")
        return
    
    conn = None

    try: 

        print("Connecting to the database...")

        with psycopg2.connect(conn_string) as conn:
            print("Connection established")


            with conn.cursor()as cur:

                # Fetching the data and putting them into the database
                cur.execute("SELECT code FROM stocks")
                stock_codes = [row[0] for row in cur.fetchall()] 

                print(f"Fetching prices for {len(stock_codes)} stocks...")


                insert_sql = """
                INSERT INTO daily_prices (record_date, code, close_price)
                VALUES (%s, %s, %s)
                ON CONFLICT (record_date, code) DO NOTHING;
                """




                for code in stock_codes:

                    ticker_symbol = f"{code}.CA"

                    df = yf.download(
                    ticker_symbol,
                    period="10d",
                    interval="1d",
                    progress=False
                    )

                    today = pd.Timestamp.utcnow().date()

                    df = df[df.index.date < today]

                    if df.empty:
                       print("\nCouldn't Find any data of the company for the last 10 days")
                       continue

                    df = df.sort_index()

                    
                    last_completed_trading_day = df.iloc[-1]

                    record = {
                        "record_date": last_completed_trading_day.name.date(),
                        "code": code,
                        "close_price": last_completed_trading_day["Close"].item()
                    }


                    cur.execute(
                        insert_sql,
                        (
                            record["record_date"],                            
                            record["code"],
                            record["close_price"]
                        )
                    )

                    print(f"\n Finished updating {code} daily price")

            conn.commit()
            print("\n Daily prices updated successfully")


    except Exception as e:
        print("Couldn't connect to the database")
        print(f"\n Error:{e}")
    

def fetch_prices_since_1_1_2024():

    if not conn_string:
        print("conn_string is missing. Check your environment variables")
        return
    
    conn = None

    try: 

        print("Connecting to the database...")

        with psycopg2.connect(conn_string) as conn:
            print("Connection established")


            with conn.cursor()as cur:

                # Fetching the data and putting them into the database
                cur.execute("SELECT code FROM stocks")
                stock_codes = [row[0] for row in cur.fetchall()] 

                print(f"Fetching historical prices for {len(stock_codes)} stocks...")

                insert_sql = """
                INSERT INTO daily_prices (record_date, code, close_price)
                VALUES (%s, %s, %s)
                ON CONFLICT (record_date, code) DO NOTHING;
                """


                for code in stock_codes:

                    ticker_symbol = f"{code}.CA"

                    df = yf.download(
                    ticker_symbol,
                    start = "2024-01-01",
                    interval="1d",
                    progress=False
                    )


                    if df.empty:
                       print("\nCouldn't Find any historical prices for this stock")
                       continue
                    
                    df = df.sort_index()


                    for trade_date, row in df.iterrows():

                        if pd.isna(row["Close"].item()):
                            continue

                        cur.execute(
                            insert_sql,
                            (
                                trade_date.date(),
                                code,
                                row["Close"].item()
                            )
                        )

                    print(f"\n Finished filling historical data for {code}")

            conn.commit()
            print("\n Historical prices added successfully")


    except Exception as e:
        print("Couldn't connect to the database")
        print(f"\n Error:{e}")












if __name__ == "__main__":
    fetch_daily_prices()
    # fetch_prices_since_1_1_2024()


