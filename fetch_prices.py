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
                cur.execute("SELECT code, isin_code FROM stocks")
                stock_codes = [[row[0],row[1]] for row in cur.fetchall()] 

                print(f"Fetching prices for {len(stock_codes)} stocks...")


                insert_sql = """
                INSERT INTO daily_prices (record_date, code, close_price, high_price, low_price, open_price)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (record_date, code) DO UPDATE SET
                        close_price = EXCLUDED.close_price,
                        high_price = EXCLUDED.high_price,
                        low_price = EXCLUDED.low_price,
                        open_price = EXCLUDED.open_price;
                """


                for stock_code in stock_codes:
                    code = stock_code[0]
                    isin_code = stock_code[1]

                    ticker_isin_code = f"{isin_code}.CA"

                    df = yf.download(
                    ticker_isin_code,
                    period="10d",
                    interval="1d",
                    progress=False
                    )

                    if df.empty:
                       print("\nCouldn't Find any data of the company for the last 10 days")
                       continue

                    df = df.sort_index()

                    
                    last_completed_trading_day = df.iloc[-1]

                    record = {
                        "record_date": last_completed_trading_day.name.date(),
                        "code": code,
                        "close_price": last_completed_trading_day["Close"].item(),
                        "high_price": last_completed_trading_day["High"].item(),
                        "low_price": last_completed_trading_day["Low"].item(),
                        "open_price": last_completed_trading_day["Open"].item()
                    }


                    cur.execute(
                        insert_sql,
                        (
                            record["record_date"],                            
                            record["code"],
                            record["close_price"],
                            record["high_price"],
                            record["low_price"],
                            record["open_price"]
                        )
                    )

                    print(f"\n Finished updating {code} daily price")

            conn.commit()
            print("\n Daily prices updated successfully")


    except Exception as e:
        print("Couldn't connect to the database")
        print(f"\n Error:{e}")
    



if __name__ == "__main__":
    fetch_daily_prices()


