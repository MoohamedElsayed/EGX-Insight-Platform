import pandas as pd
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

conn_string = os.getenv("DATABASE_URL")

CSV_FOLDER = os.path.join("data", "historical_stocks_data")


def load_csvs():

    if not os.path.exists(CSV_FOLDER):
        print(f"Error: The folder '{CSV_FOLDER}' does not exist.")
        return

    try :

        conn = psycopg2.connect(conn_string)
        print("Connection established")

        cur = conn.cursor()

        files = [f for f in os.listdir(CSV_FOLDER) if f.endswith('.csv')]
        print(f"Found {len(files)} CSV files.")


        for filename in files:
            stock_code = os.path.splitext(filename)[0].upper()
            file_path = os.path.join(CSV_FOLDER, filename) 

            try:

                df = pd.read_csv(file_path, usecols=['Date', 'Price', 'Open', 'High', 'Low'])

                df = df.rename(columns={
                    'Date': 'record_date',
                    'Price': 'close_price',
                    'Open': 'open_price',
                    'High': 'high_price',
                    'Low': 'low_price'
                })

                df['record_date'] = pd.to_datetime(df['record_date'])


                data_tuples = []

                for _, row in df.iterrows():
                    data_tuples.append((
                    row['record_date'], 
                    stock_code, 
                    row['close_price'], 
                    row['high_price'], 
                    row['low_price'], 
                    row['open_price']
                ))
                    

                sql = """
                    INSERT INTO daily_prices (
                        record_date, code, close_price, high_price, low_price, open_price) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (record_date, code) DO UPDATE SET
                        close_price = EXCLUDED.close_price,
                        high_price = EXCLUDED.high_price,
                        low_price = EXCLUDED.low_price,
                        open_price = EXCLUDED.open_price;
                """

                cur.executemany(sql, data_tuples)
                conn.commit()
                print(f"{stock_code}: Loaded {len(df)} rows.")

            except Exception as e:
                print(f"Error processing {filename}: {e}")
                conn.rollback()

        conn.close()
        print("Historical data import complete!")

    except:
        print("Couldn't connect to the database")
        print(f"\n Error:{e}")

if __name__ == "__main__":
    load_csvs()