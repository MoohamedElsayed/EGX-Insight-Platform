import requests
import psycopg2
import os
from dotenv import load_dotenv
from datetime import date, timedelta

load_dotenv()

conn_string = os.getenv("DATABASE_URL")


def get_last_trading_day():
    """
    EGX trades Sunday-Thursday.
    Closed on Friday & Saturday.
    Script runs at 8am before market opens.
    """
    today = date.today()
    weekday = today.weekday()

    if weekday == 5:   
        return today - timedelta(days=2)
    elif weekday == 6: 
        return today - timedelta(days=3)
    elif weekday == 0: 
        return today - timedelta(days=1)
    else:              
        return today - timedelta(days=1)


def fetch_daily_prices():

    if not conn_string:
        print("conn_string is missing. Check your environment variables")
        return

    try:
        print("Connecting to the database...")

        with psycopg2.connect(conn_string) as conn:
            print("Connection established")

            with conn.cursor() as cur:

                cur.execute("SELECT code FROM stocks")
                stock_codes = [row[0] for row in cur.fetchall()]

                print(f"Fetching prices for {len(stock_codes)} stocks...")

                tickers = [f"EGX:{code}" for code in stock_codes]

                url = "https://scanner.tradingview.com/egypt/scan"
                payload = {
                    "symbols": {"tickers": tickers},
                    "columns": ["close", "open", "high", "low"]
                }
                headers = {"Content-Type": "application/json"}

                response = requests.post(url, json=payload, headers=headers, timeout=30)
                response.raise_for_status()
                data = response.json()

                if "data" not in data or len(data["data"]) == 0:
                    print("No data returned from TradingView")
                    return

                record_date = get_last_trading_day()
                print(f"Received data for {len(data['data'])} stocks")
                print(f"Recording date: {record_date} ({record_date.strftime('%A')})")

                cur.execute("""
                    SELECT code, close_price 
                    FROM daily_prices 
                    WHERE record_date = (
                        SELECT MAX(record_date) FROM daily_prices
                    )
                """)
                last_prices = {row[0]: float(row[1]) for row in cur.fetchall()}

                if last_prices:
                    sample_matches = 0
                    sample_total = 0

                    for item in data["data"][:10]:
                        code = item["s"].replace("EGX:", "")
                        new_close = item["d"][0]
                        if code in last_prices and new_close is not None:
                            sample_total += 1
                            if abs(last_prices[code] - new_close) < 0.001:
                                sample_matches += 1

                    if sample_total > 0 and sample_matches == sample_total:
                        print(f"\nAll sampled prices identical to last record.")
                        print(f"Likely a holiday. Skipping update.")
                        return

                insert_sql = """
                INSERT INTO daily_prices (record_date, code, close_price, high_price, low_price, open_price)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (record_date, code) DO UPDATE SET
                        close_price = EXCLUDED.close_price,
                        high_price = EXCLUDED.high_price,
                        low_price = EXCLUDED.low_price,
                        open_price = EXCLUDED.open_price;
                """

                for item in data["data"]:
                    code = item["s"].replace("EGX:", "")
                    values = item["d"]

                    close_price = values[0]
                    open_price = values[1]
                    high_price = values[2]
                    low_price = values[3]

                    if None in [close_price, open_price, high_price, low_price]:
                        print(f"\nCouldn't find any data for {code}")
                        continue

                    cur.execute(
                        insert_sql,
                        (
                            record_date,
                            code,
                            close_price,
                            high_price,
                            low_price,
                            open_price
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