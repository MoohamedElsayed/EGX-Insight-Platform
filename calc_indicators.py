import pandas as pd
import pandas_ta as ta
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn_string = os.getenv("DATABASE_URL")


def calculate_and_store():

    if not conn_string:
        print("conn_string is missing. Check your environment variables")
        return
    
    conn = None

    try:

        print("Connecting to the database...")

        with psycopg2.connect(conn_string) as conn:
            print("Connection established")

            print("Fetching price history")
            query = """
                SELECT record_date, code, 
                    open_price AS open, 
                    high_price AS high, 
                    low_price AS low, 
                    close_price AS close 
                FROM daily_prices 
                ORDER BY code, record_date ASC
            """

            all_data = pd.read_sql(query, conn, parse_dates=['record_date'])

            stock_codes = all_data['code'].unique()
            print(f"Processing indicators for {len(stock_codes)} stocks")

            cursor = conn.cursor()

            for code in stock_codes:

                df = all_data[all_data['code'] == code].copy()
                

                if len(df) < 20: 
                    continue

                df.set_index('record_date', inplace=True)

                df['rsi_14'] = ta.rsi(df['close'], length=14)

                df['sma_50'] = ta.sma(df['close'], length=50)
                df['sma_200'] = ta.sma(df['close'], length=200)

                macd = ta.macd(df['close'], fast=12, slow=26, signal=9)
                if macd is not None:
                    df['macd_line'] = macd['MACD_12_26_9']
                    df['macd_hist'] = macd['MACDh_12_26_9']
                    df['macd_signal'] = macd['MACDs_12_26_9']


                bb = ta.bbands(df['close'], length=20, std=2.0)
                if bb is not None:
                    df['bb_lower'] = bb.iloc[:, 0]
                    df['bb_middle'] = bb.iloc[:, 1]
                    df['bb_upper'] = bb.iloc[:, 2]

                df['atr_14'] = ta.atr(df['high'], df['low'], df['close'], length=14)

                stoch = ta.stoch(df['high'], df['low'], df['close'], k=14, d=3, smooth_k=3)
                if stoch is not None:
                    df['stoch_k'] = stoch['STOCHk_14_3_3']
                    df['stoch_d'] = stoch['STOCHd_14_3_3']

                latest_day = df.tail(1).reset_index()


                for i, row in latest_day.iterrows():
                    if pd.isna(row['rsi_14']): 
                        continue

                    sql = """
                        INSERT INTO calculated_indicators (
                            record_date, code, 
                            rsi_14, sma_50, sma_200, 
                            macd_line, macd_signal, macd_hist, 
                            bb_upper, bb_lower, bb_middle, 
                            atr_14, stoch_k, stoch_d
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (record_date, code) DO UPDATE SET
                            rsi_14 = EXCLUDED.rsi_14,
                            sma_50 = EXCLUDED.sma_50,
                            sma_200 = EXCLUDED.sma_200,
                            macd_line = EXCLUDED.macd_line,
                            macd_signal = EXCLUDED.macd_signal,
                            macd_hist = EXCLUDED.macd_hist,
                            bb_upper = EXCLUDED.bb_upper,
                            bb_lower = EXCLUDED.bb_lower,
                            bb_middle = EXCLUDED.bb_middle,
                            atr_14 = EXCLUDED.atr_14,
                            stoch_k = EXCLUDED.stoch_k,
                            stoch_d = EXCLUDED.stoch_d;
                    """
                    
                    cursor.execute(sql, (
                        row['record_date'], code,
                        row['rsi_14'], row['sma_50'], row['sma_200'],
                        row.get('macd_line'), row.get('macd_signal'), row.get('macd_hist'),
                        row.get('bb_upper'), row.get('bb_lower'), row.get('bb_middle'),
                        row['atr_14'], row.get('stoch_k'), row.get('stoch_d')
                    ))
                
                print(f" {code}: Indicators updated.") 
                
            conn.commit()

    except Exception as e:
        print("Couldn't connect to the database")
        print(f"\n Error:{e}")

    print("All Indicators updated.") 



if __name__ == "__main__":
    calculate_and_store()

