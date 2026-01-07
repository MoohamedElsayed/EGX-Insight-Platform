import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv 

load_dotenv()

conn_string = os.getenv("DATABASE_URL")


SCHEMA_FILE = os.path.join("sql", "schema.sql")
CSV_FILE = os.path.join("data", "stocks.csv")

def initialize_database():
    """
    1. Connects to the database.
    2. Runs the SQL schema to create tables.
    3. Populates the 'stocks' table from the CSV file.
    """
    
    if not conn_string:
        print("conn_string is missing. Check your environment variables")
        return

    conn = None
    try:

        print("Connecting to database...")

        with psycopg2.connect(conn_string) as conn:
            print("Connection established")

            with conn.cursor() as cur:

                # Initiating the database and populating the tables
                print(f"Reading schema from {SCHEMA_FILE}...")

                try:
                    with open(SCHEMA_FILE, 'r') as f:
                        schema_sql = f.read()
                        cur.execute(schema_sql)
                        print("Schema executed successfully (Tables created).")

                except FileNotFoundError:
                    print(f"Error: Could not find schema file at {SCHEMA_FILE}")
                    return


                if os.path.exists(CSV_FILE):
                    print(f"Reading stocks from {CSV_FILE}")

                    df = pd.read_csv(CSV_FILE)
            
                    df.columns = [c.strip() for c in df.columns]

                    if 'code' not in df.columns or 'company_name' not in df.columns:
                        raise ValueError("CSV must contain 'code' and 'company_name' columns")

                    print(f"   Found {len(df)} stocks. Inserting...")
            
                    inserted_count = 0
                    for _, row in df.iterrows():

                        cur.execute("""
                            INSERT INTO stocks (code, company_name)
                            VALUES (%s, %s)
                            ON CONFLICT (code) DO NOTHING;
                        """, (row['code'], row['company_name']))
                        inserted_count += 1
            
                    print(f"Finished populating the stocks table. Processed {inserted_count} rows.")

                else:
                    print(f"Warning: CSV file not found at {CSV_FILE}. Tables created but empty.")

            conn.commit()
            print("\nSUCCESS: Database initialization complete!")

    except Exception as e:
        print(f"\nERROR: {e}")
        if conn:
            conn.rollback()
            print("Transaction rolled back. No changes were saved.")
    
    finally:
        if conn:
            cur.close()
            conn.close()
            print("Connection closed.")

if __name__ == "__main__":
    print("--- Starting Database Initialization ---")
    initialize_database()