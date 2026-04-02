from curl_cffi import requests
from bs4 import BeautifulSoup
import json
import time
import pandas as pd
import random


def fetch_financial_data():


    with open('data/stocks_link.json', 'r') as f:
        stocks = json.load(f)

        print(f"Loaded {len(stocks)} stocks to scrape.")

        all_data = []

        session = requests.Session(impersonate="chrome120")

        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1"
        }

        for symbol, url in stocks.items():

            print(f"Scraping {symbol} financials...", end=" ")


            try:
                response = session.get(url, headers=headers)

                if response.status_code != 200:
                    print(f"Failed to retrieve data for {symbol}. Status code: {response.status_code}")
                    continue

                soup = BeautifulSoup(response.content, 'html.parser')

                stock_data = {"Symbol": symbol, "Source_URL": url}

                rows = soup.find_all("div", class_="justify-between")

                for row in rows:

                    label = row.find("dt")
                    value = row.find("dd")
                    if label and value:
                        clean_label = label.get_text(strip=True)
                        clean_value = value.get_text(strip=True)
                        stock_data[clean_label] = clean_value

                all_data.append(stock_data)
                print(f"Done scraping {symbol} financials.")

            

            except Exception as e:
                print(f"Error: {e}")


            sleep_time = random.uniform(3, 10) 
            time.sleep(sleep_time)

        df = pd.DataFrame(all_data)
        df.to_csv('data/stocks_quartely_financials.csv', index=False)
        print("\nScraping complete! Saved to 'data/stocks_quartely_financials.csv'")


if __name__ == "__main__":
    fetch_financial_data()

