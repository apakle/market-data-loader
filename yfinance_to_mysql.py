import yfinance as yf
from datetime import datetime
import pytz
import pandas as pd
import pymysql
from dotenv import load_dotenv
import os
import logging
import time
import sys

# === Load environment variables ===
# Load .env only if not running in GitHub Actions
if not os.getenv("GITHUB_ACTIONS"):  # Checks if not running in GitHub Actions
    load_dotenv()  # Load .env file locally

# === MySQL config ===
timeout = 10
db_config = {
    'charset': "utf8mb4",
    'connect_timeout': timeout,
    'cursorclass': pymysql.cursors.DictCursor,
    'db': os.getenv('DB_NAME'),
    'host': os.getenv('DB_HOST'),
    'password': os.getenv('DB_PASSWORD'),
    'port': int(os.getenv('DB_PORT')),
    'user': os.getenv('DB_USER'),
    'write_timeout': timeout,
    'read_timeout': timeout
}

# === Set up logging to console ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# === Tickers and Intervals ===
interval = '1m'
period = '8d'
assets = [
    {'ticker': 'EURUSD=X', 'alias': 'EURUSD', 'interval': interval, 'period': period},
    {'ticker': 'SXR8.DE', 'alias': 'iShares Core S&P 500 ETF', 'interval': interval, 'period': period},
    {'ticker': '^GSPC', 'alias': 'S&P 500', 'interval': interval, 'period': period},
    {'ticker': 'AUDUSD=X', 'alias': 'AUDUSD', 'interval': interval, 'period': period},
    {'ticker': 'NZDUSD=X', 'alias': 'NZDUSD', 'interval': interval, 'period': period},
    {'ticker': 'GBPUSD=X', 'alias': 'GBPUSD', 'interval': interval, 'period': period},
    {'ticker': 'GC=F', 'alias': 'Gold Futures', 'interval': interval, 'period': period},
    {'ticker': 'BTC-USD', 'alias': 'Bitcoin', 'interval': interval, 'period': period},
    {'ticker': 'ETH-USD', 'alias': 'Ethereum', 'interval': interval, 'period': period},
    {'ticker': '^GDAXI', 'alias': 'German DAX', 'interval': interval, 'period': period},
    {'ticker': '^DJI', 'alias': 'Dow Jones Industrial Average', 'interval': interval, 'period': period},
    {'ticker': '^NDX', 'alias': 'Nasdaq-100', 'interval': interval, 'period': period}    
]

def fetch_data(ticker, period, interval, retries=3):
    for attempt in range(retries):
        try:
            logging.info(f"Fetching {ticker} with {interval} interval for {period} (Attempt {attempt + 1})")
            df = yf.download(ticker, interval=interval, period=period, progress=False)
            if df.empty:
                logging.warning(f"No data returned for {ticker} on attempt {attempt + 1}")
                continue
            df.columns = [col[0] for col in df.columns]  # Flatten MultiIndex
            df = df.reset_index()
            df.rename(columns={df.columns[0]: "Datetime"}, inplace=True)
            df["Datetime"] = pd.to_datetime(df["Datetime"])
            return df
        except Exception as e:
            logging.warning(f"Attempt {attempt + 1} failed for {ticker}: {e}")
            time.sleep(5)
    logging.error(f"❌ All {retries} attempts failed for {ticker}")
    return pd.DataFrame()

def connect_with_retry(retries=3, delay=10):
    for attempt in range(retries):
        try:
            logging.info(f"Connecting to MySQL (attempt {attempt+1}/{retries})...")
            conn = pymysql.connect(**db_config)
            logging.info("✅ Connected to MySQL database.")
            return conn
        except Exception as e:
            logging.warning(f"DB connection failed on attempt {attempt+1}: {e}")
            time.sleep(delay)
    logging.error("❌ Could not connect to DB after multiple attempts.")
    raise Exception("DB connection failed")

def insert_data(cursor, data, ticker, interval_str):
    cet = pytz.timezone('CET')
    now = datetime.now(cet)
    inserted_rows = 0

    for _, row in data.iterrows():
        timestamp = row['Datetime']

        sql = """
            INSERT IGNORE INTO market_data 
            (ticker, interval_type, timestamp, open, high, low, close, volume, loaded_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = (
            ticker,
            interval_str,
            timestamp,
            row['Open'],
            row['High'],
            row['Low'],
            row['Close'],
            row.get('Volume', 0),
            now
        )
        try:
            cursor.execute(sql, values)
            inserted_rows += cursor.rowcount
        except Exception as e:
            logging.error(f"Error inserting data for {ticker} at {timestamp}: {e}")
    
    return inserted_rows

def main():
    try:
        conn = connect_with_retry()
        cursor = conn.cursor()

        total_inserted = 0
        success_assets = 0

        for asset in assets:
            df = fetch_data(asset['ticker'], asset['period'], asset['interval'])
            if not df.empty:
                inserted = insert_data(cursor, df, asset['ticker'], asset['interval'])
                total_inserted += inserted
                success_assets += 1
                logging.info(f"✅ Inserted {inserted} rows for {asset['ticker']}.")
            else:
                logging.warning(f"⚠️ Skipped {asset['ticker']} — no data.")

        conn.commit()
        cursor.close()
        conn.close()

        if success_assets > 0:
            logging.info(f"✅ Finished: Inserted a total of {total_inserted} rows for {success_assets}/{len(assets)} assets.")
        else:
            logging.warning("⚠️ Finished: No data was inserted for any asset.")
    
    except Exception as e:
        logging.error(f"❌ Unexpected error in main(): {e}")
        sys.exit(1)   # 🚨 This makes the GitHub Action fail

if __name__ == "__main__":
    main()
