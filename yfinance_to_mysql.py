import yfinance as yf
from datetime import datetime
import pytz
import pandas as pd
import pymysql
from dotenv import load_dotenv
import os

# Load variables from .env
load_dotenv()

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

# === Tickers and Intervals ===
interval = '1m'
period = '8d'
assets = [{'ticker': 'EURUSD=X', 'alias': 'EURUSD', 'interval': interval, 'period': period}, 
          {'ticker': 'SXR8.DE', 'alias': 'iShares Core S&P 500 ETF', 'interval': interval, 'period': period},
          {'ticker': '^GSPC', 'alias': 'S&P 500', 'interval': interval, 'period': period}
         ]

def fetch_data(ticker, period, interval):
    print(f"Fetching {ticker} with {interval} interval for {period}")
    df = yf.download(ticker, interval=interval, period=period, progress=False)
    # Flatten the MultiIndex by extracting the first level (price type)
    df.columns = [col[0] for col in df.columns]  # Extract first level ('Close', 'Open', etc.)
    df.reset_index(inplace=True)
    return df

def insert_data(cursor, data, ticker, interval_str):
    cet = pytz.timezone('CET')
    now = datetime.now(cet)
    for _, row in data.iterrows():
        timestamp = row['Datetime'] if 'Datetime' in row else row['Date']
        if isinstance(timestamp, pd.Timestamp):
            timestamp = timestamp.to_pydatetime()

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
        cursor.execute(sql, values)

def main():
    conn = pymysql.connect(**db_config)
    cursor = conn.cursor()
    print("✅ Connected using PyMySQL!")

    # === loading data into mysql database ===
    for asset in assets:
        df = fetch_data(asset['ticker'], asset['period'], asset['interval'])
        insert_data(cursor, df, asset['ticker'], asset['interval'])

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Data inserted successfully.")

if __name__ == "__main__":
    main()

    