# 📈 Market Data Loader

This Python script fetches historical financial market data using [Yahoo Finance](https://finance.yahoo.com/) via the `yfinance` package and stores it in a MySQL database for further analysis or backtesting.

## 🚀 Features

- Fetches minute-level (`1m`) market data for multiple assets
- Automatically inserts into a normalized MySQL table with deduplication (`INSERT IGNORE`)
- Timestamped and indexed for efficient queries
- Supports timezone-aware `loaded_at` fields (CET)
- Modular and easily extensible for more tickers or intervals

## 🧱 Table Schema

```sql
CREATE TABLE market_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    interval_type VARCHAR(10) NOT NULL,
    timestamp DATETIME NOT NULL,
    open DECIMAL(12, 6),
    high DECIMAL(12, 6),
    low DECIMAL(12, 6),
    close DECIMAL(12, 6),
    volume DECIMAL(20, 2),
    loaded_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ticker, interval_type, timestamp),
    INDEX idx_ticker_interval_time (ticker, interval_type, timestamp)
);
```


## ⚙️ Setup Instructions

### 1. Clone the repository
```bash
git clone https://github.com/your-username/market-data-loader.git
cd market-data-loader
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure environment variables
Create a .env file in the root directory and add your database configuration:
```bash
DB_HOST=your_mysql_host
DB_PORT=3306
DB_USER=your_mysql_user
DB_PASSWORD=your_mysql_password
DB_NAME=your_database_name
```
### 4. Run the script
```bash
python yfinance_to_mysql.py
```

## 📝 Example Assets Loaded
- EUR/USD Forex (EURUSD=X)
- iShares Core S&P 500 ETF (SXR8.DE)
- S&P 500 Index (^GSPC)

All data is fetched using:
- interval = '1m'
- period = '8d' (last 8 days)

## 📒 Notes
- If you want to fetch different intervals or timeframes, you can modify the interval and period in the assets list inside the script. Common interval values include '5m', '1h', '1d', etc.
- For a longer historical data range, update the period to options like '1y', '5y', etc.
- Be aware of Yahoo Finance rate limits and potential inconsistencies for certain tickers or markets, especially when querying frequently.
