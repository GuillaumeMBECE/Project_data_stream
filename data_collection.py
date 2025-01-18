import yfinance as yf
import json
import time
from kafka import KafkaProducer
from pytz import timezone
from datetime import datetime, timedelta
import pandas as pd
import os

tickers = {
    "AXA": "CS.PA",         # France
    "Google": "GOOGL",      # États-Unis
    "Toyota": "TM",         # Japon
    "Alibaba": "BABA",      # Chine
    "HSBC": "HSBC"          # Royaume-Uni
}

tickers_inverse = {v: k for k, v in tickers.items()}

def fetch_data(ticker, start_date, end_date):
    print(f"Fetching data for {ticker}...")
    data = yf.download(ticker, start=start_date, end=end_date, interval="1h")
    
    if data.empty:
        print(f"No data found for {ticker}.")
        return pd.DataFrame()
    
    data = data[['Open', 'High', 'Low', 'Close', 'Volume']].reset_index()
    
    ticker_data = yf.Ticker(ticker)
    info = ticker_data.info
    
    market_cap = info.get('marketCap')
    pe_ratio = info.get('trailingPE')
    dividend_yield = info.get('dividendYield')
    
    data['market_cap'] = market_cap
    data['pe_ratio'] = pe_ratio
    data['dividend_yield'] = dividend_yield
    
    return data

start_date = "2024-09-01"
end_date = "2025-01-31"

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')  
)

all_data = {}

output_dir = "data"
os.makedirs(output_dir, exist_ok=True)


for name, ticker in tickers.items():
    data = fetch_data(ticker, start_date, end_date)
    print(f"Data for {name} ({ticker}) collected with {len(data)} rows.")
    
    if not data.empty:
        data.columns = ['DateTime', 'Open', 'High', 'Low', 'Close', 'Volume', 'market_cap', 'pe_ratio', 'dividend_yield']
        all_data[ticker] = data
        csv_path = os.path.join(output_dir, f"{name}_stock_data.csv")
        data.to_csv(csv_path, index=False)
        print(f"Données pour {name} enregistrées dans le fichier : {csv_path}")

    else:
        print(f"No data found for {name}.")

all_hourly_data = []

for ticker, data in all_data.items():
    for _, row in data.iterrows():
        all_hourly_data.append({
            "DateTime": row['DateTime'],  
            "Open": float(row['Open']),
            "High": float(row['High']),
            "Low": float(row['Low']),
            "Close": float(row['Close']),
            "Volume": int(row['Volume']),
            "market_cap": int(row['market_cap']),
            "pe_ratio": float(row['pe_ratio']),
            "dividend_yield": float(row['dividend_yield']),
            "name": tickers_inverse[ticker],
            "ticker": ticker
        })

all_hourly_data = sorted(all_hourly_data, key=lambda x: x['DateTime'])

for message in all_hourly_data:
    message['DateTime'] = message['DateTime'].strftime("%Y-%m-%d %H:%M:%S")
    
    producer.send('full_data', message)
    print(f"Sent data to Kafka: {message}\n")
    
    time.sleep(0.01)

# producer.close()
