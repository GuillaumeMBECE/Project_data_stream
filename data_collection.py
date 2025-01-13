import yfinance as yf
import json
import time
from kafka import KafkaProducer
from pytz import timezone
from datetime import datetime
import pandas as pd 

tickers = {
    "AXA": "CS.PA",         # France
    "Google": "GOOGL",      # États-Unis
    "Toyota": "TM",         # Japon
    "Alibaba": "BABA",      # Chine
    "HSBC": "HSBC"          # Royaume-Uni
}

tickers_inverse = {v: k for k, v in tickers.items()}

france_tz = timezone('Europe/Paris')

def fetch_data(ticker, start_date, end_date):
    print(f"Fetching data for {ticker}...")
    
    # Récupérer les données boursières tout les jours
    data = yf.download(ticker, start=start_date, end=end_date, interval="1d")
    
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

start_date = "2024-07-15"
end_date = "2025-01-31"

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')  
)

all_data = {}

for name, ticker in tickers.items():
    data = fetch_data(ticker, start_date, end_date)
    print(f"Data for {name} ({ticker}) collected with {len(data)} rows.")
    
    if not data.empty:
        data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'market_cap', 'pe_ratio', 'dividend_yield']
        all_data[ticker] = data
    else:
        print(f"No data found for {name}.")

for current_day in pd.date_range(start=start_date, end=end_date, freq='D'):  
    daily_data = []
    
    for ticker, data in all_data.items():
        if current_day in data['Date'].values:
            day_data = data[data['Date'] == current_day].iloc[0]
            
            daily_data.append({
                "Date": current_day.strftime("%Y-%m-%d"),  
                "Open": float(day_data['Open']), 
                "High": float(day_data['High']),  
                "Low": float(day_data['Low']),    
                "Close": float(day_data['Close']), 
                "Volume": int(day_data['Volume']), 
                "market_cap": int(day_data['market_cap']),
                "pe_ratio": float(day_data['pe_ratio']),
                "dividend_yield": float(day_data['dividend_yield']),
                "name": tickers_inverse[ticker],  
                "ticker": ticker
            })
    
    if daily_data:
        producer.send('full_data', {"date": current_day.strftime("%Y-%m-%d"), "data": daily_data})
        print(f"Sent daily data to Kafka for {current_day.strftime('%Y-%m-%d')}: {daily_data}")
    
    time.sleep(0.5) 

# producer.close()
