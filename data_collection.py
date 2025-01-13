import yfinance as yf
import json
import time
from kafka import KafkaProducer
from pytz import timezone
from datetime import datetime

# Liste des tickers des entreprises
tickers = {
    "AXA": "CS.PA",         # France
    "Google": "GOOGL",      # États-Unis
    "Toyota": "TM",         # Japon
    "Alibaba": "BABA",      # Chine
    "HSBC": "HSBC"          # Royaume-Uni
}

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

# Périodes de récupération des données
start_date = "2024-09-01"
end_date = "2025-01-31"

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')  
)

for name, ticker in tickers.items():
    data = fetch_data(ticker, start_date, end_date)
    print(f"Data for {name} ({ticker}) collected with {len(data)} rows.")
    
    if not data.empty:
        data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'market_cap', 'pe_ratio', 'dividend_yield']
        
        for current_day in data['Date']:
            daily_data = data[data['Date'] == current_day].iloc[0]
            
            # Créer un dictionnaire avec les données journalières et les informations fondamentales
            tick = {
                "Date": current_day.strftime("%Y-%m-%d"),  
                "Open": float(daily_data['Open']), 
                "High": float(daily_data['High']),  
                "Low": float(daily_data['Low']),    
                "Close": float(daily_data['Close']), 
                "Volume": int(daily_data['Volume']), 
                "market_cap": int(daily_data['market_cap']),
                "pe_ratio": float(daily_data['pe_ratio']),
                "dividend_yield": float(daily_data['dividend_yield']),
                "name": name,
                "ticker": ticker
            }
            
            tick = {str(k): v for k, v in tick.items()}
            
            producer.send('full_data', tick)
            print(f"Sent tick to Kafka for {name} ({ticker}): {tick}")
            
            time.sleep(1)  
    else:
        print(f"No data found for {name}.")

# producer.close()
