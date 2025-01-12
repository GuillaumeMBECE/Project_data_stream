import yfinance as yf
import os

# List of company tickers
tickers = {
    "AXA": "CS.PA",         # France
    "Google": "GOOGL",      # États-Unis
    "Toyota": "TM",         # Japon
    "Alibaba": "BABA",      # Chine
    "HSBC": "HSBC"          # Royaume-Uni
}

def fetch_data(ticker, start_date, end_date):
    """
    Fetch stock data for a given ticker between start_date and end_date.
    """
    print(f"Fetching data for {ticker}...")
    data = yf.download(ticker, start=start_date, end=end_date, interval="1d")
    return data

start_date = "2023-01-01"
end_date = "2024-12-31"

output_dir = "data"
os.makedirs(output_dir, exist_ok=True)

all_data = {}

# Fetch and save data
for name, ticker in tickers.items():
    data = fetch_data(ticker, start_date, end_date)
    all_data[name] = data
    print(f"Data for {name} ({ticker}) collected with {len(data)} rows.")

    # Save data to a CSV file in the 'data' directory
    if not data.empty:
        filename = os.path.join(output_dir, f"{name}_stock_data.csv")
        data.to_csv(filename)
        print(f"Data for {name} saved to {filename}.")
    else:
        print(f"No data found for {name}.")
