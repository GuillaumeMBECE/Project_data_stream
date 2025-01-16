import json
from kafka import KafkaConsumer
from river import linear_model, metrics
from sklearn.preprocessing import StandardScaler
import math

consumer = KafkaConsumer(
    'full_data', 
    bootstrap_servers='localhost:9092',  
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))  
)

models = {}

mae_metric = metrics.MAE()
mse_metric = metrics.MSE()

scaler = StandardScaler()

def reset_model():
    return linear_model.LinearRegression()  

def validate_and_train_model(model, x, y):
    y_pred = model.predict_one(x)
    model.learn_one(x, y)  
    return y_pred

def get_model_for_company(company_name):
    if company_name not in models:
        models[company_name] = reset_model() 
    return models[company_name]

print("Waiting for messages...")

for message in consumer:
    data = message.value
    ticker = data['ticker']
    company_name = data['name']

    x = {
        "Open": data['Open'],
        "High": data['High'],
        "Low": data['Low'],
        "Volume": data['Volume'],
        "market_cap": data['market_cap'],
        "pe_ratio": data['pe_ratio'],
        "dividend_yield": data['dividend_yield']
    }

    y = data['Close']

    scaled_x = scaler.fit_transform([list(x.values())])[0]

    model = get_model_for_company(company_name)

    y_pred = validate_and_train_model(model, dict(zip(x.keys(), scaled_x)), y)

    mae_metric.update(y, y_pred)
    mse_metric.update(y, y_pred)

    rmse = math.sqrt(mse_metric.get())  

    print(f"Entreprise: {company_name} ({ticker})")
    print(f"Prédiction: {y_pred}, Cible réelle: {y}")
    print(f"Métriques pour {company_name}:")
    print(f"  - MAE: {mae_metric.get()}")
    print(f"  - MSE: {mse_metric.get()}")
    print(f"  - RMSE: {rmse}")
    print('-' * 50)
