import json
from kafka import KafkaConsumer, KafkaProducer
from river import linear_model, metrics
from sklearn.preprocessing import StandardScaler
import math
from datetime import datetime

consumer = KafkaConsumer(
    'full_data', 
    bootstrap_servers='localhost:9092',  
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))  
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  
)

models = {}
metrics_per_company = {}  
scaler = StandardScaler()

start_date = datetime(2024, 10, 30)

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

def get_metrics_for_company(company_name):
    if company_name not in metrics_per_company:
        metrics_per_company[company_name] = {
            'mae': metrics.MAE(),
            'mse': metrics.MSE()
        }
    return metrics_per_company[company_name]

print("Waiting for messages...")

for message in consumer:
    data = message.value
    ticker = data['ticker']
    company_name = data['name']
    date = datetime.strptime(data['DateTime'], '%Y-%m-%d %H:%M:%S')  
    model_type = "online" 

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

    company_metrics = get_metrics_for_company(company_name)

    if date >= start_date:
        company_metrics['mae'].update(y, y_pred)
        company_metrics['mse'].update(y, y_pred)
        rmse = math.sqrt(company_metrics['mse'].get())  
    else:
        rmse = 0.0
        company_metrics['mae'] = metrics.MAE()
        company_metrics['mse'] = metrics.MSE()

    print(f"Entreprise: {company_name} ({ticker})")
    print(f"Prédiction: {y_pred}, Cible réelle: {y}")
    print(f"Métriques pour {company_name}:")
    print(f"  - MAE: {company_metrics['mae'].get()}")
    print(f"  - MSE: {company_metrics['mse'].get()}")
    print(f"  - RMSE: {rmse}")
    print('-' * 50)

    result = {
        "Model": model_type,  
        "DateTime": date.strftime('%Y-%m-%d %H:%M:%S'),  
        "Company": company_name,
        "Prediction": float(y_pred),
        "Actual": float(y),
        "RMSE": rmse,
        "MSE": float(company_metrics['mse'].get()),  
        "MAE": float(company_metrics['mae'].get())   
    }

    print(f"result {result}")

    producer.send('result', result)
