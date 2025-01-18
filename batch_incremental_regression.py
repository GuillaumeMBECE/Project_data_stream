import os
import pandas as pd
import math
from river import compose, linear_model, metrics, preprocessing
from kafka import KafkaConsumer, KafkaProducer
import json

data_dir = "data"

consumer = KafkaConsumer(
    'full_data', 
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

model = compose.Pipeline(
    ('scaler', preprocessing.StandardScaler()),  
    ('regressor', linear_model.LinearRegression()) 
)

mae_metric = metrics.MAE()
mse_metric = metrics.MSE()

# Train sur données en csv (juillet - mi-novembre)
print("\n--- Entraînement batch sur les données historiques ---")
for file_name in os.listdir(data_dir):
    if file_name.endswith("_stock_data.csv"):
        company_name = file_name.split("_")[0]
        file_path = os.path.join(data_dir, file_name)
        
        print(f"\nTraitement des données pour {company_name}...")
        df = pd.read_csv(file_path)
        df['DateTime'] = pd.to_datetime(df['DateTime'])

        training_data = df[df['DateTime'] < "2024-11-15"]

        for _, row in training_data.iterrows():
            x = {
                "Open": row['Open'],
                "High": row['High'],
                "Low": row['Low'],
                "Volume": row['Volume']
            }
            y = row['Close']

            model.learn_one(x, y)

print("Entraînement batch terminé.")

# Prédictions sur données online à partir du 15 novembre
print("\n--- Prédictions adaptatives en ligne ---")
for message in consumer:
    data = message.value
    company_name = data['name']
    date = data['DateTime']

    x = {
        "Open": data['Open'],
        "High": data['High'],
        "Low": data['Low'],
        "Volume": data['Volume']
    }
    y = data['Close']

    y_pred = model.predict_one(x)
    model.learn_one(x, y)

    mae_metric.update(y, y_pred)
    mse_metric.update(y, y_pred)
    rmse = math.sqrt(mse_metric.get())

    print(f"Entreprise: {company_name}")
    print(f"Date: {date}")
    print(f"Prédiction: {y_pred:.2f}, Cible réelle: {y:.2f}")
    print(f"Métriques pour {company_name}:")
    print(f"  - MAE: {mae_metric.get():.2f}")
    print(f"  - MSE: {mse_metric.get():.2f}")
    print(f"  - RMSE: {rmse:.2f}")
    print('-' * 50)

    result = {
        "Model": "batch_incremental",
        "DateTime": date,
        "Company": company_name,
        "Prediction": float(y_pred),
        "Actual": float(y),
        "MSE": mse_metric.get(),
        "MAE": mae_metric.get(),
        "RMSE": rmse
    }
    producer.send('result', result)
