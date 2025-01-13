from kafka import KafkaConsumer
import pandas as pd
import json
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score

consumer = KafkaConsumer(
    'full_data',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

data = []

for message in consumer:
    record = message.value
    for item in record['data']:
        data.append(item)
    if len(data) > 100:  
        break

df = pd.DataFrame(data)
print("Données reçues :")
print(df.head())

results = {}

for company in df['name'].unique():
    print(f"\n--- Modèle pour {company} ---")
    
    company_data = df[df['name'] == company]
    
    X = company_data[['Open', 'High', 'Low', 'Volume']]
    y = company_data['Close']
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    model = LinearRegression()
    model.fit(X_train, y_train)
    
    y_pred = model.predict(X_test)
    
    rmse = mean_squared_error(y_test, y_pred)** 0.5
    r2 = r2_score(y_test, y_pred)
    
    print(f"RMSE: {rmse}")
    print(f"R²: {r2}")
    print(f"Coefficients: {model.coef_}")
    print(f"Intercept: {model.intercept_}")
    
    # Stocker les résultats réels et prédits pour affichage
    results[company] = pd.DataFrame({
        'Real Close': y_test.values,
        'Predicted Close': y_pred,
        'Error': abs(y_test.values - y_pred)
    })

# Affichage des valeurs réelles et prédites pour chaque entreprise
for company, result in results.items():
    print(f"\n--- Résultats pour {company} ---")
    print(result.head())  
