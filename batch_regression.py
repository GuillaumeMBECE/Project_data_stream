import os
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
import math
from datetime import datetime

data_dir = "data"

for file_name in os.listdir(data_dir):
    if file_name.endswith("_stock_data.csv"):
        company_name = file_name.split("_")[0] 
        file_path = os.path.join(data_dir, file_name)
        
        print(f"\n--- Chargement des données pour {company_name} ---")
        df = pd.read_csv(file_path)
        
        df['DateTime'] = pd.to_datetime(df['DateTime'])
        
        X = df[['Open', 'High', 'Low', 'Volume']]
        y = df['Close']
        dates = df['DateTime']
        
        X_train, X_test, y_train, y_test, dates_train, dates_test = train_test_split(
            X, y, dates, test_size=0.2, random_state=42
        )
        
        model = LinearRegression()
        model.fit(X_train, y_train)
        
        y_pred = model.predict(X_test)
        
        mae = abs(y_test - y_pred).mean()
        mse = mean_squared_error(y_test, y_pred)
        rmse = math.sqrt(mse)
        r2 = r2_score(y_test, y_pred)
        
        print(f"Entreprise: {company_name}")
        print(f"Métriques pour {company_name}:")
        print(f"  - MAE: {mae:.2f}")
        print(f"  - MSE: {mse:.2f}")
        print(f"  - RMSE: {rmse:.2f}")
        print(f"  - R²: {r2:.4f}")
        
        results = pd.DataFrame({
            'Date': dates_test.dt.strftime('%Y-%m-%d %H:%M:%S'),
            'True Close': y_test.values,
            'Predicted Close': y_pred,
            'Error': abs(y_test.values - y_pred)
        }).sort_values(by='Date')
        
        print("\n--- Résultats pour toutes les dates ---")
        for _, row in results.iterrows():
            print(f"Date: {row['Date']}, Entreprise: {company_name}, "
                  f"True Close: {row['True Close']:.2f}, "
                  f"Predicted Close: {row['Predicted Close']:.2f}, "
                  f"Erreur: {row['Error']:.2f}")
        
        print('-' * 50)
