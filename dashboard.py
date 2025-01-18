import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from kafka import KafkaConsumer
import json

st.set_page_config(layout="wide")
st.title("Dashboard - Prédictions pour AXA, HSBC, Toyota, Alibaba et Google")

broker = "localhost:9092"
topic = "result"
group = "axa-hsbc-visualization-group"

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=broker,
    group_id=group,
    auto_offset_reset='latest',  
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

data = {
    'AXA': pd.DataFrame(columns=["DateTime", "Prediction", "Actual", "Model", "Company", "RMSE", "MSE", "MAE"]),
    'HSBC': pd.DataFrame(columns=["DateTime", "Prediction", "Actual", "Model", "Company", "RMSE", "MSE", "MAE"]),
    'Toyota': pd.DataFrame(columns=["DateTime", "Prediction", "Actual", "Model", "Company", "RMSE", "MSE", "MAE"]),
    'Alibaba': pd.DataFrame(columns=["DateTime", "Prediction", "Actual", "Model", "Company", "RMSE", "MSE", "MAE"]),
    'Google': pd.DataFrame(columns=["DateTime", "Prediction", "Actual", "Model", "Company", "RMSE", "MSE", "MAE"])
}

metrics = {
    'AXA': {'RMSE_batch': None, 'RMSE_online': None, 'RMSE_batch_incremental': None,
            'MSE_batch': None, 'MSE_online': None, 'MSE_batch_incremental': None,
            'MAE_batch': None, 'MAE_online': None, 'MAE_batch_incremental': None},
    'HSBC': {'RMSE_batch': None, 'RMSE_online': None, 'RMSE_batch_incremental': None,
             'MSE_batch': None, 'MSE_online': None, 'MSE_batch_incremental': None,
             'MAE_batch': None, 'MAE_online': None, 'MAE_batch_incremental': None},
    'Toyota': {'RMSE_batch': None, 'RMSE_online': None, 'RMSE_batch_incremental': None,
               'MSE_batch': None, 'MSE_online': None, 'MSE_batch_incremental': None,
               'MAE_batch': None, 'MAE_online': None, 'MAE_batch_incremental': None},
    'Alibaba': {'RMSE_batch': None, 'RMSE_online': None, 'RMSE_batch_incremental': None,
                'MSE_batch': None, 'MSE_online': None, 'MSE_batch_incremental': None,
                'MAE_batch': None, 'MAE_online': None, 'MAE_batch_incremental': None},
    'Google': {'RMSE_batch': None, 'RMSE_online': None, 'RMSE_batch_incremental': None,
               'MSE_batch': None, 'MSE_online': None, 'MSE_batch_incremental': None,
               'MAE_batch': None, 'MAE_online': None, 'MAE_batch_incremental': None}
}


graph_containers = {
    'AXA': st.empty(),
    'HSBC': st.empty(),
    'Toyota': st.empty(),
    'Alibaba': st.empty(),
    'Google': st.empty()
}

metrics_tables = {
    'AXA': st.empty(),
    'HSBC': st.empty(),
    'Toyota': st.empty(),
    'Alibaba': st.empty(),
    'Google': st.empty()
}

try:
    for message in consumer:
        result = message.value
        print(result)

        company_name = result["Company"]
        if company_name in data:
            model_type = result["Model"]
            rmse = result["RMSE"]
            mse = result["MSE"]
            mae = result["MAE"]

            new_row = {
                "DateTime": pd.to_datetime(result["DateTime"]),
                "Prediction": result["Prediction"],
                "Actual": result["Actual"],
                "Model": model_type,
                "Company": company_name,
                "RMSE": rmse,
                "MSE": mse,
                "MAE": mae
            }

            data[company_name] = pd.concat([data[company_name], pd.DataFrame([new_row])], ignore_index=True)

            data[company_name] = data[company_name].tail(1000)

            if model_type == "batch":
                metrics[company_name]['RMSE_batch'] = rmse
                metrics[company_name]['MSE_batch'] = mse
                metrics[company_name]['MAE_batch'] = mae
            elif model_type == "online":
                metrics[company_name]['RMSE_online'] = rmse
                metrics[company_name]['MSE_online'] = mse
                metrics[company_name]['MAE_online'] = mae
            elif model_type == "batch_incremental":
                metrics[company_name]['RMSE_batch_incremental'] = rmse
                metrics[company_name]['MSE_batch_incremental'] = mse
                metrics[company_name]['MAE_batch_incremental'] = mae


            metrics_with_company_name = metrics[company_name].copy()
            metrics_with_company_name['Company'] = company_name  

            metrics_tables[company_name].write(pd.DataFrame([metrics_with_company_name]))
                
            fig = go.Figure()

            if "online" in data[company_name]['Model'].values:
                fig.add_trace(go.Scatter(
                    x=data[company_name][data[company_name]['Model'] == 'online']['DateTime'],
                    y=data[company_name][data[company_name]['Model'] == 'online']['Prediction'],
                    mode='lines',
                    name='Prediction (Online)',
                    line=dict(color='blue')  
                ))
            
            if "batch_incremental" in data[company_name]['Model'].values:
                fig.add_trace(go.Scatter(
                    x=data[company_name][data[company_name]['Model'] == 'batch_incremental']['DateTime'],
                    y=data[company_name][data[company_name]['Model'] == 'batch_incremental']['Prediction'],
                    mode='lines',
                    name='Prediction (Batch Incremental)',
                    line=dict(color='red')  
                ))


            if "batch" in data[company_name]['Model'].values:
                fig.add_trace(go.Scatter(
                    x=data[company_name][data[company_name]['Model'] == 'batch']['DateTime'],
                    y=data[company_name][data[company_name]['Model'] == 'batch']['Prediction'],
                    mode='lines',
                    name='Prediction (Batch)',
                    line=dict(color='green')  
                ))

            if "online" in data[company_name]['Model'].values:
                fig.add_trace(go.Scatter(
                    x=data[company_name][data[company_name]['Model'] == 'online']['DateTime'],
                    y=data[company_name][data[company_name]['Model'] == 'online']['Actual'],
                    mode='lines',
                    name='Actual (Online)',
                    line=dict(color='orange')  
                ))

            fig.update_layout(
                title=f"Prédiction vs Réel pour {company_name}",
                xaxis_title="Date",
                yaxis_title="Prix de Clôture",
                legend_title="Modèles",
                template="plotly_white"
            )

            graph_containers[company_name].plotly_chart(fig, use_container_width=True)

except KeyboardInterrupt:
    st.write("Arrêt du consommateur Kafka.")
    consumer.close()

finally:
    if consumer:
        consumer.close()
        st.write("Le consommateur Kafka a été correctement fermé.")
