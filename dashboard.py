import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from kafka import KafkaConsumer
import json

st.set_page_config(layout="wide")
st.title("Dashboard - Prédictions pour HSBC")

broker = "localhost:9092"
topic = "result"
group = "hsbc-visualization-group"

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=broker,
    group_id=group,
    auto_offset_reset='latest',  
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

graph_container = st.empty()
data_table = st.empty()

data = pd.DataFrame(columns=["DateTime", "Prediction", "Actual", "Model"])

online_received = False
batch_received = False

try:
    for message in consumer:
        result = message.value
        print(result)

        if result["Company"] == "AXA":
            new_row = {
                "DateTime": pd.to_datetime(result["DateTime"]),  
                "Prediction": result["Prediction"],
                "Actual": result["Actual"],
                "Model": result["Model"]  
            }
            data = pd.concat([data, pd.DataFrame([new_row])], ignore_index=True)

            data_table.write(data)

            if "online" in data['Model'].values:
                online_received = True
            if "batch" in data['Model'].values:
                batch_received = True

            fig = go.Figure()

            if online_received:
                fig.add_trace(go.Scatter(
                    x=data[data['Model'] == 'online']['DateTime'],
                    y=data[data['Model'] == 'online']['Prediction'],
                    mode='lines',
                    name='Prediction (Online)',
                    line=dict(color='blue')  
                ))

            if batch_received:
                fig.add_trace(go.Scatter(
                    x=data[data['Model'] == 'batch']['DateTime'],
                    y=data[data['Model'] == 'batch']['Prediction'],
                    mode='lines',
                    name='Prediction (Batch)',
                    line=dict(color='green')  
                ))

            if online_received:
                fig.add_trace(go.Scatter(
                    x=data[data['Model'] == 'online']['DateTime'],
                    y=data[data['Model'] == 'online']['Actual'],
                    mode='lines',
                    name='Actual (Online)',
                    line=dict(color='orange')  
                ))

            fig.update_layout(
                title="Prédiction vs Réel pour HSBC",
                xaxis_title="Date",
                yaxis_title="Prix de Clôture",
                legend_title="Modèles",
                template="plotly_white"
            )

            graph_container.plotly_chart(fig, use_container_width=True)

except KeyboardInterrupt:
    st.write("Arrêt du consommateur Kafka.")
    consumer.close()

finally:
    if consumer:
        consumer.close()
        st.write("Le consommateur Kafka a été correctement fermé.")
