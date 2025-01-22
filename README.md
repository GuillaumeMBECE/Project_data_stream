# Project_data_stream

# I - Compile the project

  # 1. Clone the repository

```shell
git clone git@github.com:GuillaumeMBECE/Project_data_stream.git
```

## 2. Start the project :

### 2.1 On Windows:

Run Zookeeper :

```shell 
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties 
```

Run kafka server :

```shell
.\bin\windows\kafka-server-start.bat .\config\server.properties 
```


Create topics :

```shell 
.\bin\windows\kafka-topics.bat --create --topic full_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 
```

  
```shell 
.\bin\windows\kafka-topics.bat --create --topic result --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 
```

Display topics :

```shell 
.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092 
```

### 2.2 On Linux

Run Zookeeper :

```shell 
./bin/zookeeper-server-start.sh ./config/zookeeper.properties 
```

Run kafka server :

```shell 
./bin/kafka-server-start.sh ./config/server.properties 
```

Create topics :

```shell 
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic full_data 
```


```shell 
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic result 
```

## 3. Launching scripts

### 3.1 Application

We must first launch the application to access the results. The following Python command opens a local website for visualization:

```shell 
streamlit run dashboard.py 
```

### 3.2 Batch Regression:

Run the Batch model (static data):

```shell 
python batch_regression.py 
```

### 3.3 Start the data stream

```shell 
python data_collection.py 
```

### 3.4 Incremental

Run the incremental model:

```shell 
python batch_regression.py 
```

### 3.5 Online Regression

Run the online model:

```shell 
python adaptive_regression.py 
```

## 4. Visualization

Go to http://localhost:8501/ to see the dashboard in real time

# II - Goal of the project

The aim of this project is to create a real time streaming application with Kafka, collecting financial data from Yahoo Finance API. The main goal was to design a pipeline that could handle both historical data and real-time data streams, to predict stock prices of large companies like AXA, HSBC, Toyota, Alibaba, and Google.

## 3 models :

- Full batch linear regression

- Incremental linear regression

- Online linear regression with river

## Result :


| Company  | Online Model MSE | Online Model MAE | Online Model RMSE | Batch Incremental Model MSE | Batch Incremental Model MAE | Batch Incremental Model RMSE | Batch Model MSE | Batch Model MAE | Batch Model RMSE |
|----------|------------------|------------------|-------------------|----------------------------|-----------------------------|------------------------------|-----------------|-----------------|------------------|
| AXA      | 0.333            | 0.4622           | 0.5771            | 0.8239                     | 0.4097                      | 0.6789                       | 0.3016          | 0.3189          | 0.5492           |
| HSBC     | 0.8433           | 0.8043           | 0.9183            | 0.825                      | 0.331                       | 0.6806                       | 0.2305          | 0.3195          | 0.4801           |
| Toyota   | 27.5298          | 3.3289           | 5.2469            | 0.8252                     | 1.8379                      | 0.681                        | 6.3021          | 0.3197          | 2.5104           |
| Alibaba  | 12.0614          | 2.7797           | 3.4729            | 0.8251                     | 1.364                       | 0.6808                       | 3.6192          | 0.3196          | 1.9024           |
| Google   | 46.2691          | 5.1443           | 6.8021            | 0.8253                     | 2.5814                      | 0.6812                       | 12.7031         | 0.3197          | 3.5641           |




## Collaborators

Jaishan BURTON-ELMO

Guillaume MARIN-BERTIN