# Project_data_stream

## 1. Clone the repository 

```git clone git@github.com:lhteillet/Stream_Project.git```

## 2. Start the project :
# 2.1 On Windows: 

# Run Zookeeper

```bash .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties ```

# Run kafka server

```bash .\bin\windows\kafka-server-start.bat .\config\server.properties ```

# Create topics 

```bash .\bin\windows\kafka-topics.bat --create --topic full_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 ```

```bash .\bin\windows\kafka-topics.bat --create --topic result --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 ```

# Display topics

```bash .\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092 ```

# 2.2 On Linux

# Run Zookeeper

```bash ./bin/zookeeper-server-start.sh ./config/zookeeper.properties ```

# Run kafka server

```bash ./bin/kafka-server-start.sh ./config/server.properties ```

# Create topics 

```bash ./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic full_data ```

```bash ./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic result ```

## 3. Launching scripts

# Application

We must first launch the application to access the results. The following Python command opens a local website for visualization:

```bash streamlit run dashboard.py ```

# Batch Regression:

To obtain the results of the Batch model (static data):

```bash python batch_regression.py ```

# Start the data stream

```bash python data_collection.py ```

# Incremental

The results of the incremental model:

```bash python batch_regression.py ```

# Online Regression

```bash python adaptive_regression.py ```

## Collaborators

Jaishan BURTON-ELMO
Guillaume MARIN-BERTIN
