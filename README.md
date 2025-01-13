Stock Market Data Pipeline with Kafka and Cassandra.

This project is a real-time data pipeline for processing and analyzing stock market data. It uses Apache Kafka for real-time messaging and Apache Cassandra for scalable and fault-tolerant data storage. The producer generates synthetic stock market data, while the consumer processes and stores it in a Cassandra database.

Table of Contents
Features
Technologies Used
Prerequisites
Setup and Installation
Usage
Folder Structure
Future Enhancements

Features
Real-Time Data Streaming: Simulate live stock market data streams using Kafka.
Data Enrichment: Enhance stock data with synthetic features (e.g., moving averages, volatility).
Anomaly Injection: Introduce anomalies like price spikes, drops, and null values for testing.
Batch Processing: Process stock data in batches for optimized database insertion.
Fault-Tolerant Storage: Store processed data in Cassandra for scalability and reliability.
Metrics Logging: Track message processing rate and throughput.

Technologies Used
Apache Kafka: Real-time message streaming platform.
Apache Cassandra: Distributed NoSQL database for scalable storage.
Python Libraries:
kafka-python: Kafka integration.
cassandra-driver: Cassandra database client.
pandas: Data manipulation.
Other Tools: tqdm for monitoring progress, logging for metrics tracking

System Requirements:

Java 8 or higher (for Kafka).
Python 3.6+.
Cassandra installed and running.
Python Dependencies: Install the required libraries using:

bash
Copy code
pip install -r requirements.txt

The requirements.txt file includes:
cassandra-driver
kafka-python
pandas

Setup and Installation
1. Install Apache Kafka
Run the command.sh script to download, extract, and set up Kafka:

bash
Copy code
bash command.sh
ZooKeeper: Start the ZooKeeper server.
Kafka Broker: Start the Kafka server.
Create Kafka Topic: A topic named demo_test will be created for message streaming.
2. Configure Cassandra
Start the Cassandra database:

bash
Copy code
cassandra -f
3. Set Up the Producer
The producer reads the stockData.csv file and sends enriched data to Kafka.

4. Set Up the Consumer
The consumer reads messages from Kafka and inserts them into the Cassandra database.

Usage
1. Start the Producer
Run the producer to send stock data to Kafka:
python producer.py

3. Start the Consumer
Run the consumer to process Kafka messages and store them in Cassandra:
python consumer.py

5. Monitor Logs
Both the producer and consumer log key metrics, such as:

Messages sent/processed.
Processing rate (messages/second).
Anomalies injected (producer only).
Folder Structure
bash
Copy code
.
├── command.sh         # Script to set up Kafka
├── producer.py        # Kafka producer (data generation and streaming)
├── consumer.py        # Kafka consumer (data processing and storage)
├── stockData.csv      # Sample stock market data (CSV format)
├── requirements.txt   # Python dependencies
Future Enhancements
Dashboard Integration: Visualize data in real-time using analytics dashboards.
Anomaly Detection: Implement machine learning models to detect anomalies in real-time.
Cloud Deployment: Deploy Kafka and Cassandra on AWS/GCP for scalability.
Data Visualization: Add tools to visualize stock trends and processed data.

