# **Stock Market Data Pipeline with Kafka and Cassandra**

This project is a real-time data pipeline for processing and analyzing stock market data. It uses **Apache Kafka** for real-time messaging and **Apache Cassandra** for scalable and fault-tolerant data storage. The producer generates synthetic stock market data, while the consumer processes and stores it in a Cassandra database.

---

## **Table of Contents**
1. [Features](#features)
2. [Technologies Used](#technologies-used)
3. [System Requirements](#system-requirements)
4. [Setup and Installation](#setup-and-installation)

---

## **Features**
- **Real-Time Data Streaming**: Simulate live stock market data streams using Kafka.
- **Data Enrichment**: Enhance stock data with synthetic features (e.g., moving averages, volatility).
- **Anomaly Injection**: Introduce anomalies like price spikes, drops, and null values for testing.
- **Batch Processing**: Process stock data in batches for optimized database insertion.
- **Fault-Tolerant Storage**: Store processed data in Cassandra for scalability and reliability.
- **Metrics Logging**: Track message processing rate and throughput.

---

## **Technologies Used**
- **Apache Kafka**: Real-time message streaming platform.
- **Apache Cassandra**: Distributed NoSQL database for scalable storage.
- **Python Libraries**:
  - `kafka-python`: Kafka integration.
  - `cassandra-driver`: Cassandra database client.
  - `pandas`: Data manipulation.
- **Other Tools**:
  - `tqdm`: For monitoring progress.
  - `logging`: For metrics tracking.

---

## **System Requirements**
- **Java 8 or higher** (for Kafka).
- **Python 3.6+**.
- **Cassandra**: Installed and running.
- **Python Dependencies**: Install the required libraries using:
  ```bash
  pip install -r requirements.txt

---
# **Setup and Installation Guide**

This document explains how to set up **Apache Kafka** for the project. Follow these steps to download, extract, and configure Kafka.

---

## **Steps**

### **1. Install Apache Kafka**
Run the `command.sh` script to download, extract, and set up Kafka:

```bash
bash command.sh

