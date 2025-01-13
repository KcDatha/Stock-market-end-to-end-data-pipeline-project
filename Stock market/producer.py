from kafka import KafkaProducer
from time import sleep
from json import dumps
import pandas as pd
import logging
import random
from datetime import datetime, timedelta
import signal
import sys
import threading
from tqdm import tqdm
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StockDataProducer:
    def __init__(self):
        self.running = True
        self.messages_sent = 0
        self.start_time = datetime.now()
        self.producer = None
        self.df = None
        self.anomaly_probability = 0.05  # 5% chance of anomaly
        
    def init_producer(self):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda x: dumps(x).encode('utf-8'),
                acks='all',  # Wait for all replicas
                retries=3,
                batch_size=16384,
                linger_ms=1,
                compression_type='gzip'
            )
            logger.info("Kafka producer initialized successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize producer: {e}")
            return False

    def load_data(self):
        try:
            self.df = pd.read_csv('stockData.csv')
            logger.info(f"Loaded {len(self.df)} records from CSV")
            return True
        except Exception as e:
            logger.error(f"Failed to load CSV: {e}")
            return False

    def inject_anomaly(self, data):
        """Randomly inject anomalies into the data"""
        if random.random() < self.anomaly_probability:
            anomaly_type = random.choice(['spike', 'drop', 'null'])
            if anomaly_type == 'spike':
                data['close'] *= random.uniform(1.5, 2.0)
                data['anomaly'] = 'price_spike'
            elif anomaly_type == 'drop':
                data['close'] *= random.uniform(0.5, 0.8)
                data['anomaly'] = 'price_drop'
            else:
                data['close'] = None
                data['anomaly'] = 'null_value'
            logger.warning(f"Injected {anomaly_type} anomaly")
        return data

    def add_synthetic_features(self, data):
        """Add synthetic features for analysis"""
        data['timestamp'] = datetime.now().isoformat()
        data['moving_avg_5'] = self.df['close'].rolling(window=5).mean().iloc[-1]
        data['volatility'] = self.df['close'].rolling(window=10).std().iloc[-1]
        data['price_momentum'] = (data['close'] - data['open']) / data['open']
        data['volume_surge'] = random.uniform(0.8, 1.2)
        return data

    def simulate_market_sentiment(self):
        """Simulate market sentiment indicators"""
        return {
            'market_sentiment': random.choice(['bullish', 'bearish', 'neutral']),
            'confidence_score': random.uniform(0, 1),
            'trading_volume_trend': random.choice(['increasing', 'decreasing', 'stable'])
        }

    def progress_monitor(self):
        """Monitor and display progress statistics"""
        with tqdm(total=None, desc="Messages Sent") as pbar:
            last_count = 0
            while self.running:
                current_count = self.messages_sent
                increment = current_count - last_count
                pbar.update(increment)
                last_count = current_count
                sleep(0.1)

    def calculate_metrics(self):
        """Calculate and return performance metrics"""
        duration = (datetime.now() - self.start_time).total_seconds()
        return {
            'total_messages': self.messages_sent,
            'messages_per_second': self.messages_sent / duration if duration > 0 else 0,
            'runtime_seconds': duration,
            'anomalies_generated': int(self.messages_sent * self.anomaly_probability)
        }

    def signal_handler(self, signum, frame):
        """Handle graceful shutdown"""
        logger.info("Shutdown signal received, stopping producer...")
        self.running = False
        metrics = self.calculate_metrics()
        logger.info("Performance Metrics:")
        for key, value in metrics.items():
            logger.info(f"{key}: {value:.2f}")
        
        if self.producer:
            self.producer.flush()
            self.producer.close()
        sys.exit(0)

    def run(self):
        if not self.init_producer() or not self.load_data():
            return

        # Start progress monitor in separate thread
        monitor_thread = threading.Thread(target=self.progress_monitor)
        monitor_thread.daemon = True
        monitor_thread.start()

        signal.signal(signal.SIGINT, self.signal_handler)
        
        while self.running:
            try:
                # Get base data
                sample_data = self.df.sample(1).to_dict(orient='records')[0]
                
                # Enhance data
                sample_data = self.inject_anomaly(sample_data)
                sample_data = self.add_synthetic_features(sample_data)
                sample_data.update(self.simulate_market_sentiment())
                
                # Send to Kafka
                self.producer.send('demo_test', value=sample_data)
                self.messages_sent += 1

                # Adaptive sleep based on system load
                sleep_time = random.uniform(0.05, 0.15)
                sleep(sleep_time)

                # Periodic status update
                if self.messages_sent % 100 == 0:
                    metrics = self.calculate_metrics()
                    logger.info(f"Messages sent: {metrics['total_messages']}, "
                              f"Rate: {metrics['messages_per_second']:.2f} msgs/sec")

            except Exception as e:
                logger.error(f"Error in message production: {e}")
                sleep(1)  # Back off on error

if __name__ == "__main__":
    producer = StockDataProducer()
    producer.run()
