from cassandra.cluster import Cluster
from kafka import KafkaConsumer
from json import loads, dumps
import logging
from datetime import datetime
import time
from collections import deque
import signal
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Metrics tracking
class Metrics:
    def __init__(self):
        self.messages_processed = 0
        self.start_time = time.time()
        self.batch_queue = deque(maxlen=100)  # Process in batches of 100
        
    def add_message(self):
        self.messages_processed += 1
        
    def get_throughput(self):
        elapsed_time = time.time() - self.start_time
        return self.messages_processed / elapsed_time if elapsed_time > 0 else 0

metrics = Metrics()

# Graceful shutdown handler
def signal_handler(sig, frame):
    logger.info("Shutting down gracefully...")
    logger.info(f"Total messages processed: {metrics.messages_processed}")
    logger.info(f"Average throughput: {metrics.get_throughput():.2f} messages/second")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# Initialize Kafka consumer with retry logic
def init_kafka_consumer(max_retries=3, retry_delay=5):
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                'demo_test',
                bootstrap_servers=['localhost:9092'],
                value_deserializer=lambda x: loads(x.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                group_id='stock_market_group'
            )
            logger.info("Kafka consumer initialized successfully")
            return consumer
        except Exception as e:
            logger.error(f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
    return None

# Initialize Cassandra session with retry logic
def init_cassandra_session(max_retries=3, retry_delay=5):
    for attempt in range(max_retries):
        try:
            cluster = Cluster(['localhost'])
            session = cluster.connect()
            
            # Create keyspace and table
            session.execute("""
                CREATE KEYSPACE IF NOT EXISTS stockmarket 
                WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}
            """)
            session.set_keyspace("stockmarket")
            session.execute('''
                CREATE TABLE IF NOT EXISTS stock_market_data (
                    id int PRIMARY KEY,
                    "index" varchar,
                    date varchar,
                    open float,
                    high float,
                    low float,
                    close float,
                    "adj close" float,
                    volume bigint,
                    closeUSD float,
                    processed_timestamp timestamp
                )
            ''')
            logger.info("Cassandra session initialized successfully")
            return session
        except Exception as e:
            logger.error(f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
    return None

def batch_insert(session, messages):
    if not messages:
        return
    
    try:
        # Prepare batch statement
        batch_query = "BEGIN BATCH "
        for msg in messages:
            msg['processed_timestamp'] = datetime.now()
            batch_query += f"INSERT INTO stock_market_data JSON '{dumps(msg)}'; "
        batch_query += "APPLY BATCH;"
        
        session.execute(batch_query)
        logger.info(f"Batch inserted successfully: {len(messages)} records")
    except Exception as e:
        logger.error(f"Batch insert failed: {str(e)}")

def main():
    consumer = init_kafka_consumer()
    session = init_cassandra_session()

    if not consumer or not session:
        logger.error("Failed to initialize services. Exiting.")
        return

    logger.info("Starting message processing...")
    message_id = 0
    
    try:
        for message in consumer:
            if message.value:
                try:
                    message_id += 1
                    new_data = {
                        'Id': message_id,
                        **message.value,
                    }
                    metrics.batch_queue.append(new_data)
                    metrics.add_message()

                    # Process batch when queue is full
                    if len(metrics.batch_queue) == metrics.batch_queue.maxlen:
                        batch_insert(session, list(metrics.batch_queue))
                        metrics.batch_queue.clear()
                        
                    # Log progress every 1000 messages
                    if message_id % 1000 == 0:
                        logger.info(f"Processed {message_id} messages. Current throughput: {metrics.get_throughput():.2f} msgs/sec")
                        
                except Exception as e:
                    logger.error(f"Error processing message {message_id}: {str(e)}")
                    continue
                
            consumer.commit()  # Commit offset only after successful processing
            
    except KeyboardInterrupt:
        signal_handler(None, None)
    finally:
        # Process any remaining messages in the batch queue
        if metrics.batch_queue:
            batch_insert(session, list(metrics.batch_queue))

if __name__ == "__main__":
    main()
