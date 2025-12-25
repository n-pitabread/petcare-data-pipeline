from kafka import KafkaConsumer
import psycopg2
import json
import time
import datetime
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

def connect_postgres():
    conn = psycopg2.connect(
        host='postgres',
        port=5432,
        database='dbt_db',
        user='dbt_user',
        password='dbt_pass'
    )
    logger.info("Connected to PostgreSQL")
    return conn

def create_raw_stream_table():
    conn = connect_postgres()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_stream (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            topic VARCHAR(255) NOT NULL,
            message JSONB NOT NULL
        )
    """)
    logger.info("Raw stream table created")
    conn.commit()
    cursor.close()
    conn.close()

def setup_consumer():
    consumer = KafkaConsumer(
        bootstrap_servers=['kafka-broker:29092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id='kafka-consumer-group-2',
        auto_offset_reset='earliest',  # Start from the beginning if no offset is committed
        enable_auto_commit=False,  # Manually commit offsets for better control
        consumer_timeout_ms=1000  # Timeout for polling
    )
    consumer.bootstrap_connected()
    logger.info("Consumer created")
    consumer.subscribe(pattern='.*')
    logger.info("Consumer subscribed to all topics")
    return consumer


def ingest_messages(consumer):
    logger.info("Polling for messages")
    records = consumer.poll(timeout_ms=1000)
    if not records:
        logger.info("No messages received in this poll")
        return
    
    logger.info(f"Received messages from {len(records)} partition(s)")
    for topic_partition, messages in records.items():
        for message in messages:
            logger.info(f"Message offset: {message.offset}, partition: {message.partition}")
            topic = message.topic
            value = message.value
            ts = datetime.datetime.fromtimestamp(message.timestamp / 1000)
            logger.info(f"Ingesting message from topic: {topic} with timestamp: {ts}")
            logger.info(f"Message: {value}")
            insert_raw_message(topic, value, ts)


def insert_raw_message(topic, message, ts):
    conn = connect_postgres()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO raw_stream (timestamp, topic, message)
        VALUES (%s, %s, %s)
    """, (ts, topic, json.dumps(message)))
    conn.commit()
    logger.info("Messages ingested")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    time.sleep(10)
    logger.info("Waiting for Kafka to be ready")
    create_raw_stream_table()
    consumer = setup_consumer()
    logger.info("Starting message ingestion loop")
    while True:
        try:
            ingest_messages(consumer)
            consumer.commit()
        except Exception as e:
            logger.error(f"Error: {e}")
            time.sleep(5)
