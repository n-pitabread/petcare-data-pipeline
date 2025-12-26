import datetime
import json
import logging
import os
import time

from kafka import KafkaConsumer
import psycopg2

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
logger.addHandler(handler)

PG_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": os.getenv("POSTGRES_PORT", 5432),
    "database": os.getenv("POSTGRES_DB", "dbt_db"),
    "user": os.getenv("POSTGRES_USER", "dbt_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "dbt_pass"),
}


def connect_postgres():
    conn = psycopg2.connect(**PG_CONFIG)
    logger.info("Connected to PostgreSQL")
    return conn


def create_raw_stream_table(pg_client):
    with pg_client.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_stream (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP NOT NULL,
                topic VARCHAR(255) NOT NULL,
                message JSONB NOT NULL
            )
        """
        )
        logger.info("Raw stream table created")
        pg_client.commit()
    pg_client.close()
    logger.info("PostgreSQL connection closed")


def setup_consumer():
    consumer = KafkaConsumer(
        bootstrap_servers=[os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker:29092")],
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        group_id=os.getenv("KAFKA_CONSUMER_GROUP_ID", "kafka-consumer-group"),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=1000,
    )
    if not consumer.bootstrap_connected():
        raise Exception("Failed to connect to Kafka")
    logger.info("Consumer connected to Kafka")
    consumer.subscribe(topics=os.getenv("KAFKA_TOPICS", [".*"]))
    logger.info(f"Consumer subscribed to topics: {os.getenv('KAFKA_TOPICS', ['.*'])}")
    return consumer


def ingest_messages(consumer):
    records = consumer.poll(timeout_ms=1000)
    if not records:
        return

    logger.info(f"Received messages from {len(records)} partition(s)")
    for topic_partition, messages in records.items():
        for message in messages:
            logger.info(
                f"Message offset: {message.offset}, partition: {message.partition}"
            )
            topic = message.topic
            value = message.value
            ts = datetime.datetime.fromtimestamp(message.timestamp / 1000)
            logger.info(f"Ingesting message from topic: {topic} with timestamp: {ts}")
            logger.info(f"Message: {value}")
            pg_client = connect_postgres()
            insert_raw_message(pg_client, topic, value, ts)


def insert_raw_message(pg_client, topic, message, ts):
    with pg_client.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO raw_stream (timestamp, topic, message)
            VALUES (%s, %s, %s)
        """,
            (ts, topic, json.dumps(message)),
        )
        pg_client.commit()
        logger.info("Messages ingested")
        cursor.close()
    pg_client.close()


if __name__ == "__main__":
    pg_client = connect_postgres()
    consumer = setup_consumer()

    create_raw_stream_table(pg_client)
    logger.info("Starting message ingestion loop")
    while True:
        try:
            ingest_messages(consumer)
            consumer.commit()
        except Exception as e:
            logger.error(f"Error: {e}")
            time.sleep(5)
