import logging
import os

from kafka import KafkaProducer

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
logger.addHandler(handler)


def get_kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers=[os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker:29092")],
        value_serializer=lambda x: x.encode("utf-8"),
    )
    if not producer.bootstrap_connected():
        raise Exception("Failed to connect to Kafka")
    logger.info("Kafka producer connected")
    return producer

def get_consultation_cursor():
    cursor_path = os.getenv("CONSULTATION_CURSOR_PATH", "./webapp/consultation_cursor.txt")

    access_mode = 'r+' if os.path.exists(cursor_path) else 'w+'
    with open(cursor_path, access_mode) as f:
        content = f.read().strip()
        index = int(content) if content.isdigit() else 0
        new_index = (index + 1) % 1999
        f.seek(0)
        f.truncate(0)
        f.write(str(new_index))
        return index
