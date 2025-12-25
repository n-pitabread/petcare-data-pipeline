import logging
import flask
from kafka import KafkaProducer

app = flask.Flask(__name__)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

@app.route('/')
def index():
    logger.info("Index page requested")
    return flask.Response("Hello, Welcome to the Petcare WebApp!", status=200)

@app.route('/record/consultations')
def record_dummy_consultations():
    kafka_producer = KafkaProducer(
        bootstrap_servers=['kafka-broker:29092'],
        value_serializer=lambda x: x.encode('utf-8'),
    )
    kafka_producer.bootstrap_connected()
    logger.info("Kafka producer connected")
    with open('./webapp/consultation_cursor.txt', 'r') as f:
        index = int(f.read())
        new_index = (index + 1) % 1999
    with open('./webapp/consultation_cursor.txt', 'w') as f:
        f.write(str(new_index))

    with open('./data/consultations.json', 'r') as f:
        line = f.readlines()
        consultation = line[index].rstrip('\n')
        logger.info(f"Sending consultation: {consultation}")
        kafka_producer.send('consultations', value=consultation)
    return flask.Response("Consultation ended successfully.", status=200)
