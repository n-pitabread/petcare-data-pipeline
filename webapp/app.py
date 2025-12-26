import flask

from utils.utils import get_kafka_producer, get_consultation_cursor
from utils.utils import logger

app = flask.Flask(__name__)

@app.route("/")
def index():
    logger.info("Index page requested")
    return flask.Response("Hello, Welcome to the Petcare WebApp!", status=200)

@app.route("/record/consultations")
def record_dummy_consultations():
    producer = get_kafka_producer()
    index = get_consultation_cursor()
    
    with open("./data/consultations.json", "r") as f:
        line = f.readlines()
        consultation = line[index].rstrip("\n")
        logger.info(f"Sending consultation: {consultation}")
        kafka_producer.send("consultations", value=consultation)
    return flask.Response("Consultation ended successfully.", status=200)
