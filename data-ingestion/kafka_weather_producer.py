import os
import requests
import json
import time

from kafka import KafkaProducer, KafkaAdminClient
from kafka.errors import NoBrokersAvailable


CITY_NAME = os.getenv("CITY_NAME")
API_KEY = os.getenv("WEATHER_API_KEY")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_BOOTSTRAP_SERVER = "kafka:9092"
API_URL = f"http://api.weatherapi.com/v1/current.json?key={API_KEY}&q={CITY_NAME}&aqi=no"


def is_kafka_up() -> bool:
    retry_timeout = 10
    retries = 5
    while retries > 0:
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER)
            return True
        except NoBrokersAvailable:
            retries -= 1
            print("Kafka broker is not available, retrying...")
            time.sleep(retry_timeout)
    return False


def fetch_weather_data() -> dict:
    response = requests.get(API_URL, timeout=30)
    data = response.json()
    return data


if not is_kafka_up():
    print("Kafka is dead, exiting...")
    exit(1)


PRODUCER = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))


while True:
    weather_data = fetch_weather_data()
    PRODUCER.send(KAFKA_TOPIC, weather_data)
    time.sleep(300)  # fetch the data every five minutes
