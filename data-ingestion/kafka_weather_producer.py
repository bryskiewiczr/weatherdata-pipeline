import os
import requests
import json
import time

from kafka import KafkaProducer

API_KEY = os.getenv("OPEN_WEATHER_MAP_API_KEY")
CITY = os.getenv("CITY_NAME")
KAFKA_TOPIC = os.getenv("KAFKA_WEATHER_TOPIC")


def fetch_weather_data() -> dict:
    url = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}"
    response = requests.get(url)
    data = response.json()
    return data


producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

while True:
    weather_data = fetch_weather_data()
    producer.send(KAFKA_TOPIC, weather_data)
    time.sleep(600)  # fetch the data every ten minutes
