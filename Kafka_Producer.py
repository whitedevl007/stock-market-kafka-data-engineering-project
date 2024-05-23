
import pandas as pd
from kafka import KafkaProducer
from time import sleep
import time
from json import dumps
import json
from dotenv import load_dotenv
import os


load_dotenv()


bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
kafka_topic = os.getenv('KAFKA_TOPIC')

producer = KafkaProducer(
    bootstrap_servers=[bootstrap_servers],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

producer.send(kafka_topic, value={'name': 'nizam'})

df = pd.read_csv("indexProcessed.csv")
print(df.head()) 

while True:
    dict_stock = df.sample(1).to_dict(orient="records")[0]
    producer.send(kafka_topic, value=dict_stock)
    sleep(1)

