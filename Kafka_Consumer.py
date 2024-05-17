from kafka import KafkaConsumer
from time import sleep
from json import dumps, loads
import json

consumer = KafkaConsumer(
                'demo_test',
                bootstrap_servers=['13.202.25.54:9092'],
                value_deserializer = lambda x: loads(x.decode('utf-8')))

for c in consumer:
    print (c.value)                  