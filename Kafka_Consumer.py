from kafka import KafkaConsumer
from time import sleep
from json import loads
from dotenv import load_dotenv
import os
from s3fs import S3FileSystem


load_dotenv()


bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
kafka_topic = os.getenv('KAFKA_TOPIC')

consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=[bootstrap_servers],
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

# for message in consumer:
#     print(message.value)

s3 = S3FileSystem()

for count, i in enumerate(consumer):
    print(count)
    print(i.value)
