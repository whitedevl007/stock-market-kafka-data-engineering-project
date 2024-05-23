from kafka import KafkaConsumer
from time import sleep
from json import loads, dumps
from dotenv import load_dotenv
import json
import os
from s3fs import S3FileSystem

load_dotenv()

bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
kafka_topic = os.getenv('KAFKA_TOPIC')

if not bootstrap_servers:
    raise ValueError("KAFKA_BOOTSTRAP_SERVERS environment variable is not set")
if not kafka_topic:
    raise ValueError("KAFKA_TOPIC environment variable is not set")

consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=[bootstrap_servers],
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

s3 = S3FileSystem()

for count, message in enumerate(consumer):
    try:
        # Create the filename for the S3 object
        file_path = f"s3://my-kafka-stock/stock_market_{count}.json"
        
        # Write the message value to the S3 file
        with s3.open(file_path, 'w') as file:
            json.dump(message.value, file)
            
        print(f"Message {count} written to {file_path}")
        
    except Exception as e:
        print(f"Error writing message {count} to S3: {e}")

    # Sleep to simulate a delay (optional)
    sleep(1)
