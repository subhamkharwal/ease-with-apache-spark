# Method posts events to Kafka Server
# run command in kafka server to create topic : 
# ./usr/bin/kafka-topics --create --topic device_data --bootstrap-server kafka:9092 
from kafka import KafkaProducer, KafkaConsumer
import time
import random
from device_events import generate_events

__bootstrap_server = "kafka:29092"


def post_to_kafka(data):
    print('data: '+ str(data))
    producer = KafkaProducer(bootstrap_servers=__bootstrap_server)
    producer.send('devices', key=b'device', value=data)
    #producer.flush()
    producer.close()
    print("Posted to topic")


if __name__ == "__main__":
    _offset = 10000
    while True:
        post_to_kafka(bytes(str(generate_events(offset=_offset)), 'utf-8'))
        time.sleep(random.randint(0, 5))
        _offset += 1