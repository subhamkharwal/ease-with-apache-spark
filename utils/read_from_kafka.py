# Method posts events to Kafka Server
from kafka import KafkaProducer, KafkaConsumer

__bootstrap_server = "kafka:29092"


def read_from_kafka():
    print("Reading through consumer")
    consumer = KafkaConsumer('devices', bootstrap_servers = __bootstrap_server)
    consumer.poll(timeout_ms=2000)
    for m in consumer:
        msg = str(m.value.decode('utf-8'))
        print('getting->' + msg)


if __name__ == "__main__":
    read_from_kafka()