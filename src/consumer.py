from kafka import KafkaConsumer
import json

KAFKA_TOPIC = 'ngx_market_data'

# Create a Kafka consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None
)

print(f"Listening for messages on topic: '{KAFKA_TOPIC}'")
for message in consumer:
    if message.value:
        print(f"Received message: {message.value}")
    else:
        print("Received an empty or invalid message, skipping...")
