from kafka import KafkaConsumer
import time

topic = 'ngx_market_data'
consumer = KafkaConsumer(
    topic,
    group_id='mg',
    bootstrap_servers='localhost:9092',
)

for message in consumer:
    print(f"{message.partition}:{message.offset}: v={message.value}, time={time.time()}")