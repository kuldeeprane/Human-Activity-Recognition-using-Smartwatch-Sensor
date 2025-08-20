import json, random, time
from datetime import datetime
from kafka import KafkaProducer

KAFKA_TOPIC = 'har_events'
KAFKA_BROKER = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
print(f"Starting Kafka producer for topic '{KAFKA_TOPIC}'...")

while True:
    try:
        data = {
            'user_id': f'user_{random.randint(1, 10)}',
            'activity': random.choice(['Walking', 'Running', 'Cycling', 'Yoga']),
            'duration_minutes': random.randint(10, 60),
            'event_time': datetime.now().isoformat()
        }
        producer.send(KAFKA_TOPIC, value=data)
        print(f"Sent: {data}")
        time.sleep(5)
    except KeyboardInterrupt:
        print("\nProducer stopped.")
        break
