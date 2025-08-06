import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# --- Kafka Producer Setup ---
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

ACTIVITIES = ['Walking', 'Running', 'Working', 'Resting', 'Eating']
USER_IDS = ['user_A', 'user_B', 'user_C']

def generate_activity_data():
    """Generates a single activity event."""
    return {
        'user_id': random.choice(USER_IDS),
        'activity': random.choice(ACTIVITIES),
        'timestamp': datetime.now().isoformat()
    }

if __name__ == "__main__":
    print("Starting activity data simulation...")
    while True:
        activity_data = generate_activity_data()
        print(f"Producing: {activity_data}")
        producer.send('raw_activity_data', activity_data)
        time.sleep(random.uniform(1, 5)) # Simulate data every 1-5 seconds