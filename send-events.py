import json
import random
import time
from datetime import datetime
import sys

if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

from kafka import KafkaProducer

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",  # Kafka broker address
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Constants for simulation
USER_IDS = [f"user_{i}" for i in range(1, 101)]  # Simulate 100 users
URLS = [f"https://example.com/page{i}" for i in range(1, 11)]  # Simulate 10 pages
EVENT_TYPES = ["visit", "scroll", "stay", "trigger"]  # Event types

def generate_event(event_type, user_id):
    """Generate an event based on the type."""
    now = int(time.time() * 1000)  # Current time in milliseconds
    if event_type == "visit":
        return {
            "event_type": "visit",
            "url": random.choice(URLS),
            "time": now,
            "user_id": user_id
        }
    elif event_type == "scroll":
        return {
            "event_type": "scroll",
            "url": random.choice(URLS),
            "scroll_value": random.randint(100, 1000),
            "time": now,
            "user_id": user_id
        }
    elif event_type == "stay":
        return {
            "event_type": "stay",
            "url": random.choice(URLS),
            "time": now,
            "user_id": user_id
        }
    elif event_type == "trigger":
        return {
            "event_type": "trigger",
            "time": now,
            "user_id": user_id
        }

def send_event(topic, event):
    """Send an event to the Kafka topic."""
    producer.send(topic, event)
    producer.flush()

def simulate_events(topic, num_events, delay_ms):
    """Simulate sending events to Kafka."""
    print(f"Simulating {num_events} events with a delay of {delay_ms}ms")
    for _ in range(num_events):
        user_id = random.choice(USER_IDS)
        event_type = random.choice(EVENT_TYPES)
        event = generate_event(event_type, user_id)
        send_event(topic, event)
        print(f"Sent event: {event}")
        time.sleep(delay_ms / 1000.0)

if __name__ == "__main__":
    # Simulation parameters
    KAFKA_TOPIC = "event-topic"
    NUM_EVENTS = 1000  # Number of events to send
    DELAY_MS = 10      # Delay between events in milliseconds

    # Simulate events
    simulate_events(KAFKA_TOPIC, NUM_EVENTS, DELAY_MS)