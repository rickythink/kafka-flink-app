import json
import random
import time
from datetime import datetime
import sys
from threading import Thread

if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

from kafka import KafkaProducer

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers="localhost:9093",  # Kafka broker address
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


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

def simulate_user(user_id, topic, request_per_sec, trigger_ratio, duration):
    """Simulate events for a single user."""
    delay_ms = 1000 / request_per_sec
    end_time = time.time() + duration
    while time.time() < end_time:
        # Decide event type
        if random.random() < trigger_ratio:
            event_type = "trigger"
        else:
            event_type = random.choice(["visit", "scroll", "stay"])

        # Generate and send event
        event = generate_event(event_type, user_id)
        send_event(topic, event)
        print(f"User {user_id} sent event: {event}")
        
        # Wait for next event
        time.sleep(delay_ms / 1000.0)

if __name__ == "__main__":
    # Top-level parameters
    USER = int(sys.argv[1]) if len(sys.argv) > 1 else 1000  # Default 1000 users
    REQUEST_PER_SEC = int(sys.argv[2]) if len(sys.argv) > 2 else 8  # Default 8 requests/sec per user
    TRIGGER_RATIO = float(sys.argv[3]) if len(sys.argv) > 3 else 0.2  # Default 20% trigger events
    DURATION = int(sys.argv[4]) if len(sys.argv) > 4 else 10  # Default 10 seconds

    # Constants
    KAFKA_TOPIC = "event-topic"
    URLS = [f"https://example.com/page{i}" for i in range(1, 11)]

    # Start simulation
    threads = []
    for i in range(USER):
        user_id = f"user_{i+1}"
        thread = Thread(target=simulate_user, args=(user_id, KAFKA_TOPIC, REQUEST_PER_SEC, TRIGGER_RATIO, DURATION))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    print("Event simulation completed.")