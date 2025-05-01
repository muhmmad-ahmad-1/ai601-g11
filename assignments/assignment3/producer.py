
from kafka import KafkaProducer
import json
import time
import random

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
TOPIC = "traffic_data"

# Create Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

sensors = ["S101", "S102", "S103", "S104", "S105"]

# Generate and send random traffic data
while True:
    data = {
        "sensor_id": random.choice(sensors),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "vehicle_count": random.randint(5, 50),
        "average_speed": round(random.uniform(20, 80), 2),
        "congestion_level": random.choice(["LOW", "MEDIUM", "HIGH"])
    }
    producer.send(TOPIC, value=data)
    print(f"Sent: {data}")
    time.sleep(1)  # Simulating real-time data
