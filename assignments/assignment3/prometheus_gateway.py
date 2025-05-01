from kafka import KafkaConsumer
import json
import requests
import time

TOPIC = "traffic_analysis"

# Base Pushgateway URL
PUSHGATEWAY_BASE_URL = "http://localhost:9091/metrics/job/"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    group_id='prometheus-pusher',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


def push_to_prometheus(data):
    """Push metrics to Prometheus Pushgateway with job name based on sensor_id."""
    metric_type = data["type"]
    sensor_id = data["sensor_id"]
    metric_name = f"traffic_{metric_type}"
    
    # Define the job name using the sensor_id
    job_name = f"traffic_sensor_{sensor_id}"
    PUSHGATEWAY_URL = f"{PUSHGATEWAY_BASE_URL}{job_name}"
    
    # Labels for the metric (sensor_id included for consistency)
    labels = [f'sensor_id="{sensor_id}"']
    label_str = "{" + ", ".join(labels) + "}"

    metrics = []

    # Build the metric payload based on the metric type
    if metric_type == "traffic_volume":
        metrics.append(f'{metric_name}{label_str} {data["col4"]}')
    elif metric_type == "congestion_hotspots":
        metrics.append(f'{metric_name}{label_str} {data["col4"]}')
    elif metric_type == "average_speed":
        metrics.append(f'{metric_name}{label_str} {data["col4"]}')
    elif metric_type == "speed_drop":
        metrics.append(f'{metric_name}_max_speed{label_str} {data["col4"]}')
        metrics.append(f'{metric_name}_min_speed{label_str} {data["col5"]}')
    elif metric_type == "busiest_sensors":
        metrics.append(f'{metric_name}{label_str} {data["col3"]}')

    # Send metrics to Pushgateway
    payload = "\n".join(metrics) + "\n"
    try:
        response = requests.post(PUSHGATEWAY_URL, data=payload.encode('utf-8'), timeout=5)
        response.raise_for_status()
        print(f"Successfully pushed {metric_type} metrics for {sensor_id} to {PUSHGATEWAY_URL}")
    except requests.exceptions.RequestException as e:
        print(f"Error pushing to Pushgateway: {e}")


# Consume messages from Kafka and push to Prometheus
for message in consumer:
    data = message.value
    print(f"Received data: {data}")
    push_to_prometheus(data)