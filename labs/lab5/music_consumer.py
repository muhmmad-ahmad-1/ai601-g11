from kafka import KafkaConsumer


consumer = KafkaConsumer(
    "now_trending_results",   
    bootstrap_servers="localhost:9092",  
    group_id="trending_songs_consumer",  
    auto_offset_reset="earliest" 
)

print("Listening for trending songs...")
while True:
    try:
        for message in consumer:
            print(f"Received: {message.value.decode('utf-8')}")

    except KeyboardInterrupt:
        print("\nShutting down...")
        consumer.close()
        break