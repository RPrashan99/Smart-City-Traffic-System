import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

SENSORS = ["J1", "J2", "J3", "J4"]

producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],  # ← Docker exposes 9092:9092
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=3,  # Retry on failure
    request_timeout_ms=30000,
    connections_max_idle_ms=60000
)

def generate_event():
    sensor_id = random.choice(SENSORS)

    # Occasionally generate critical congestion (low speed)
    if random.random() < 0.05:
        avg_speed = random.uniform(2, 8)       # below 10 km/h
        vehicle_count = random.randint(60, 140)
    else:
        avg_speed = random.uniform(15, 60)
        vehicle_count = random.randint(5, 90)

    return {
        "sensor_id": sensor_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "vehicle_count": vehicle_count,
        "avg_speed": round(avg_speed, 2)
    }

def main():
    print("🚦 Starting Colombo Traffic Producer → kafka:9092/traffic_raw")
    print("✅ Press Ctrl+C to stop")
    
    while True:
        try:
            event = generate_event()
            producer.send("traffic_raw", value=event)
            print(f"✅ Sent: {event['sensor_id']} | {event['vehicle_count']} vehicles | {event['avg_speed']}kph")
            time.sleep(1)
        except Exception as e:
            print(f"❌ Error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
