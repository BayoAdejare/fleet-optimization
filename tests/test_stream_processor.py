#!/usr/bin/env python3
"""Test script for the stream processor."""

import json
import time
from kafka import KafkaProducer

def generate_sample_data():
    """Generate sample vehicle telemetry data."""
    return {
        "vehicle_id": "TRUCK-1234",
        "timestamp": int(time.time() * 1000),
        "latitude": 40.7128,
        "longitude": -74.0060,
        "speed": 65.5,
        "fuel_level": 78.2,
        "engine_rpm": 2100.0,
        "engine_temp": 85.0,
        "oil_pressure": 42.5,
        "odometer": 125430.7,
        "acceleration": 0.2,
        "brake_usage": 0.1
    }

def main():
    """Produce test messages to Kafka."""
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    for i in range(10):
        data = generate_sample_data()
        # Vary some values for testing
        data['speed'] = 65 + (i % 3) * 10
        data['engine_temp'] = 85 + (i % 5)
        
        producer.send('raw_vehicle_telemetry', data)
        print(f"Sent message {i+1}")
        time.sleep(0.5)
    
    producer.flush()
    print("Test messages sent")

if __name__ == "__main__":
    main()