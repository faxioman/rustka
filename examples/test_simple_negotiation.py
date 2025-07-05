#!/usr/bin/env python3
"""
Test simple API version negotiation - force older protocol version
"""
from kafka import KafkaProducer
import os

# Force kafka-python to use older protocol that might work better
os.environ['KAFKA_VERSION'] = '0.10.0'

print("Testing with forced Kafka version 0.10.0...")

try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        api_version=(0, 10, 0)  # Explicitly use 0.10.0
    )
    print("✓ Producer connected successfully!")
    
    # Send a test message
    future = producer.send('test-topic', b'hello rustka')
    result = future.get(timeout=5)
    print(f"✓ Message sent: {result}")
    
    producer.close()
    
except Exception as e:
    print(f"✗ Failed: {type(e).__name__}: {e}")
