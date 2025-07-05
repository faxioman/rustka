#!/usr/bin/env python3
"""
Simple test client for Rustka using kafka-python
"""
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import json
import time

def test_rustka():
    print("Testing Rustka broker...")
    
    # Test 1: Metadata
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=5000,
            api_version=(0, 10, 0)  # Use a compatible version
        )
        print("✓ Connected to broker")
    except Exception as e:
        print(f"✗ Failed to connect: {e}")
        return
    
    # Test 2: Produce
    try:
        future = producer.send('test-topic', {'message': 'Hello Rustka!'})
        result = future.get(timeout=10)
        print(f"✓ Produced message to partition {result.partition} at offset {result.offset}")
    except KafkaError as e:
        print(f"✗ Failed to produce: {e}")
    
    producer.close()
    
    # Test 3: Consume
    try:
        consumer = KafkaConsumer(
            'test-topic',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=2000,
            api_version=(0, 10, 0)
        )
        
        for message in consumer:
            print(f"✓ Consumed message: {message.value} from partition {message.partition} offset {message.offset}")
            break
        
        consumer.close()
    except Exception as e:
        print(f"✗ Failed to consume: {e}")

if __name__ == "__main__":
    test_rustka()
