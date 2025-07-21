#!/usr/bin/env python3
"""
Minimal test for debugging
"""
from kafka import KafkaProducer
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

print("Creating producer with debug logging...")

try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        api_version=(0, 10, 0),
        request_timeout_ms=5000,
        connections_max_idle_ms=5000
    )
    
    print("\nProducer created! Sending message...")
    
    future = producer.send('test', b'hello')
    result = future.get(timeout=5)
    
    print(f"\nSuccess! Message sent to partition {result.partition} offset {result.offset}")
    
    producer.close()
    
except Exception as e:
    print(f"\nError: {e}")
    import traceback
    traceback.print_exc()
