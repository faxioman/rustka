#!/usr/bin/env python3
"""
Minimal test for debugging
"""
from confluent_kafka import Producer
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

print("Creating producer with debug logging...")

try:
    producer = Producer({
        'bootstrap.servers': '127.0.0.1:9092',
        'debug': 'broker,topic,msg',  # Enable debug logging
    })
    
    print("\nProducer created! Sending message...")
    
    def delivery_report(err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"\nSuccess! Message sent to partition {msg.partition()} offset {msg.offset()}")
    
    producer.produce('test', b'hello', callback=delivery_report)
    
    # Wait for delivery
    producer.flush(timeout=5)
    
except Exception as e:
    print(f"\nError: {e}")
    import traceback
    traceback.print_exc()
