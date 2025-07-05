#!/usr/bin/env python3
"""
Test API version negotiation with Rustka
"""
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import time
import logging


print("Testing API version negotiation...")

try:
    # Try to connect without specifying API version
    # This should trigger ApiVersions request
    print("\n1. Creating producer WITHOUT api_version (should auto-negotiate)...")
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        request_timeout_ms=5000,
        api_version_auto_timeout_ms=5000
    )
    print("✓ Producer created successfully with auto-negotiation")
    
    # Send a test message
    topic = f'negotiation-test-{int(time.time())}'
    future = producer.send(topic, b'test message')
    result = future.get(timeout=5)
    print(f"✓ Message sent successfully to {topic}")
    
    producer.close()
    
except KafkaError as e:
    print(f"✗ Failed with error: {type(e).__name__}: {e}")
    print("\nTrying with explicit API version...")
    
    # Fallback to explicit version
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        api_version=(0, 10, 0)
    )
    print("✓ Producer created with explicit api_version=(0, 10, 0)")
    producer.close()

print("\n2. Testing consumer auto-negotiation...")
try:
    consumer = KafkaConsumer(
        'test-topic',
        bootstrap_servers=['localhost:9092'],
        group_id='test-negotiation',
        request_timeout_ms=30000,  # Increased to be larger than session timeout
        session_timeout_ms=10000,  # Explicit session timeout
        api_version_auto_timeout_ms=5000
    )
    print("✓ Consumer created successfully with auto-negotiation")
    consumer.close()
except KafkaError as e:
    print(f"✗ Consumer failed: {type(e).__name__}: {e}")
