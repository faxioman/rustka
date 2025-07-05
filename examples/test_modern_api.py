#!/usr/bin/env python3
"""
Test with modern API version to see if throughput improves
"""
from kafka import KafkaProducer, KafkaConsumer
import time
import json

# Test different API versions
api_versions = [
    (0, 10, 0),  # Legacy - Fetch v3
    (0, 11, 0),  # Should use Fetch v4+
    (2, 0, 0),   # Modern
]

topic = f'api-test-{int(time.time())}'

# Produce 50 messages
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(0, 10, 0)  # Use a known working version
)

for i in range(50):
    producer.send(topic, value={'id': i})

producer.flush()
producer.close()
print("✓ Produced 50 messages")

# Test each API version
for idx, api_version in enumerate(api_versions):
    print(f"\n--- Testing API version {api_version} ---")
    
    # Use different group ID and add small delay between tests
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        group_id=f'test-{int(time.time())}-{idx}-{api_version[0]}-{api_version[1]}',
        auto_offset_reset='earliest',
        api_version=api_version,
        consumer_timeout_ms=2000,
        max_poll_records=100
    )
    
    start_time = time.time()
    consumed = 0
    polls = 0
    
    while consumed < 50 and time.time() - start_time < 5:
        polls += 1
        messages = consumer.poll(timeout_ms=100)
        for tp, records in messages.items():
            consumed += len(records)
    
    elapsed = time.time() - start_time
    rate = consumed / elapsed if elapsed > 0 else 0
    
    consumer.close()
    
    print(f"  Consumed: {consumed}/50 messages")
    print(f"  Time: {elapsed:.2f}s")
    print(f"  Polls: {polls}")
    print(f"  Rate: {rate:.1f} msg/sec")
