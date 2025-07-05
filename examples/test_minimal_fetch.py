#!/usr/bin/env python3
"""
Minimal test to check record format
"""
from kafka import KafkaProducer, KafkaConsumer
from kafka.structs import TopicPartition
import time
import struct

# Produce a simple message
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    api_version=(0, 10, 0)
)

topic = f'minimal-test-{int(time.time())}'
future = producer.send(topic, b'Hello World')
metadata = future.get(timeout=10)
print(f"Produced to {metadata.topic} partition {metadata.partition} offset {metadata.offset}")
producer.close()

# Create consumer
consumer = KafkaConsumer(
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id=f'test-{int(time.time())}',
    api_version=(0, 10, 0),
    consumer_timeout_ms=2000,
    enable_auto_commit=False
)

# Assign specific partition
tp = TopicPartition(topic, 0)
consumer.assign([tp])

# Try to poll
print("\nPolling for messages...")
try:
    messages = consumer.poll(timeout_ms=1000, max_records=1)
    print(f"Poll returned {len(messages)} topic-partitions")
    
    for tp, records in messages.items():
        print(f"\nTopicPartition: {tp}")
        print(f"Number of records: {len(records)}")
        
        for record in records:
            print(f"\nRecord details:")
            print(f"  offset: {record.offset}")
            print(f"  timestamp: {record.timestamp}")
            print(f"  key: {record.key}")
            print(f"  value: {record.value}")
            print(f"  value type: {type(record.value)}")
            
            # Try to see raw bytes
            if record.value:
                print(f"  First 20 bytes: {record.value[:20]}")
                # Try to decode as string
                try:
                    decoded = record.value.decode('utf-8')
                    print(f"  Decoded: {decoded}")
                except Exception as e:
                    print(f"  Failed to decode: {e}")
except Exception as e:
    print(f"Error during poll: {e}")
    import traceback
    traceback.print_exc()

consumer.close()
