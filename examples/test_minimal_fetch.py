#!/usr/bin/env python3
"""
Minimal test to check record format with librdkafka
"""
from confluent_kafka import Producer, Consumer, TopicPartition
import time

# Produce a simple message
producer = Producer({
    'bootstrap.servers': '127.0.0.1:9092',
})

topic = f'minimal-test-{int(time.time())}'
partition = 0
delivered_offset = None

def delivery_report(err, msg):
    global delivered_offset
    if err is None:
        delivered_offset = msg.offset()
        print(f"Produced to {msg.topic()} partition {msg.partition()} offset {msg.offset()}")

producer.produce(topic, b'Hello World', partition=partition, callback=delivery_report)
producer.flush()

# Create consumer
consumer = Consumer({
    'bootstrap.servers': '127.0.0.1:9092',
    'group.id': f'test-{int(time.time())}',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
})

# Assign specific partition
tp = TopicPartition(topic, partition, 0)  # Start from offset 0
consumer.assign([tp])

# Try to poll
print("\nPolling for messages...")
try:
    msg = consumer.poll(timeout=2.0)
    
    if msg is None:
        print("No messages received")
    elif msg.error():
        print(f"Consumer error: {msg.error()}")
    else:
        print(f"\nRecord details:")
        print(f"  topic: {msg.topic()}")
        print(f"  partition: {msg.partition()}")
        print(f"  offset: {msg.offset()}")
        print(f"  timestamp: {msg.timestamp()}")
        print(f"  key: {msg.key()}")
        print(f"  value: {msg.value()}")
        print(f"  value type: {type(msg.value())}")
        
        # Show raw bytes
        if msg.value():
            print(f"  First 20 bytes: {msg.value()[:20]}")
            # Try to decode as string
            try:
                decoded = msg.value().decode('utf-8')
                print(f"  Decoded: {decoded}")
            except Exception as e:
                print(f"  Failed to decode: {e}")
                
except Exception as e:
    print(f"Error during poll: {e}")
    import traceback
    traceback.print_exc()

consumer.close()