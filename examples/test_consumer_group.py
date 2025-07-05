#!/usr/bin/env python3
"""
Test consumer groups con Rustka
"""
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import json
import time
import threading

def producer_thread():
    """Produce messaggi continuamente"""
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(0, 10, 0)
    )
    
    for i in range(100):
        msg = {'index': i, 'timestamp': time.time()}
        producer.send('test-topic', msg, partition=i % 3)  # 3 partitions
        print(f"Produced: {msg}")
        time.sleep(0.5)
    
    producer.close()

def consumer_in_group(consumer_id, group_id='test-group'):
    """Consumer that is part of a consumer group"""
    consumer = KafkaConsumer(
        'test-topic',
        bootstrap_servers=['localhost:9092'],
        group_id=group_id,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        api_version=(0, 10, 0)
    )
    
    print(f"Consumer {consumer_id} started in group {group_id}")
    
    try:
        for message in consumer:
            print(f"Consumer {consumer_id} consumed: {message.value} from partition {message.partition}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print(f"Consumer {consumer_id} stopped")

def test_consumer_groups():
    print("Testing Rustka Consumer Groups...")
    
    # Start producer in background
    producer = threading.Thread(target=producer_thread, daemon=True)
    producer.start()
    
    # Start 3 consumers in the same group
    consumers = []
    for i in range(3):
        t = threading.Thread(
            target=consumer_in_group, 
            args=(f"consumer-{i}",),
            daemon=True
        )
        t.start()
        consumers.append(t)
        time.sleep(1)  # Give time for join
    
    print("\n3 consumers running in the same group. Each should handle different partitions.")
    print("Press Ctrl+C to stop...\n")
    
    try:
        # Wait for producer to finish
        producer.join()
        # Wait a bit more to see consumers process
        time.sleep(5)
    except KeyboardInterrupt:
        print("\nStopping...")

if __name__ == "__main__":
    test_consumer_groups()
