#!/usr/bin/env python3
"""
Test consumer groups with Rustka
"""
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import json
import time
import threading

def producer_thread(topic_name):
    """Produce messages continuously"""
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(0, 10, 0)
    )
    
    for i in range(100):
        msg = {'index': i, 'timestamp': time.time()}
        producer.send(topic_name, msg, partition=i % 3)  # 3 partitions
        print(f"Produced: {msg}")
        time.sleep(0.5)
    
    producer.close()

def consumer_in_group(consumer_id, topic_name, group_id='test-group'):
    """Consumer that is part of a consumer group"""
    consumer = KafkaConsumer(
        topic_name,
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
    except Exception as e:
        print(f"Consumer {consumer_id} error: {type(e).__name__}: {e}")
        # Print raw message for debugging
        print(f"Debug - Raw message value: {message.value if 'message' in locals() else 'N/A'}")
    finally:
        consumer.close()
        print(f"Consumer {consumer_id} stopped")

def test_consumer_groups():
    print("Testing Rustka Consumer Groups...")
    
    # Use unique topic name to avoid conflicts
    topic_name = f'test-consumer-group-{int(time.time())}'
    group_id = f'test-group-{int(time.time())}'
    
    # Start producer in background
    producer = threading.Thread(target=producer_thread, args=(topic_name,), daemon=True)
    producer.start()
    
    # Start 3 consumers in the same group
    consumers = []
    for i in range(3):
        t = threading.Thread(
            target=consumer_in_group, 
            args=(f"consumer-{i}", topic_name, group_id),
            daemon=True
        )
        t.start()
        consumers.append(t)
        time.sleep(1)  # Give time for join
    
    print(f"\n3 consumers running in group '{group_id}' on topic '{topic_name}'")
    print("Each should handle different partitions.")
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
