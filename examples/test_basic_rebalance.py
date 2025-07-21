#!/usr/bin/env python3
"""
Basic test to verify that rebalancing works
"""
import time
from kafka import KafkaConsumer, KafkaProducer
import threading

def test_rebalancing():
    topic = 'test-rebalance'
    group = 'test-rebalance-group'
    
    # Produce some messages first
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    for i in range(30):
        producer.send(topic, f'msg-{i}'.encode(), partition=i % 3)
    producer.flush()
    producer.close()
    print("Produced 30 messages across 3 partitions")
    
    consumed_by = {}
    
    def consumer_thread(consumer_id):
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=['localhost:9092'],
            group_id=group,
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000
        )
        
        # Get initial assignment
        consumer.poll(timeout_ms=1000)
        partitions = consumer.assignment()
        print(f"Consumer {consumer_id} assigned partitions: {partitions}")
        
        # Consume messages
        for message in consumer:
            key = f"{message.partition}-{message.offset}"
            consumed_by[key] = consumer_id
            print(f"Consumer {consumer_id} got message from partition {message.partition}")
        
        consumer.close()
        print(f"Consumer {consumer_id} finished")
    
    # Start 3 consumers
    threads = []
    for i in range(3):
        t = threading.Thread(target=consumer_thread, args=(i,))
        t.start()
        threads.append(t)
        time.sleep(1)  # Stagger starts
    
    # Wait for all to finish
    for t in threads:
        t.join()
    
    # Check results
    consumers_used = set(consumed_by.values())
    print(f"\nMessages consumed by {len(consumers_used)} different consumers: {consumers_used}")
    print(f"Total messages consumed: {len(consumed_by)}")
    
    return len(consumers_used) > 1

if __name__ == '__main__':
    success = test_rebalancing()
    if success:
        print("\n✅ Rebalancing works!")
    else:
        print("\n❌ Rebalancing broken - all messages went to one consumer")