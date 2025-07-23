#!/usr/bin/env python3
"""
Basic test to verify that rebalancing works
"""
import time
from confluent_kafka import Producer, Consumer, KafkaError
import threading

def test_rebalancing():
    topic = 'test-rebalance'
    group = 'test-rebalance-group'
    
    consumed_by = {}
    consumer_ready = [False, False, False]
    
    def consumer_thread(consumer_id):
        consumer = Consumer({
            'bootstrap.servers': '127.0.0.1:9092',
            'group.id': group,
            'client.id': f'consumer-{consumer_id}',
            'auto.offset.reset': 'earliest',
        })
        
        consumer.subscribe([topic])
        
        # Get initial assignment
        def on_assign(consumer, partitions):
            parts = [p.partition for p in partitions]
            print(f"Consumer {consumer_id} assigned partitions: {parts}")
            consumer_ready[consumer_id] = True
        
        def on_revoke(consumer, partitions):
            parts = [p.partition for p in partitions]
            print(f"Consumer {consumer_id} revoked partitions: {parts}")
        
        consumer.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)
        
        # Consume messages
        start_time = time.time()
        while time.time() - start_time < 10:
            msg = consumer.poll(0.5)
            if msg and not msg.error():
                key = f"{msg.partition()}-{msg.offset()}"
                consumed_by[key] = consumer_id
                print(f"Consumer {consumer_id} got message from partition {msg.partition()}")
        
        consumer.close()
        print(f"Consumer {consumer_id} finished")
    
    # Start all consumers first
    threads = []
    
    print("\nStarting all consumers...")
    for i in range(3):
        print(f"Starting consumer {i}...")
        t = threading.Thread(target=consumer_thread, args=(i,))
        t.start()
        threads.append(t)
        time.sleep(0.1)  # Small delay between starts
    
    # Wait for all consumers to be ready (assigned partitions)
    print("\nWaiting for all consumers to be ready...")
    for i in range(30):  # Max 3 seconds
        if all(consumer_ready):
            print("All consumers ready!")
            break
        time.sleep(0.1)
    
    # Now produce messages after all consumers are ready
    print("\nProducing messages...")
    producer = Producer({'bootstrap.servers': '127.0.0.1:9092'})
    for i in range(30):
        producer.produce(topic, f'msg-{i}'.encode(), partition=i % 3)
    producer.flush()
    print("Produced 30 messages across 3 partitions")
    
    # Wait for all to finish consuming
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