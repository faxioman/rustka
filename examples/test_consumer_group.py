#!/usr/bin/env python3
"""
Test consumer groups with Rustka using librdkafka
"""
from confluent_kafka import Producer, Consumer, KafkaError
import json
import time
import threading

def producer_thread(topic_name):
    producer = Producer({
        'bootstrap.servers': '127.0.0.1:9092',
    })
    
    def delivery_report(err, msg):
        if err is not None:
            print(f"Delivery failed: {err}")
    
    for i in range(100):
        msg = {'index': i, 'timestamp': time.time()}
        value = json.dumps(msg).encode('utf-8')
        producer.produce(topic_name, value, partition=i % 3, callback=delivery_report)
        print(f"Produced: {msg}")
        producer.poll(0)
        time.sleep(0.5)
    
    producer.flush()

def consumer_in_group(consumer_id, topic_name, group_id='test-group'):
    consumer = Consumer({
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'auto.commit.interval.ms': 5000,
        'client.id': f'consumer-{consumer_id}',
    })
    
    consumer.subscribe([topic_name])
    print(f"Consumer {consumer_id} started in group {group_id}")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer {consumer_id} error: {msg.error()}")
                    continue
            
            try:
                value = json.loads(msg.value().decode('utf-8'))
                print(f"Consumer {consumer_id} consumed: {value} from partition {msg.partition()}")
            except Exception as e:
                print(f"Consumer {consumer_id} decode error: {e}")
                print(f"Debug - Raw message value: {msg.value()}")
                
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"Consumer {consumer_id} error: {type(e).__name__}: {e}")
    finally:
        consumer.close()

def test_consumer_group():
    topic = f'test-group-{int(time.time())}'
    producer = Producer({'bootstrap.servers': '127.0.0.1:9092'})
    producer.produce(topic, b'init', partition=0)
    producer.flush()
    
    print(f"Testing consumer group with topic: {topic}")
    producer_t = threading.Thread(target=producer_thread, args=(topic,))
    producer_t.daemon = True
    producer_t.start()
    consumers = []
    for i in range(3):
        t = threading.Thread(
            target=consumer_in_group, 
            args=(i, topic)
        )
        t.daemon = True
        t.start()
        consumers.append(t)
        time.sleep(1)
    
    print("\nRunning for 20 seconds...")
    print("You should see partitions distributed among consumers")
    print("Press Ctrl+C to stop\n")
    
    try:
        time.sleep(20)
    except KeyboardInterrupt:
        print("\nStopping...")
    
    print("\nTest completed")

if __name__ == "__main__":
    test_consumer_group()