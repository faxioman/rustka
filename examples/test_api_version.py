#!/usr/bin/env python3
import time
from confluent_kafka import Producer, Consumer, KafkaError

def test_api_compatibility():
    print("=== Testing API version compatibility ===")
    
    topic = f'test-api-version-{int(time.time() * 1000)}'
    producer = Producer({
        'bootstrap.servers': '127.0.0.1:9092',
        'debug': 'broker',
    })
    delivered = 0
    def delivery_report(err, msg):
        nonlocal delivered
        if err is None:
            delivered += 1
    
    for i in range(3):
        producer.produce(topic, value=f'message-{i}'.encode('utf-8'), callback=delivery_report)
    
    producer.flush()
    print(f"Produced {delivered} messages (librdkafka auto-negotiated API version)")
    consumer = Consumer({
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': f'test-{int(time.time())}',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    })
    
    consumer.subscribe([topic])
    
    count = 0
    start_time = time.time()
    while time.time() - start_time < 2 and count < 3:
        msg = consumer.poll(0.1)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"Consumer error: {msg.error()}")
            continue
        
        count += 1
        print(f"Consumed: {msg.value().decode('utf-8')}")
    
    consumer.close()
    
    print(f"\nConsumed {count} messages")
    return count == 3

if __name__ == "__main__":
    if test_api_compatibility():
        print("\n✓ API version compatibility test passed")
    else:
        print("\n✗ API version compatibility test failed")
        exit(1)