#!/usr/bin/env python3
from confluent_kafka import Producer, Consumer
import time

def test_auto_negotiation():
    print("Testing API version negotiation...")
    print("\n1. Creating producer (librdkafka auto-negotiates)...")
    producer = Producer({
        'bootstrap.servers': '127.0.0.1:9092',
    })
    print("✓ Producer created successfully with auto-negotiation")
    
    # Send a test message
    topic = f'negotiation-test-{int(time.time())}'
    delivered = False
    
    def delivery_report(err, msg):
        nonlocal delivered
        if err is None:
            delivered = True
    
    producer.produce(topic, b'test message', callback=delivery_report)
    producer.flush(timeout=5)
    
    if delivered:
        print(f"✓ Message sent successfully to {topic}")
    else:
        print("✗ Failed to deliver message")
        return False
    print("\n2. Creating consumer (librdkafka auto-negotiates)...")
    consumer = Consumer({
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': f'test-negotiation-{int(time.time())}',
        'auto.offset.reset': 'earliest',
    })
    print("✓ Consumer created successfully with auto-negotiation")
    
    consumer.subscribe([topic])
    start_time = time.time()
    consumed = False
    while time.time() - start_time < 2:
        msg = consumer.poll(0.1)
        if msg and not msg.error():
            print(f"✓ Consumed message: {msg.value().decode('utf-8')}")
            consumed = True
            break
    
    consumer.close()
    
    return delivered and consumed

if __name__ == "__main__":
    if test_auto_negotiation():
        print("\n✓ API negotiation test passed")
    else:
        print("\n✗ API negotiation test failed")
        exit(1)