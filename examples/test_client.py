#!/usr/bin/env python3
from confluent_kafka import Producer, Consumer, KafkaError
import json
import time

def test_rustka():
    print("Testing Rustka broker...")
    try:
        producer = Producer({
            'bootstrap.servers': '127.0.0.1:9092',
        })
        print("✓ Connected to broker")
    except Exception as e:
        print(f"✗ Failed to connect: {e}")
        return
    try:
        delivered = False
        partition = None
        offset = None
        
        def delivery_report(err, msg):
            nonlocal delivered, partition, offset
            if err is not None:
                print(f"✗ Failed to produce: {err}")
            else:
                delivered = True
                partition = msg.partition()
                offset = msg.offset()
        
        value = json.dumps({'message': 'Hello Rustka!'}).encode('utf-8')
        producer.produce('test-topic', value, callback=delivery_report)
        producer.flush(timeout=10)
        
        if delivered:
            print(f"✓ Produced message to partition {partition} at offset {offset}")
    except Exception as e:
        print(f"✗ Failed to produce: {e}")
    try:
        consumer = Consumer({
            'bootstrap.servers': '127.0.0.1:9092',
            'group.id': 'test-client-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        })
        
        consumer.subscribe(['test-topic'])
        start_time = time.time()
        while time.time() - start_time < 2:
            msg = consumer.poll(0.1)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"✗ Consumer error: {msg.error()}")
                    break
            
            value = json.loads(msg.value().decode('utf-8'))
            print(f"✓ Consumed message: {value} from partition {msg.partition()} offset {msg.offset()}")
            break
        
        consumer.close()
    except Exception as e:
        print(f"✗ Failed to consume: {e}")

if __name__ == "__main__":
    test_rustka()
