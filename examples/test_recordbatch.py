#!/usr/bin/env python3
import sys
from confluent_kafka import Producer, Consumer, KafkaError
import json
import time

def test_single_fetch():
    print("1. Producing a single test record...")
    
    producer = Producer({
        'bootstrap.servers': '127.0.0.1:9092',
    })
    
    topic = f'test-{int(time.time())}'
    
    test_value = json.dumps({'test': 'message', 'id': 1})
    
    delivered = False
    partition = None
    offset = None
    
    def delivery_report(err, msg):
        nonlocal delivered, partition, offset
        if err is None:
            delivered = True
            partition = msg.partition()
            offset = msg.offset()
    
    producer.produce(topic, value=test_value.encode('utf-8'), callback=delivery_report)
    producer.flush(timeout=5)
    
    if delivered:
        print(f"✓ Produced to topic={topic}, partition={partition}, offset={offset}")
    else:
        print(f"✗ Failed to produce")
        return False
    
    print("\n2. Fetching the record...")
    
    consumer = Consumer({
        'bootstrap.servers': '127.0.0.1:9092',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'group.id': f'test-group-{int(time.time())}',
    })
    
    consumer.subscribe([topic])
    
    messages_found = 0
    start_time = time.time()
    try:
        while time.time() - start_time < 5:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"✗ Consumer error: {msg.error()}")
                    return False
            else:
                value = json.loads(msg.value().decode('utf-8'))
                print(f"✓ Received: topic={msg.topic()}, partition={msg.partition()}, offset={msg.offset()}, value={value}")
                messages_found += 1
                if messages_found >= 1:
                    break
    except Exception as e:
        print(f"✗ Error consuming: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        consumer.close()
    
    return messages_found > 0

def test_empty_topic():
    print("\n3. Testing empty topic fetch...")
    
    topic = f'empty-test-{int(time.time())}'
    
    consumer = Consumer({
        'bootstrap.servers': '127.0.0.1:9092',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'group.id': f'test-group-{int(time.time())}',
    })
    
    consumer.subscribe([topic])
    
    try:
        for i in range(3):
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print("✓ Got expected PARTITION_EOF for empty topic")
                    return True
        print("✓ No messages in empty topic (as expected)")
        return True
    except Exception as e:
        print(f"✗ Error polling empty topic: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        consumer.close()

def test_batch_fetch():
    print("\n4. Testing batch fetch...")
    
    producer = Producer({
        'bootstrap.servers': '127.0.0.1:9092',
    })
    
    topic = f'batch-test-{int(time.time())}'
    
    # Produce 5 records
    delivered = 0
    def delivery_report(err, msg):
        nonlocal delivered
        if err is None:
            delivered += 1
    
    for i in range(5):
        producer.produce(topic, value=json.dumps({'id': i, 'data': f'message-{i}'}).encode('utf-8'), callback=delivery_report)
    
    producer.flush()
    print(f"✓ Produced {delivered} records")
    
    consumer = Consumer({
        'bootstrap.servers': '127.0.0.1:9092',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'group.id': f'test-group-{int(time.time())}',
    })
    
    consumer.subscribe([topic])
    
    try:
        total_records = 0
        start_time = time.time()
        while time.time() - start_time < 5 and total_records < 5:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"✗ Consumer error: {msg.error()}")
                    return False
            else:
                try:
                    value = json.loads(msg.value().decode('utf-8'))
                    print(f"    Record offset={msg.offset()}, value={value}")
                    total_records += 1
                except Exception as e:
                    print(f"    ✗ Failed to decode record at offset {msg.offset()}: {e}")
                    print(f"      Raw bytes: {msg.value()[:50]}...")
                    return False
        
        print(f"✓ Successfully fetched {total_records} records")
        return total_records == 5
    except Exception as e:
        print(f"✗ Error in batch fetch: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        consumer.close()

def main():
    print("=== RecordBatch Debug Test ===\n")
    
    tests = [
        ("Single record fetch", test_single_fetch),
        ("Empty topic fetch", test_empty_topic),
        ("Batch fetch", test_batch_fetch),
    ]
    
    passed = 0
    failed = 0
    
    for name, test_func in tests:
        print(f"\n{'='*50}")
        print(f"Running: {name}")
        print('='*50)
        
        try:
            if test_func():
                passed += 1
                print(f"\n✅ {name} PASSED")
            else:
                failed += 1
                print(f"\n❌ {name} FAILED")
        except Exception as e:
            failed += 1
            print(f"\n❌ {name} FAILED with exception: {e}")
            import traceback
            traceback.print_exc()
    
    print(f"\n{'='*50}")
    print(f"SUMMARY: {passed} passed, {failed} failed")
    print('='*50)
    
    return failed == 0

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)