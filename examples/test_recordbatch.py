#!/usr/bin/env python3
"""
Test to debug RecordBatch format issues
"""
import sys
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import json
import time

def test_single_fetch():
    """Test fetching a single record"""
    print("1. Producing a single test record...")
    
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(0, 10, 0)
    )
    
    topic = f'test-{int(time.time())}'
    test_value = {'test': 'message', 'id': 1}
    
    future = producer.send(topic, value=test_value)
    try:
        record_metadata = future.get(timeout=10)
        print(f"✓ Produced to topic={record_metadata.topic}, partition={record_metadata.partition}, offset={record_metadata.offset}")
    except KafkaError as e:
        print(f"✗ Failed to produce: {e}")
        return False
    
    producer.close()
    
    print("\n2. Fetching the record...")
    
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id=f'test-group-{int(time.time())}',
        consumer_timeout_ms=5000,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
        api_version=(0, 10, 0)
    )
    
    messages_found = 0
    try:
        for message in consumer:
            print(f"✓ Received: topic={message.topic}, partition={message.partition}, offset={message.offset}, value={message.value}")
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
    """Test fetching from an empty topic"""
    print("\n3. Testing empty topic fetch...")
    
    topic = f'empty-test-{int(time.time())}'
    
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id=f'test-group-{int(time.time())}',
        consumer_timeout_ms=2000,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
        api_version=(0, 10, 0)
    )
    
    try:
        messages = consumer.poll(timeout_ms=1000, max_records=10)
        print(f"✓ Poll returned: {len(messages)} topic partitions")
        for tp, records in messages.items():
            print(f"  - {tp}: {len(records)} records")
    except Exception as e:
        print(f"✗ Error polling empty topic: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        consumer.close()
    
    return True

def test_batch_fetch():
    """Test fetching multiple records"""
    print("\n4. Testing batch fetch...")
    
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(0, 10, 0)
    )
    
    topic = f'batch-test-{int(time.time())}'
    
    # Produce 5 records
    for i in range(5):
        producer.send(topic, value={'id': i, 'data': f'message-{i}'})
    
    producer.flush()
    producer.close()
    
    print("✓ Produced 5 records")
    
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id=f'test-group-{int(time.time())}',
        consumer_timeout_ms=5000,
        max_poll_records=10,
        api_version=(0, 10, 0)
    )
    
    try:
        messages = consumer.poll(timeout_ms=2000, max_records=10)
        total_records = 0
        for tp, records in messages.items():
            print(f"  - {tp}: {len(records)} records")
            for record in records:
                try:
                    value = json.loads(record.value.decode('utf-8'))
                    print(f"    Record offset={record.offset}, value={value}")
                    total_records += 1
                except Exception as e:
                    print(f"    ✗ Failed to decode record at offset {record.offset}: {e}")
                    print(f"      Raw bytes: {record.value[:50]}...")
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
