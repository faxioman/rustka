#!/usr/bin/env python3
"""
Simple Kafka compatibility test
"""
import sys

try:
    from kafka import KafkaProducer, KafkaConsumer
    print("✓ kafka-python installed")
except ImportError:
    print("✗ kafka-python not installed. Please run: pip install kafka-python")
    sys.exit(1)

import json
import time

def test_basic_produce_consume():
    """Basic produce/consume test"""
    print("\n1. Testing basic produce...")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            api_version=(0, 10, 0),
            request_timeout_ms=5000
        )
        
        future = producer.send('test-topic', b'Hello Rustka!')
        result = future.get(timeout=5)
        print(f"✓ Produced message to partition {result.partition} offset {result.offset}")
        producer.close()
        
    except Exception as e:
        print(f"✗ Producer failed: {e}")
        return False
    
    print("\n2. Testing basic consume...")
    
    try:
        consumer = KafkaConsumer(
            'test-topic',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000,
            api_version=(0, 10, 0)
        )
        
        message_found = False
        for message in consumer:
            if message.value == b'Hello Rustka!':
                print(f"✓ Consumed message: {message.value.decode()}")
                message_found = True
                break
        
        consumer.close()
        
        if not message_found:
            print("✗ Message not found")
            return False
            
    except Exception as e:
        print(f"✗ Consumer failed: {e}")
        return False
    
    return True

def test_consumer_group():
    """Test consumer group base"""
    print("\n3. Testing consumer groups...")
    
    try:
        # Use unique topic for this test
        topic_name = f'test-cg-topic-{int(time.time())}'
        group_id = f'test-group-{int(time.time())}'
        
        # Produce a message first
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            api_version=(0, 10, 0)
        )
        
        test_msg = f'Group test at {time.time()}'
        producer.send(topic_name, test_msg.encode())
        producer.flush()
        producer.close()
        
        # Consumer with group
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=['localhost:9092'],
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            api_version=(0, 10, 0)
        )
        
        print(f"✓ Joined consumer group '{group_id}'")
        
        # Try to consume
        messages = consumer.poll(timeout_ms=5000)
        if messages:
            print(f"✓ Consumed {sum(len(msgs) for msgs in messages.values())} messages in group")
            consumer.close()
            return True
        else:
            print("✗ No messages consumed")
            consumer.close()
            return False
        
    except Exception as e:
        print(f"✗ Consumer group failed: {e}")
        return False

def test_offset_management():
    """Test offset commit/fetch"""
    print("\n4. Testing offset management...")
    
    try:
        group_id = f'test-offset-group-{int(time.time())}'
        
        # First consumer: read and commit
        consumer1 = KafkaConsumer(
            'test-topic',
            bootstrap_servers=['localhost:9092'],
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset='earliest',
            api_version=(0, 10, 0)
        )
        
        # Read one message and commit
        for message in consumer1:
            print(f"✓ Consumer 1 read offset {message.offset}")
            consumer1.commit()
            break
        
        consumer1.close()
        
        # Second consumer: should start after committed offset
        consumer2 = KafkaConsumer(
            'test-topic',
            bootstrap_servers=['localhost:9092'],
            group_id=group_id,
            enable_auto_commit=False,
            api_version=(0, 10, 0)
        )
        
        # Poll to trigger assignment
        consumer2.poll(timeout_ms=1000)
        
        # Check committed offset
        for partition in consumer2.assignment():
            committed = consumer2.committed(partition)
            if committed is not None:
                print(f"✓ Consumer 2 sees committed offset: {committed}")
            else:
                print("✗ No committed offset found")
        
        consumer2.close()
        return True
        
    except Exception as e:
        print(f"✗ Offset management failed: {e}")
        return False

def main():
    print("=== Rustka Compatibility Test ===")
    print("Testing if Rustka behaves like Kafka...\n")
    
    tests = [
        ("Basic Produce/Consume", test_basic_produce_consume),
        ("Consumer Groups", test_consumer_group),
        ("Offset Management", test_offset_management),
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
    
    print(f"\n{'='*50}")
    print(f"SUMMARY: {passed} passed, {failed} failed")
    print('='*50)
    
    return failed == 0

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
