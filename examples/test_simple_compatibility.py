#!/usr/bin/env python3
"""
Simple Kafka compatibility test
"""
import sys

try:
    from confluent_kafka import Producer, Consumer, KafkaError, TopicPartition
    from confluent_kafka.admin import AdminClient, NewTopic
    print("✓ confluent-kafka installed")
except ImportError:
    print("✗ confluent-kafka not installed. Please run: pip install confluent-kafka")
    sys.exit(1)

import json
import time

def ensure_topic(topic_name):
    """Ensure topic exists"""
    admin = AdminClient({'bootstrap.servers': '127.0.0.1:9092'})
    
    try:
        new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
        fs = admin.create_topics([new_topic])
        for topic, f in fs.items():
            try:
                f.result()
            except Exception as e:
                pass  # Topic might already exist
    except Exception as e:
        pass

def test_basic_produce_consume():
    """Basic produce/consume test"""
    print("\n1. Testing basic produce...")
    
    # Ensure topic exists
    ensure_topic('test-topic')
    
    try:
        producer = Producer({
            'bootstrap.servers': '127.0.0.1:9092',
        })
        
        delivered = False
        partition = None
        offset = None
        
        def delivery_report(err, msg):
            nonlocal delivered, partition, offset
            if err is None:
                delivered = True
                partition = msg.partition()
                offset = msg.offset()
        
        producer.produce('test-topic', b'Hello Rustka!', callback=delivery_report)
        producer.flush(timeout=5)
        
        if delivered:
            print(f"✓ Produced message to partition {partition} offset {offset}")
        else:
            print("✗ Failed to deliver message")
            return False
        
    except Exception as e:
        print(f"✗ Producer failed: {e}")
        return False
    
    print("\n2. Testing basic consume...")
    
    try:
        consumer = Consumer({
            'bootstrap.servers': '127.0.0.1:9092',
            'group.id': f'test-basic-{int(time.time())}',
            'auto.offset.reset': 'earliest',
        })
        
        consumer.subscribe(['test-topic'])
        
        message_found = False
        start_time = time.time()
        while time.time() - start_time < 5:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
            elif msg.value() == b'Hello Rustka!':
                print(f"✓ Consumed message: {msg.value().decode()}")
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
        
        # Ensure topic exists
        ensure_topic(topic_name)
        
        # Produce a message first
        producer = Producer({
            'bootstrap.servers': '127.0.0.1:9092',
        })
        
        test_msg = f'Group test at {time.time()}'
        producer.produce(topic_name, test_msg.encode())
        producer.flush()
        
        # Consumer with group
        consumer = Consumer({
            'bootstrap.servers': '127.0.0.1:9092',
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
        })
        
        consumer.subscribe([topic_name])
        print(f"✓ Joined consumer group '{group_id}'")
        
        # Try to consume
        messages_consumed = 0
        start_time = time.time()
        while time.time() - start_time < 5:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
            else:
                messages_consumed += 1
                print(f"✓ Consumed {messages_consumed} messages in group")
                break
        
        consumer.close()
        
        if messages_consumed > 0:
            return True
        else:
            print("✗ No messages consumed")
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
        consumer1 = Consumer({
            'bootstrap.servers': '127.0.0.1:9092',
            'group.id': group_id,
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest',
        })
        
        consumer1.subscribe(['test-topic'])
        
        # Read one message and commit
        committed_offset = None
        partition_used = None
        start_time = time.time()
        while time.time() - start_time < 5:
            msg = consumer1.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
            else:
                print(f"✓ Consumer 1 read offset {msg.offset()}")
                committed_offset = msg.offset()
                partition_used = msg.partition()
                consumer1.commit()
                break
        
        consumer1.close()
        
        if committed_offset is None:
            print("✗ No message to commit")
            return False
        
        # Second consumer: should start after committed offset
        consumer2 = Consumer({
            'bootstrap.servers': '127.0.0.1:9092',
            'group.id': group_id,
            'enable.auto.commit': False,
        })
        
        consumer2.subscribe(['test-topic'])
        
        # Poll to trigger assignment
        consumer2.poll(timeout=1.0)
        
        # Check committed offset
        assignment = consumer2.assignment()
        if assignment:
            for partition in assignment:
                if partition.partition == partition_used:
                    committed_list = consumer2.committed([partition])
                    if committed_list and committed_list[0].offset >= 0:
                        print(f"✓ Consumer 2 sees committed offset: {committed_list[0].offset}")
                        consumer2.close()
                        return True
            
            print("✗ No committed offset found for partition")
        else:
            print("✗ No assignment for consumer 2")
        
        consumer2.close()
        return False
        
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