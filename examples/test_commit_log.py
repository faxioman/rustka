#!/usr/bin/env python3
"""
Test that verifies commit log messages have proper keys
"""
from kafka import KafkaProducer, KafkaConsumer
import time
import json

def test_commit_log():
    print("Testing commit log functionality...")
    
    group_id = f'test-commit-log-{int(time.time())}'
    topic = 'test-topic'
    
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        group_id=group_id,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        consumer_timeout_ms=1000
    )
    
    consumer.poll(timeout_ms=100)
    
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    producer.send(topic, value={'test': 'message'})
    producer.flush()
    producer.close()
    
    # Consume the message
    for msg in consumer:
        print(f"✓ Consumed message at offset {msg.offset}")
        break
    
    consumer.commit()
    print(f"✓ Committed offsets for group {group_id}")
    
    commit_log_consumer = KafkaConsumer(
        '__commit_log',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        consumer_timeout_ms=2000
    )
    
    found_our_commit = False
    for message in commit_log_consumer:
        if message.key is not None:
            print(f"✓ Found message with key: {len(message.key)} bytes")
            if len(message.key) >= 8:
                key_str = message.key.hex()
                if group_id.encode().hex() in key_str:
                    print(f"✓ Found our commit message with proper key!")
                    found_our_commit = True
                    break
        else:
            print(f"✗ Found message with key=None at offset {message.offset}")
    
    consumer.close()
    commit_log_consumer.close()
    
    if found_our_commit:
        print("\n✅ SUCCESS: Commit log messages now have proper keys!")
        return True
    else:
        print("\n❌ FAILED: Could not find commit message with proper key")
        return False

if __name__ == "__main__":
    # Wait a bit for Rustka to start
    time.sleep(1)
    
    success = test_commit_log()
    exit(0 if success else 1)
