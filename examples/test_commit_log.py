#!/usr/bin/env python3
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
import time
import json

def ensure_topic(topic_name):
    admin = AdminClient({'bootstrap.servers': '127.0.0.1:9092'})
    
    try:
        topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
        fs = admin.create_topics([topic])
        for topic, f in fs.items():
            try:
                f.result()
            except Exception as e:
                pass
    except Exception as e:
        pass

def test_commit_log():
    print("Testing commit log functionality...")
    
    group_id = f'test-commit-log-{int(time.time())}'
    topic = 'test-topic'
    ensure_topic(topic)
    
    consumer = Consumer({
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    })
    
    consumer.subscribe([topic])
    consumer.poll(timeout=0.1)
    producer = Producer({
        'bootstrap.servers': '127.0.0.1:9092',
    })
    
    delivered = False
    def delivery_report(err, msg):
        nonlocal delivered
        if err is None:
            delivered = True
    
    producer.produce(topic, value=json.dumps({'test': 'message'}).encode('utf-8'), callback=delivery_report)
    producer.flush()
    
    if not delivered:
        print("✗ Failed to deliver message")
        return False
    consumed_offset = None
    start_time = time.time()
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
            consumed_offset = msg.offset()
            print(f"✓ Consumed message at offset {consumed_offset}")
            break
    
    if consumed_offset is None:
        print("✗ Failed to consume message")
        return False
    consumer.commit()
    print(f"✓ Committed offsets for group {group_id}")
    commit_log_consumer = Consumer({
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': f'commit-log-reader-{int(time.time())}',
        'auto.offset.reset': 'earliest',
    })
    
    commit_log_consumer.subscribe(['__commit_log'])
    
    found_our_commit = False
    start_time = time.time()
    while time.time() - start_time < 5:
        msg = commit_log_consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                continue
        
        if msg.key() is not None:
            print(f"✓ Found message with key: {len(msg.key())} bytes")
            if len(msg.key()) >= 8:
                key_str = msg.key().hex()
                if group_id.encode().hex() in key_str:
                    print(f"✓ Found our commit message with proper key!")
                    found_our_commit = True
                    break
        else:
            print(f"✗ Found message with key=None at offset {msg.offset()}")
    
    consumer.close()
    commit_log_consumer.close()
    
    if found_our_commit:
        print("\n✅ SUCCESS: Commit log messages now have proper keys!")
        return True
    else:
        print("\n❌ FAILED: Could not find commit message with proper key")
        return False

if __name__ == "__main__":
    time.sleep(1)
    
    success = test_commit_log()
    exit(0 if success else 1)