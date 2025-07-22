#!/usr/bin/env python3
"""
Test that verifies __commit_log messages are compatible with Snuba's expectations
"""
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
import time
import json
import struct

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

def parse_commit_log_key(key_bytes):
    """Parse the commit log key format"""
    if not key_bytes or len(key_bytes) < 8:
        return None
    
    offset = 0
    result = {}
    
    try:
        # Version (2 bytes)
        version = struct.unpack('>H', key_bytes[offset:offset+2])[0]
        offset += 2
        result['version'] = version
        
        # Group ID length (2 bytes) + Group ID
        group_len = struct.unpack('>H', key_bytes[offset:offset+2])[0]
        offset += 2
        result['group_id'] = key_bytes[offset:offset+group_len].decode('utf-8')
        offset += group_len
        
        # Topic length (2 bytes) + Topic
        topic_len = struct.unpack('>H', key_bytes[offset:offset+2])[0]
        offset += 2
        result['topic'] = key_bytes[offset:offset+topic_len].decode('utf-8')
        offset += topic_len
        
        # Partition (4 bytes)
        result['partition'] = struct.unpack('>I', key_bytes[offset:offset+4])[0]
        
        return result
    except Exception as e:
        print(f"Error parsing key: {e}")
        return None

def parse_commit_log_value(value_bytes):
    """Parse the commit log value format"""
    if not value_bytes or len(value_bytes) < 4:
        return None
    
    offset = 0
    result = {}
    
    try:
        # Version (2 bytes)
        version = struct.unpack('>H', value_bytes[offset:offset+2])[0]
        offset += 2
        result['version'] = version
        
        # Offset (8 bytes)
        result['offset'] = struct.unpack('>q', value_bytes[offset:offset+8])[0]
        offset += 8
        
        # Metadata length (2 bytes) + Metadata
        metadata_len = struct.unpack('>H', value_bytes[offset:offset+2])[0]
        offset += 2
        if metadata_len > 0:
            result['metadata'] = value_bytes[offset:offset+metadata_len].decode('utf-8')
            offset += metadata_len
        else:
            result['metadata'] = ''
        
        # Timestamp (8 bytes)
        result['timestamp'] = struct.unpack('>q', value_bytes[offset:offset+8])[0]
        
        return result
    except Exception as e:
        print(f"Error parsing value: {e}")
        return None

def test_snuba_compatibility():
    print("Testing Snuba compatibility for __commit_log...")
    
    # Create a unique group ID
    group_id = f'snuba-test-{int(time.time())}'
    topic = 'test-events'
    
    # Ensure topic exists
    ensure_topic(topic)
    
    # Produce some messages
    producer = Producer({
        'bootstrap.servers': '127.0.0.1:9092',
    })
    
    delivered = 0
    def delivery_report(err, msg):
        nonlocal delivered
        if err is None:
            delivered += 1
    
    for i in range(5):
        producer.produce(topic, value=json.dumps({'event_id': i, 'data': f'event-{i}'}).encode('utf-8'), callback=delivery_report)
    producer.flush()
    print(f"✓ Produced {delivered} messages to {topic}")
    
    # Create consumer and commit offsets
    consumer = Consumer({
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    })
    
    consumer.subscribe([topic])
    
    consumed_count = 0
    start_time = time.time()
    while consumed_count < 5 and time.time() - start_time < 5:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
        else:
            consumed_count += 1
    
    # Commit the offset
    consumer.commit()
    consumer.close()
    print(f"✓ Consumed {consumed_count} and committed offsets for group {group_id}")
    
    # Now read from __commit_log and verify format
    commit_log_consumer = Consumer({
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': f'commit-log-reader-{int(time.time())}',
        'auto.offset.reset': 'earliest',
    })
    
    commit_log_consumer.subscribe(['__commit_log'])
    
    found_commits = 0
    valid_format = True
    
    start_time = time.time()
    while time.time() - start_time < 5:
        msg = commit_log_consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
        else:
            if msg.key():
                key_data = parse_commit_log_key(msg.key())
                if key_data and key_data.get('group_id') == group_id:
                    found_commits += 1
                    print(f"\n✓ Found commit for our group:")
                    print(f"  Key parsed: {key_data}")
                    
                    value_data = parse_commit_log_value(msg.value())
                    if value_data:
                        print(f"  Value parsed: {value_data}")
                        
                        # Verify the format matches Snuba's expectations
                        if key_data['version'] != 1 or value_data['version'] != 1:
                            print(f"  ✗ Version mismatch: key_v={key_data['version']}, value_v={value_data['version']}")
                            valid_format = False
                        
                        if value_data['offset'] < 0:
                            print(f"  ✗ Invalid offset: {value_data['offset']}")
                            valid_format = False
                    else:
                        print(f"  ✗ Could not parse value")
                        valid_format = False
    
    commit_log_consumer.close()
    
    print(f"\n{'='*60}")
    if found_commits > 0 and valid_format:
        print(f"✅ SUCCESS: Found {found_commits} properly formatted commits")
        print("✅ Format is compatible with Snuba's expectations")
        return True
    elif found_commits > 0:
        print(f"❌ FAILED: Found {found_commits} commits but format issues detected")
        return False
    else:
        print("❌ FAILED: No commits found for our group")
        return False

if __name__ == "__main__":
    # Wait a bit for Rustka to be ready
    time.sleep(1)
    
    success = test_snuba_compatibility()
    exit(0 if success else 1)