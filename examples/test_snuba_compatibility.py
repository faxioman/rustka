#!/usr/bin/env python3
"""
Test that verifies __commit_log messages are compatible with Snuba's expectations
"""
from kafka import KafkaProducer, KafkaConsumer
import time
import json
import struct

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
    
    # Produce some messages
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    for i in range(5):
        producer.send(topic, value={'event_id': i, 'data': f'event-{i}'})
    producer.flush()
    producer.close()
    print(f"✓ Produced 5 messages to {topic}")
    
    # Create consumer and commit offsets
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        group_id=group_id,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        consumer_timeout_ms=1000
    )
    
    consumed_count = 0
    for msg in consumer:
        consumed_count += 1
        if consumed_count >= 5:
            break
    
    # Commit the offset
    consumer.commit()
    consumer.close()
    print(f"✓ Consumed and committed offsets for group {group_id}")
    
    # Now read from __commit_log and verify format
    commit_log_consumer = KafkaConsumer(
        '__commit_log',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        consumer_timeout_ms=2000
    )
    
    found_commits = 0
    valid_format = True
    
    for message in commit_log_consumer:
        if message.key:
            key_data = parse_commit_log_key(message.key)
            if key_data and key_data.get('group_id') == group_id:
                found_commits += 1
                print(f"\n✓ Found commit for our group:")
                print(f"  Key parsed: {key_data}")
                
                value_data = parse_commit_log_value(message.value)
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
