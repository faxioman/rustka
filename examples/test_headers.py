#!/usr/bin/env python3
"""Test Kafka message headers functionality"""

import time
import struct
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

def test_headers():
    """Test complete headers functionality"""
    print("\n=== Testing Kafka Headers Support ===")
    
    # Test 1: Basic headers with new API
    print("\n1. Testing basic headers...")
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        api_version=(2, 1, 0),
    )
    
    headers = [
        ('version', b'1.0'),
        ('type', b'event'),
        ('timestamp', str(int(time.time())).encode()),
    ]
    
    try:
        future = producer.send('test-headers-basic', value=b'Basic test', headers=headers)
        future.get(timeout=10)
        print("✓ Basic headers sent successfully")
    except KafkaError as e:
        print(f"✗ Failed to send basic headers: {e}")
        return False
    finally:
        producer.close()
    
    # Test 2: Sentry-style headers (version as 4-byte int)
    print("\n2. Testing Sentry-style headers...")
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        api_version=(2, 0, 0),
    )
    
    sentry_headers = [
        ('version', struct.pack('>i', 2)),
        ('type', b'event'),
        ('project_id', struct.pack('>q', 1)),
    ]
    
    try:
        future = producer.send('test-headers-sentry', value=b'{"event": "test"}', headers=sentry_headers)
        future.get(timeout=10)
        print("✓ Sentry-style headers sent successfully")
    except KafkaError as e:
        print(f"✗ Failed to send Sentry headers: {e}")
        return False
    finally:
        producer.close()
    
    # Test 3: Headers with old fetch API (regression test for Sentry)
    print("\n3. Testing headers with old consumer API...")
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        api_version=(2, 1, 0),
    )
    
    try:
        future = producer.send('test-headers-old-api', value=b'Old API test', 
                             headers=[('version', struct.pack('>i', 1))])
        future.get(timeout=10)
    except KafkaError as e:
        print(f"✗ Failed to send for old API test: {e}")
        return False
    finally:
        producer.close()
    
    # Verify all messages with headers
    print("\n4. Verifying headers...")
    all_passed = True
    
    # Check basic headers
    consumer = KafkaConsumer(
        'test-headers-basic',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        api_version=(2, 1, 0),
        consumer_timeout_ms=3000,
    )
    
    found = False
    for msg in consumer:
        if msg.headers:
            header_dict = dict(msg.headers)
            if 'version' in header_dict or b'version' in header_dict:
                found = True
                break
    consumer.close()
    
    if found:
        print("✓ Basic headers verified")
    else:
        print("✗ Basic headers not found")
        all_passed = False
    
    # Check Sentry headers
    consumer = KafkaConsumer(
        'test-headers-sentry',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        api_version=(2, 0, 0),
        consumer_timeout_ms=3000,
    )
    
    found = False
    for msg in consumer:
        if msg.headers:
            header_dict = dict(msg.headers)
            version_bytes = header_dict.get('version') or header_dict.get(b'version')
            if version_bytes:
                version = struct.unpack('>i', version_bytes)[0]
                if version == 2:
                    found = True
                    break
    consumer.close()
    
    if found:
        print("✓ Sentry headers verified (version=2)")
    else:
        print("✗ Sentry headers not found")
        all_passed = False
    
    # Check old API compatibility
    consumer = KafkaConsumer(
        'test-headers-old-api',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        api_version=(1, 0, 0),  # OLD API
        consumer_timeout_ms=3000,
    )
    
    found = False
    for msg in consumer:
        if msg.headers:
            found = True
            break
    consumer.close()
    
    if found:
        print("✓ Headers work with old consumer API")
    else:
        print("✗ Headers missing with old consumer API")
        all_passed = False
    
    return all_passed

if __name__ == "__main__":
    success = test_headers()
    if success:
        print("\n=== Headers Test: PASSED ✓ ===")
    else:
        print("\n=== Headers Test: FAILED ✗ ===")
        exit(1)
