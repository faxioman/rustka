#!/usr/bin/env python3

import time
import struct
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

def ensure_topic(topic_name):
    admin = AdminClient({'bootstrap.servers': '127.0.0.1:9092'})
    
    try:
        topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
        fs = admin.create_topics([topic])
        for topic, f in fs.items():
            try:
                f.result()
                print(f"Created topic {topic}")
            except Exception as e:
                pass
    except Exception as e:
        pass

def test_headers():
    print("\n=== Testing Kafka Headers Support ===")
    print("\n1. Testing basic headers...")
    producer = Producer({
        'bootstrap.servers': '127.0.0.1:9092',
    })
    
    headers = [
        ('version', b'1.0'),
        ('type', b'event'),
        ('timestamp', str(int(time.time())).encode()),
    ]
    
    ensure_topic('test-headers-basic')
    
    delivered = False
    error_msg = None
    
    def delivery_report(err, msg):
        nonlocal delivered, error_msg
        if err is not None:
            error_msg = str(err)
        else:
            delivered = True
    
    producer.produce('test-headers-basic', value=b'Basic test', headers=headers, callback=delivery_report)
    producer.flush(timeout=10)
    
    if delivered:
        print("✓ Basic headers sent successfully")
    else:
        print(f"✗ Failed to send basic headers: {error_msg}")
        return False
    print("\n2. Testing Sentry-style headers...")
    producer = Producer({
        'bootstrap.servers': '127.0.0.1:9092',
    })
    
    sentry_headers = [
        ('version', struct.pack('>i', 2)),
        ('type', b'event'),
        ('project_id', struct.pack('>q', 1)),
    ]
    
    ensure_topic('test-headers-sentry')
    
    delivered = False
    error_msg = None
    
    def delivery_report2(err, msg):
        nonlocal delivered, error_msg
        if err is not None:
            error_msg = str(err)
        else:
            delivered = True
    
    producer.produce('test-headers-sentry', value=b'{"event": "test"}', headers=sentry_headers, callback=delivery_report2)
    producer.flush(timeout=10)
    
    if delivered:
        print("✓ Sentry-style headers sent successfully")
    else:
        print(f"✗ Failed to send Sentry headers: {error_msg}")
        return False
    print("\n3. Testing headers with old consumer API...")
    producer = Producer({
        'bootstrap.servers': '127.0.0.1:9092',
    })
    
    ensure_topic('test-headers-old-api')
    
    delivered = False
    error_msg = None
    
    def delivery_report3(err, msg):
        nonlocal delivered, error_msg
        if err is not None:
            error_msg = str(err)
        else:
            delivered = True
    
    producer.produce('test-headers-old-api', value=b'Old API test', 
                     headers=[('version', struct.pack('>i', 1))], callback=delivery_report3)
    producer.flush(timeout=10)
    
    if delivered:
        print("✓ Message sent for old API test")
    else:
        print(f"✗ Failed to send for old API test: {error_msg}")
        return False
    print("\n4. Verifying headers...")
    all_passed = True
    consumer = Consumer({
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': f'test-headers-group-{int(time.time())}',
        'auto.offset.reset': 'earliest',
    })
    
    consumer.subscribe(['test-headers-basic'])
    
    found = False
    start_time = time.time()
    while time.time() - start_time < 3:
        msg = consumer.poll(0.5)
        if msg and not msg.error() and msg.headers():
            header_dict = dict(msg.headers())
            if 'version' in header_dict:
                found = True
                break
    consumer.close()
    
    if found:
        print("✓ Basic headers verified")
    else:
        print("✗ Basic headers not found")
        all_passed = False
    consumer = Consumer({
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': f'test-headers-sentry-group-{int(time.time())}',
        'auto.offset.reset': 'earliest',
    })
    
    consumer.subscribe(['test-headers-sentry'])
    
    found = False
    start_time = time.time()
    while time.time() - start_time < 3:
        msg = consumer.poll(0.5)
        if msg and not msg.error() and msg.headers():
            header_dict = dict(msg.headers())
            version_bytes = header_dict.get('version')
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
    consumer = Consumer({
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': f'test-headers-old-api-group-{int(time.time())}',
        'auto.offset.reset': 'earliest',
    })
    
    consumer.subscribe(['test-headers-old-api'])
    
    found = False
    start_time = time.time()
    while time.time() - start_time < 3:
        msg = consumer.poll(0.5)
        if msg and not msg.error() and msg.headers():
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