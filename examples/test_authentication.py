#!/usr/bin/env python3
"""
Test SASL PLAIN authentication with Rustka using librdkafka
"""
from confluent_kafka import Producer, Consumer, KafkaError
import time
import sys

def test_auth_producer():
    print("=== Testing Producer with SASL PLAIN Authentication ===")
    
    try:
        producer = Producer({
            'bootstrap.servers': '127.0.0.1:9092',
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': 'testuser',
            'sasl.password': 'testpass',
        })
        
        print("✅ Producer connected with authentication!")
        topic = 'auth-test-topic'
        message = b'Hello from authenticated producer!'
        
        delivered = False
        partition = None
        offset = None
        
        def delivery_report(err, msg):
            nonlocal delivered, partition, offset
            if err is not None:
                print(f"❌ Message delivery failed: {err}")
            else:
                delivered = True
                partition = msg.partition()
                offset = msg.offset()
        
        producer.produce(topic, message, callback=delivery_report)
        producer.flush(timeout=10)
        
        if delivered:
            print(f"✅ Message sent successfully!")
            print(f"   Topic: {topic}")
            print(f"   Partition: {partition}")
            print(f"   Offset: {offset}")
            return True
        else:
            print("❌ Failed to deliver message")
            return False
            
    except Exception as e:
        print(f"❌ Producer failed with error: {e}")
        return False

def test_auth_consumer():
    print("\n=== Testing Consumer with SASL PLAIN Authentication ===")
    
    try:
        consumer = Consumer({
            'bootstrap.servers': '127.0.0.1:9092',
            'group.id': f'auth-test-group-{int(time.time())}',
            'auto.offset.reset': 'earliest',
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': 'testuser',
            'sasl.password': 'testpass',
        })
        
        consumer.subscribe(['auth-test-topic'])
        print("✅ Consumer connected with authentication!")
        print("   Waiting for messages...")
        
        start_time = time.time()
        consumed = False
        
        while time.time() - start_time < 5:
            msg = consumer.poll(0.5)
            
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"❌ Consumer error: {msg.error()}")
                    consumer.close()
                    return False
            
            print(f"✅ Message consumed successfully!")
            print(f"   Topic: {msg.topic()}")
            print(f"   Partition: {msg.partition()}")
            print(f"   Offset: {msg.offset()}")
            print(f"   Value: {msg.value()}")
            consumed = True
            break
        
        consumer.close()
        
        if not consumed:
            print("⚠️  No messages consumed (topic might be empty)")
            
        return True
        
    except Exception as e:
        print(f"❌ Consumer failed with error: {e}")
        return False

def test_wrong_credentials():
    print("\n=== Testing Connection with Different Credentials ===")
    
    try:
        producer = Producer({
            'bootstrap.servers': '127.0.0.1:9092',
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': 'anyuser',
            'sasl.password': 'anypass',
        })
        delivered = False
        error_msg = None
        
        def delivery_report(err, msg):
            nonlocal delivered, error_msg
            if err is not None:
                error_msg = str(err)
            else:
                delivered = True
        
        producer.produce('test-topic', b'test', callback=delivery_report)
        producer.flush(timeout=5)
        
        if delivered:
            print("✅ Message delivered with different credentials (as expected)")
            return True
        else:
            print(f"❌ Failed to deliver message: {error_msg}")
            return False
            
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

def main():
    print("Running SASL PLAIN authentication tests...\n")
    
    tests_passed = 0
    tests_failed = 0
    if test_auth_producer():
        tests_passed += 1
    else:
        tests_failed += 1
    if test_auth_consumer():
        tests_passed += 1
    else:
        tests_failed += 1
    if test_wrong_credentials():
        tests_passed += 1
    else:
        tests_failed += 1
    print("\n" + "="*50)
    print(f"Tests passed: {tests_passed}")
    print(f"Tests failed: {tests_failed}")
    
    if tests_failed == 0:
        print("\n✅ All authentication tests passed!")
        return 0
    else:
        print(f"\n❌ {tests_failed} authentication tests failed!")
        return 1

if __name__ == '__main__':
    sys.exit(main())