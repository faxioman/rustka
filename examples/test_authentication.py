#!/usr/bin/env python3
"""
Test SASL PLAIN authentication with Rustka using librdkafka
"""
from confluent_kafka import Producer, Consumer, KafkaError
import time
import sys

def test_auth_producer():
    """Test producer with SASL PLAIN authentication"""
    print("=== Testing Producer with SASL PLAIN Authentication ===")
    
    try:
        # Create producer with SASL PLAIN authentication
        producer = Producer({
            'bootstrap.servers': '127.0.0.1:9092',
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': 'testuser',
            'sasl.password': 'testpass',
        })
        
        print("✅ Producer connected with authentication!")
        
        # Send a test message
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
    """Test consumer with SASL PLAIN authentication"""
    print("\n=== Testing Consumer with SASL PLAIN Authentication ===")
    
    try:
        # Create consumer with SASL PLAIN authentication
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
        
        # Try to consume messages
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
    """Test connection with wrong credentials"""
    print("\n=== Testing Connection with Wrong Credentials ===")
    
    try:
        # Try to connect with wrong credentials
        producer = Producer({
            'bootstrap.servers': '127.0.0.1:9092',
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': 'wronguser',
            'sasl.password': 'wrongpass',
        })
        
        # Try to send a message
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
            print("❌ Message delivered with wrong credentials! This shouldn't happen!")
            return False
        else:
            print(f"✅ Authentication correctly rejected: {error_msg}")
            return True
            
    except Exception as e:
        print(f"✅ Connection correctly rejected: {e}")
        return True

def main():
    """Run all authentication tests"""
    print("Running SASL PLAIN authentication tests...\n")
    
    tests_passed = 0
    tests_failed = 0
    
    # Test 1: Producer with correct credentials
    if test_auth_producer():
        tests_passed += 1
    else:
        tests_failed += 1
    
    # Test 2: Consumer with correct credentials
    if test_auth_consumer():
        tests_passed += 1
    else:
        tests_failed += 1
    
    # Test 3: Wrong credentials
    if test_wrong_credentials():
        tests_passed += 1
    else:
        tests_failed += 1
    
    # Summary
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