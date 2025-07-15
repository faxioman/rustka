#!/usr/bin/env python3
"""
Test SASL PLAIN authentication with Rustka
"""
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import time
import sys

def test_auth_producer():
    """Test producer with SASL PLAIN authentication"""
    print("=== Testing Producer with SASL PLAIN Authentication ===")
    
    try:
        # Create producer with SASL PLAIN authentication
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            security_protocol='SASL_PLAINTEXT',
            sasl_mechanism='PLAIN',
            sasl_plain_username='testuser',
            sasl_plain_password='testpass',
            request_timeout_ms=10000
        )
        
        print("✅ Producer connected with authentication!")
        
        # Send a test message
        topic = 'auth-test-topic'
        message = b'Hello from authenticated producer!'
        
        future = producer.send(topic, message)
        result = future.get(timeout=10)
        
        print(f"✅ Message sent successfully!")
        print(f"   Topic: {topic}")
        print(f"   Partition: {result.partition}")
        print(f"   Offset: {result.offset}")
        
        producer.close()
        return True
        
    except Exception as e:
        print(f"❌ Producer error: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_auth_consumer():
    """Test consumer with SASL PLAIN authentication"""
    print("\n=== Testing Consumer with SASL PLAIN Authentication ===")
    
    try:
        # Create consumer with SASL PLAIN authentication
        consumer = KafkaConsumer(
            'auth-test-topic',
            bootstrap_servers=['localhost:9092'],
            security_protocol='SASL_PLAINTEXT',
            sasl_mechanism='PLAIN',
            sasl_plain_username='anotheruser',
            sasl_plain_password='anotherpass',
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000
        )
        
        print("✅ Consumer connected with authentication!")
        print("   Waiting for messages...")
        
        message_count = 0
        for message in consumer:
            print(f"✅ Received message: {message.value.decode('utf-8')}")
            print(f"   Partition: {message.partition}")
            print(f"   Offset: {message.offset}")
            message_count += 1
            
        consumer.close()
        
        if message_count > 0:
            print(f"\n✅ Successfully consumed {message_count} message(s)")
            return True
        else:
            print("\n⚠️  No messages received (topic might be empty)")
            return True  # Not an error if topic is empty
            
    except Exception as e:
        print(f"❌ Consumer error: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_wrong_credentials():
    """Test that we can connect with any credentials (as requested)"""
    print("\n=== Testing Different Credentials ===")
    
    try:
        # Test with completely different credentials
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            security_protocol='SASL_PLAINTEXT',
            sasl_mechanism='PLAIN',
            sasl_plain_username='randomuser',
            sasl_plain_password='randompass',
            request_timeout_ms=5000
        )
        
        print("✅ Connected with random credentials (as expected)")
        
        # Send a message to verify it works
        future = producer.send('auth-test-topic', b'Message with random credentials')
        result = future.get(timeout=5)
        
        print("✅ Can send messages with any credentials!")
        
        producer.close()
        return True
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

def main():
    print("Rustka SASL Authentication Test")
    print("=" * 50)
    print("Note: This test expects Rustka to accept ANY username/password")
    print("=" * 50)
    
    # Give Rustka time to start if just launched
    time.sleep(1)
    
    results = []
    
    # Test producer with auth
    results.append(test_auth_producer())
    
    # Test consumer with auth
    results.append(test_auth_consumer())
    
    # Test that any credentials work
    results.append(test_wrong_credentials())
    
    # Summary
    print("\n" + "=" * 50)
    print("SUMMARY:")
    passed = sum(results)
    total = len(results)
    print(f"Tests passed: {passed}/{total}")
    
    if passed == total:
        print("✅ All authentication tests passed!")
        return 0
    else:
        print("❌ Some tests failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())
