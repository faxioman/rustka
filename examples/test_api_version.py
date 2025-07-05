#!/usr/bin/env python3
"""
Test with different API versions to understand the format
"""
import time
from kafka import KafkaProducer, KafkaConsumer

def test_with_api_version(api_version):
    print(f"\n=== Testing with API version {api_version} ===")
    
    # Use unique topic for each test run to avoid reading old messages
    topic = f'test-api-version-{int(time.time() * 1000)}'
    
    # Producer
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        api_version=api_version
    )
    
    # Send a few messages
    for i in range(3):
        producer.send(topic, value=f'message-{i}-v{api_version[0]}.{api_version[1]}'.encode('utf-8'))
    
    producer.flush()
    producer.close()
    print(f"Produced 3 messages with API v{api_version[0]}.{api_version[1]}")
    
    # Consumer
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        group_id=f'test-{int(time.time())}',
        auto_offset_reset='earliest',
        api_version=api_version,
        consumer_timeout_ms=2000,
        enable_auto_commit=False  # Disable auto commit to avoid hanging
    )
    
    count = 0
    try:
        for message in consumer:
            count += 1
            print(f"Consumed: {message.value}")
            if count >= 3:
                break
    except StopIteration:
        pass  # Consumer timeout is expected
    finally:
        try:
            consumer.close(autocommit=False)  # Don't autocommit on close
        except:
            pass  # Ignore any errors on close
    
    print(f"Consumed {count} messages")

if __name__ == '__main__':
    # Test with different API versions
    # API v0.8.2
    test_with_api_version((0, 8, 2))
    
    # API v0.10.0
    test_with_api_version((0, 10, 0))
    
    # API v0.10.1
    test_with_api_version((0, 10, 1))
