#!/usr/bin/env python3
"""
Test with modern API version to see if throughput improves
"""
from confluent_kafka import Producer, Consumer, KafkaError
import time
import json

# librdkafka automatically negotiates the best API version
# so we'll test throughput directly

topic = f'api-test-{int(time.time())}'

# Skip topic creation - let Rustka auto-create it
# This avoids potential issues with AdminClient

# Produce 50 messages
producer = Producer({
    'bootstrap.servers': '127.0.0.1:9092',
})

delivered = 0
def delivery_report(err, msg):
    global delivered
    if err is None:
        delivered += 1

for i in range(50):
    producer.produce(topic, value=json.dumps({'id': i}).encode('utf-8'), callback=delivery_report)

producer.flush()
print(f"✓ Produced {delivered} messages")

# Test different consumer configurations
configs = [
    {'name': 'Default config', 'config': {}},
    {'name': 'Large batch', 'config': {'fetch.max.bytes': 52428800}},  # 50MB
    {'name': 'Small timeout', 'config': {'fetch.wait.max.ms': 100}},
]

for idx, test_config in enumerate(configs):
    print(f"\n--- Testing {test_config['name']} ---")
    
    # Base consumer config
    config = {
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': f'test-{int(time.time())}-{idx}',
        'auto.offset.reset': 'earliest',
    }
    # Add test-specific config
    config.update(test_config['config'])
    
    consumer = Consumer(config)
    consumer.subscribe([topic])
    
    start_time = time.time()
    consumed = 0
    polls = 0
    
    while consumed < 50 and time.time() - start_time < 5:
        polls += 1
        msg = consumer.poll(0.1)
        
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
        else:
            consumed += 1
    
    elapsed = time.time() - start_time
    rate = consumed / elapsed if elapsed > 0 else 0
    
    consumer.close()
    
    print(f"  Consumed: {consumed}/50 messages")
    print(f"  Time: {elapsed:.2f}s")
    print(f"  Polls: {polls}")
    print(f"  Rate: {rate:.1f} msg/sec")

print("\n✓ librdkafka automatically negotiates optimal API versions")