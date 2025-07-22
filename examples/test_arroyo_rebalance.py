#!/usr/bin/env python3
"""
Test that simulates Arroyo/librdkafka behavior with consumer groups
This should test the REBALANCE_IN_PROGRESS issue we fixed
"""
import sys
import time
import threading
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

def setup_topics():
    """Create topics with 3 partitions each like Snuba"""
    admin = AdminClient({'bootstrap.servers': '127.0.0.1:9092'})
    
    topics = ['outcomes_raw', 'events', 'transactions', 'sessions']
    new_topics = []
    
    for topic in topics:
        new_topics.append(NewTopic(
            topic=topic,
            num_partitions=3,
            replication_factor=1
        ))
    
    try:
        fs = admin.create_topics(new_topics)
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print(f"Created topic {topic}")
            except Exception as e:
                print(f"Topic {topic} might already exist: {e}")
    except Exception as e:
        print(f"Error creating topics: {e}")
    
    return topics

def simulate_arroyo_consumer(consumer_id, topic, group_id, error_log):
    """Simulate Arroyo consumer behavior using librdkafka"""
    print(f"Starting Arroyo consumer {consumer_id} for topic {topic}")
    
    # Arroyo uses these settings
    config = {
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': group_id,
        'client.id': f'arroyo-consumer-{consumer_id}',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'session.timeout.ms': 30000,
        'heartbeat.interval.ms': 3000,
        'max.poll.interval.ms': 300000,
    }
    
    consumer = Consumer(config)
    consumer.subscribe([topic])
    
    messages_consumed = 0
    rebalance_errors = 0
    
    try:
        start_time = time.time()
        while time.time() - start_time < 30:  # Run for 30 seconds
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
                
            if msg.error():
                error_code = msg.error().code()
                if error_code == KafkaError._PARTITION_EOF:
                    continue
                elif error_code == KafkaError.REBALANCE_IN_PROGRESS:
                    rebalance_errors += 1
                    error_log.append(f"Consumer {consumer_id}: REBALANCE_IN_PROGRESS")
                    print(f"Consumer {consumer_id}: REBALANCE_IN_PROGRESS error #{rebalance_errors}")
                else:
                    error_log.append(f"Consumer {consumer_id}: {msg.error()}")
                    print(f"Consumer {consumer_id} error: {msg.error()}")
            else:
                messages_consumed += 1
                if messages_consumed % 10 == 0:
                    print(f"Consumer {consumer_id}: consumed {messages_consumed} messages")
    
    except Exception as e:
        error_log.append(f"Consumer {consumer_id} exception: {e}")
        print(f"Consumer {consumer_id} exception: {e}")
    
    finally:
        consumer.close()
        print(f"Consumer {consumer_id} finished: {messages_consumed} messages, {rebalance_errors} rebalance errors")

def produce_messages(topics):
    """Produce messages to topics"""
    producer = Producer({'bootstrap.servers': '127.0.0.1:9092'})
    
    print("Producing messages...")
    for i in range(100):
        for topic in topics:
            producer.produce(topic, f'message-{i}'.encode('utf-8'), partition=i % 3)
        if i % 10 == 0:
            producer.poll(0)
    
    producer.flush()
    print("Finished producing messages")

def test_arroyo_rebalancing():
    """Test rebalancing with multiple consumer groups like Arroyo"""
    print("Setting up Arroyo rebalancing test...")
    
    topics = setup_topics()
    error_log = []
    
    # Produce some messages
    produce_messages(topics)
    
    # Start multiple consumer groups like Snuba/Arroyo
    threads = []
    consumer_id = 0
    
    # Simulate multiple consumer groups each consuming different topics
    for topic in topics:
        group_id = f'snuba-consumers-{topic}'
        
        # Start 2 consumers per group to trigger rebalancing
        for i in range(2):
            t = threading.Thread(
                target=simulate_arroyo_consumer,
                args=(consumer_id, topic, group_id, error_log)
            )
            t.daemon = True
            t.start()
            threads.append(t)
            consumer_id += 1
            time.sleep(0.5)  # Stagger starts to trigger rebalancing
    
    print(f"\nStarted {len(threads)} consumers across {len(topics)} topics")
    print("Running for 30 seconds...\n")
    
    # Wait for consumers
    for t in threads:
        t.join()
    
    # Check results
    print("\n" + "="*50)
    print("TEST RESULTS")
    print("="*50)
    
    rebalance_errors = [e for e in error_log if "REBALANCE_IN_PROGRESS" in e]
    other_errors = [e for e in error_log if "REBALANCE_IN_PROGRESS" not in e]
    
    print(f"Total REBALANCE_IN_PROGRESS errors: {len(rebalance_errors)}")
    print(f"Other errors: {len(other_errors)}")
    
    if len(rebalance_errors) > 50:  # Threshold for "too many" rebalances
        print("\n❌ FAILED: Too many rebalance errors, possible rebalance loop!")
        for e in other_errors[:5]:  # Show first 5 other errors
            print(f"  {e}")
        return False
    else:
        print("\n✅ PASSED: Rebalancing completed successfully")
        return True

if __name__ == '__main__':
    success = test_arroyo_rebalancing()
    sys.exit(0 if success else 1)