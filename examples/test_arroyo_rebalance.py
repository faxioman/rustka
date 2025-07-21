#!/usr/bin/env python3
"""
Test that simulates Arroyo/librdkafka behavior with consumer groups
This should reproduce the REBALANCE_IN_PROGRESS loop issue
"""
import sys
import time
import threading
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError

def setup_topics():
    """Create topics with 3 partitions each like Snuba"""
    admin = KafkaAdminClient(
        bootstrap_servers=['localhost:9092'],
        api_version=(0, 10, 0)
    )
    
    topics = ['outcomes_raw', 'events', 'transactions', 'sessions']
    new_topics = []
    
    for topic in topics:
        new_topics.append(NewTopic(
            name=topic,
            num_partitions=3,
            replication_factor=1
        ))
    
    try:
        admin.create_topics(new_topics)
        print(f"Created topics: {topics}")
    except Exception as e:
        print(f"Topics might already exist: {e}")
    
    admin.close()
    return topics

def simulate_arroyo_consumer(consumer_id, topic, group_id, error_log):
    """Simulate Arroyo consumer behavior"""
    print(f"[Consumer {consumer_id}] Starting for topic {topic}")
    
    consecutive_errors = 0
    max_consecutive_errors = 10
    
    while consecutive_errors < max_consecutive_errors:
        try:
            # Arroyo creates a new consumer instance
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=['localhost:9092'],
                group_id=group_id,
                enable_auto_commit=True,
                auto_offset_reset='earliest',
                session_timeout_ms=6000,
                api_version=(0, 10, 0)
            )
            
            print(f"[Consumer {consumer_id}] Connected successfully")
            consecutive_errors = 0  # Reset on successful connection
            
            # Simulate Arroyo's poll loop
            poll_errors = 0
            while poll_errors < 5:
                try:
                    messages = consumer.poll(timeout_ms=100)
                    if messages:
                        print(f"[Consumer {consumer_id}] Received {sum(len(m) for m in messages.values())} messages")
                    poll_errors = 0  # Reset on successful poll
                except KafkaError as e:
                    error_msg = str(e)
                    print(f"[Consumer {consumer_id}] Poll error: {error_msg}")
                    error_log.append((consumer_id, time.time(), error_msg))
                    
                    if "REBALANCE_IN_PROGRESS" in error_msg:
                        poll_errors += 1
                        # Arroyo/librdkafka might retry very quickly
                        time.sleep(0.01)  # Very short backoff
                    else:
                        poll_errors += 1
                        time.sleep(0.1)
            
            consumer.close()
            print(f"[Consumer {consumer_id}] Closed after poll errors")
            consecutive_errors += 1
            
        except Exception as e:
            error_msg = str(e)
            print(f"[Consumer {consumer_id}] Connection error: {error_msg}")
            error_log.append((consumer_id, time.time(), error_msg))
            consecutive_errors += 1
            
            # Arroyo might retry quickly
            time.sleep(0.1)
    
    print(f"[Consumer {consumer_id}] Giving up after {consecutive_errors} consecutive errors")

def test_concurrent_consumers():
    """Test multiple consumers joining simultaneously like Snuba startup"""
    print("=== Testing Arroyo/librdkafka-style consumer group behavior ===")
    
    # Setup topics
    topics = setup_topics()
    time.sleep(1)
    
    # Produce some test data
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        api_version=(0, 10, 0)
    )
    
    for topic in topics:
        for i in range(10):
            producer.send(topic, value=f"test-{i}".encode())
    
    producer.flush()
    producer.close()
    print("Produced test messages")
    
    # Start multiple consumers simultaneously (like Snuba startup)
    error_log = []
    threads = []
    group_id = f'snuba-consumers-{int(time.time())}'
    
    print(f"\nStarting consumers with group_id: {group_id}")
    print("This simulates Snuba starting up with multiple consumers...\n")
    
    # Start 4 consumers for different topics at nearly the same time
    for i, topic in enumerate(topics):
        thread = threading.Thread(
            target=simulate_arroyo_consumer,
            args=(i, topic, group_id, error_log),
            daemon=True
        )
        threads.append(thread)
        thread.start()
        # Very short delay to simulate near-simultaneous startup
        time.sleep(0.01)
    
    # Let them run for a bit
    time.sleep(5)
    
    # Analyze errors
    print("\n=== Error Analysis ===")
    rebalance_errors = [e for e in error_log if "REBALANCE_IN_PROGRESS" in e[2]]
    
    if rebalance_errors:
        print(f"\nFound {len(rebalance_errors)} REBALANCE_IN_PROGRESS errors:")
        for consumer_id, timestamp, error in rebalance_errors[:10]:  # Show first 10
            print(f"  Consumer {consumer_id} at {timestamp:.2f}: {error}")
        
        if len(rebalance_errors) > 10:
            print(f"  ... and {len(rebalance_errors) - 10} more")
        
        # Check if we're in a loop
        if len(rebalance_errors) > 20:
            print("\n❌ REBALANCE_IN_PROGRESS LOOP DETECTED!")
            print("This matches the Snuba/Arroyo issue.")
            return False
    else:
        print("✅ No REBALANCE_IN_PROGRESS errors found")
        return True
    
    return len(rebalance_errors) < 5  # Allow a few transient errors

def test_rapid_reconnects():
    """Test rapid reconnection pattern that might trigger the issue"""
    print("\n=== Testing rapid reconnection pattern ===")
    
    group_id = f'rapid-reconnect-{int(time.time())}'
    topic = 'test-rapid'
    
    # Create topic
    admin = KafkaAdminClient(
        bootstrap_servers=['localhost:9092'],
        api_version=(0, 10, 0)
    )
    
    try:
        admin.create_topics([NewTopic(name=topic, num_partitions=3, replication_factor=1)])
    except:
        pass
    admin.close()
    
    errors = []
    
    # Simulate rapid connect/disconnect pattern
    for i in range(10):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=['localhost:9092'],
                group_id=group_id,
                session_timeout_ms=3000,  # Short timeout
                api_version=(0, 10, 0)
            )
            
            # Poll once
            try:
                consumer.poll(timeout_ms=50)
            except KafkaError as e:
                errors.append(str(e))
            
            # Immediately close
            consumer.close()
            
        except Exception as e:
            errors.append(str(e))
        
        # Very short delay between reconnects
        time.sleep(0.05)
    
    rebalance_errors = [e for e in errors if "REBALANCE_IN_PROGRESS" in e]
    
    print(f"Rapid reconnect test: {len(rebalance_errors)} REBALANCE_IN_PROGRESS errors out of {len(errors)} total")
    
    return len(rebalance_errors) < 3

def main():
    passed = 0
    failed = 0
    
    tests = [
        ("Concurrent consumers (Snuba-like)", test_concurrent_consumers),
        ("Rapid reconnections", test_rapid_reconnects),
    ]
    
    for name, test_func in tests:
        print(f"\n{'='*60}")
        print(f"Running: {name}")
        print('='*60)
        
        try:
            if test_func():
                passed += 1
                print(f"\n✅ {name} PASSED")
            else:
                failed += 1
                print(f"\n❌ {name} FAILED")
        except Exception as e:
            failed += 1
            print(f"\n❌ {name} FAILED with exception: {e}")
            import traceback
            traceback.print_exc()
    
    print(f"\n{'='*60}")
    print(f"SUMMARY: {passed} passed, {failed} failed")
    print('='*60)
    
    if failed > 0:
        print("\n⚠️  The REBALANCE_IN_PROGRESS issue is reproduced!")
        print("This confirms the problem seen with Snuba/Arroyo.")
    
    return failed == 0

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)