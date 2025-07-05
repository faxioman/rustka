#!/usr/bin/env python3
"""
Test that verifies Kafka features used specifically by Sentry
"""
import sys
import time
import json
import threading
from collections import defaultdict

try:
    from kafka import KafkaProducer, KafkaConsumer, TopicPartition
    from kafka.structs import OffsetAndMetadata
except ImportError:
    print("Please install kafka-python: pip install kafka-python")
    sys.exit(1)

def test_sentry_event_processing():
    """Simulates Sentry event processing with consumer groups"""
    print("\n1. Testing Sentry-style event processing...")
    
    # Topics that Sentry uses
    topics = ['events', 'transactions', 'outcomes']
    events_processed = defaultdict(list)
    stop_flag = threading.Event()
    
    # Use unique group ID for each test run to avoid reading old messages
    group_id = f'sentry-processors-{int(time.time() * 1000)}'
    test_run_id = f'test-{int(time.time() * 1000)}'  # Unique ID for this test run
    print(f"  Using consumer group: {group_id}")
    print(f"  Test run ID: {test_run_id}")
    
    def sentry_worker(worker_id, topics_to_consume):
        """Simulates a Sentry worker"""
        print(f"  Worker {worker_id} starting...")
        try:
            consumer = KafkaConsumer(
                *topics_to_consume,
                bootstrap_servers=['localhost:9092'],
                group_id=group_id,
                auto_offset_reset='earliest',
                enable_auto_commit=False,  # Manual commit for debugging
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                api_version=(0, 10, 0),
                max_poll_records=500
            )
            print(f"  Worker {worker_id} connected and subscribed to {topics_to_consume}")
            
            # Force assignment check
            consumer.poll(timeout_ms=100)
            assignment = consumer.assignment()
            print(f"  Worker {worker_id} assigned partitions: {assignment}")
        except Exception as e:
            print(f"  Worker {worker_id} failed to connect: {e}")
            return
        
        while not stop_flag.is_set():
            messages = consumer.poll(timeout_ms=500)
            if messages:
                print(f"  Worker {worker_id} received messages")
            for topic_partition, records in messages.items():
                for record in records:
                    # Only count messages from this test run
                    if record.value.get('test_run_id') == test_run_id:
                        event_data = {
                            'topic': record.topic,
                            'partition': record.partition,
                            'offset': record.offset,
                            'value': record.value
                        }
                        events_processed[worker_id].append(event_data)
                        print(f"  Worker {worker_id} processed: topic={record.topic}, partition={record.partition}, offset={record.offset}")
        
        consumer.close()
    
    # First produce all events
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        api_version=(0, 10, 0)
    )
    
    # Produce different event types
    for i in range(10):
        # Error event
        producer.send('events', 
                     key=f'project-{i % 3}',
                     value={
                         'test_run_id': test_run_id,
                         'type': 'error',
                         'project_id': i % 3,
                         'message': f'Error {i}',
                         'timestamp': time.time()
                     })
        
        # Transaction event
        producer.send('transactions',
                     key=f'project-{i % 3}',
                     value={
                         'test_run_id': test_run_id,
                         'type': 'transaction',
                         'project_id': i % 3,
                         'name': f'/api/endpoint/{i}',
                         'duration': 100 + i * 10
                     })
        
        # Outcome event
        producer.send('outcomes',
                     value={
                         'test_run_id': test_run_id,
                         'type': 'outcome',
                         'outcome': 'accepted',
                         'category': 'error'
                     })
    
    producer.flush()
    producer.close()
    
    print("✓ Produced 30 events")
    time.sleep(1)  # Give broker time to process
    
    # Now start workers AFTER messages are produced
    workers = []
    for i in range(3):
        t = threading.Thread(
            target=sentry_worker,
            args=(f'worker-{i}', topics),
            daemon=True
        )
        t.start()
        workers.append(t)
        time.sleep(0.5)  # Stagger worker starts
    
    # Let workers process
    time.sleep(10)  # Even more time for processing
    stop_flag.set()
    
    for t in workers:
        t.join(timeout=2)  # Don't wait forever
    
    # Verify results
    total_processed = sum(len(events) for events in events_processed.values())
    workers_that_processed = len(events_processed)
    
    print(f"✓ {total_processed} events processed by {workers_that_processed} workers")
    
    # Check that work was distributed
    if workers_that_processed > 1:
        print("✓ Work distributed across multiple workers")
    else:
        print("⚠ All work went to a single worker (might be normal for small dataset)")
    
    # WE SHOULD HAVE 30 EVENTS!
    if total_processed != 30:
        print(f"✗ MISSING EVENTS: Expected 30, got {total_processed}")
    
    return total_processed == 30  # Must be EXACTLY 30

def test_sentry_offset_tracking():
    """Test that offsets are tracked correctly for recovery"""
    print("\n2. Testing Sentry-style offset tracking...")
    
    group_id = f'sentry-offset-test-{int(time.time() * 1000)}'
    topic = 'events'
    test_run_id = f'offset-test-{int(time.time() * 1000)}'
    
    # Send test messages
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(0, 10, 0)
    )
    
    print("  Producing test messages...")
    for i in range(10):
        producer.send(topic, value={
            'test_run_id': test_run_id,
            'message_id': i,
            'content': f'Test message {i}'
        })
    
    producer.flush()
    producer.close()
    time.sleep(0.5)
    
    # Consumer 1: Read messages with manual commit
    consumer1 = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset='earliest',
        api_version=(0, 10, 0),
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    processed_messages = []
    print("  First consumer processing messages...")
    
    start_time = time.time()
    while len(processed_messages) < 5 and time.time() - start_time < 10:
        messages = consumer1.poll(timeout_ms=500)
        
        for topic_partition, records in messages.items():
            for message in records:
                if message.value.get('test_run_id') == test_run_id:
                    processed_messages.append(message.value['message_id'])
                    print(f"  - Processed message {message.value['message_id']}")
                    
                    # Commit after each message
                    consumer1.commit()
                    
                    if len(processed_messages) >= 5:
                        break
            if len(processed_messages) >= 5:
                break
    
    consumer1.close()
    print(f"✓ First consumer processed {len(processed_messages)} messages: {processed_messages}")
    
    # Consumer 2: Same group, should NOT get the same messages
    consumer2 = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        group_id=group_id,
        enable_auto_commit=False,
        api_version=(0, 10, 0),
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    duplicate_messages = []
    new_messages = []
    
    print("\n  Second consumer checking for duplicates...")
    start_time = time.time()
    while time.time() - start_time < 5:
        messages = consumer2.poll(timeout_ms=500)
        
        if not messages:
            continue
            
        for topic_partition, records in messages.items():
            for message in records:
                if message.value.get('test_run_id') == test_run_id:
                    msg_id = message.value['message_id']
                    
                    if msg_id in processed_messages:
                        duplicate_messages.append(msg_id)
                        print(f"  ✗ DUPLICATE: Second consumer got already-processed message {msg_id}")
                    else:
                        new_messages.append(msg_id)
                        print(f"  - Second consumer got new message {msg_id}")
    
    consumer2.close()
    
    # Evaluate results
    success = True
    
    if duplicate_messages:
        print(f"\n✗ Found {len(duplicate_messages)} duplicate messages: {duplicate_messages}")
        print("  This indicates offset commits are not working properly!")
        success = False
    else:
        print("\n✓ No duplicate messages found - offset commits are working correctly")
    
    if new_messages:
        print(f"✓ Second consumer got {len(new_messages)} new messages: {new_messages}")
    
    total_unique = len(set(processed_messages + new_messages))
    print(f"  Total unique messages seen: {total_unique}/10")
    
    return success

def test_sentry_high_throughput():
    """Test high throughput scenario tipico di Sentry"""
    print("\n3. Testing high throughput scenario...")
    
    num_events = 100
    test_run_id = f'throughput-test-{int(time.time() * 1000)}'
    start_time = time.time()
    
    # Produce many events quickly
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        batch_size=16384,  # Batch for efficiency
        linger_ms=10,
        api_version=(0, 10, 0)
    )
    
    futures = []
    for i in range(num_events):
        future = producer.send('events', value={
            'test_run_id': test_run_id,
            'event_id': i,
            'timestamp': time.time(),
            'data': 'x' * 1000  # 1KB payload
        })
        futures.append(future)
    
    # Wait for all sends
    for future in futures:
        try:
            future.get(timeout=10)
        except Exception as e:
            print(f"✗ Failed to send: {e}")
            return False
    
    producer.close()
    
    produce_time = time.time() - start_time
    events_per_second = num_events / produce_time
    
    print(f"✓ Produced {num_events} events in {produce_time:.2f}s ({events_per_second:.0f} events/sec)")
    
    # Consume with high throughput settings
    consumer = KafkaConsumer(
        'events',
        bootstrap_servers=['localhost:9092'],
        group_id=f'perf-test-{int(time.time())}',
        auto_offset_reset='earliest',
        fetch_min_bytes=1,  # Don't wait for more bytes
        fetch_max_wait_ms=10,  # Reduce wait time
        max_poll_records=500,  # Get more records per poll
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        api_version=(0, 10, 0)
    )
    
    start_time = time.time()
    consumed = 0
    
    poll_count = 0
    while consumed < num_events:
        poll_count += 1
        messages = consumer.poll(timeout_ms=100, max_records=500)
        
        if messages:
            for topic_partition, records in messages.items():
                # Count only messages from this test run
                test_messages = [r for r in records if r.value.get('test_run_id') == test_run_id]
                consumed += len(test_messages)
                if test_messages:
                    print(f"  Poll {poll_count}: got {len(test_messages)} test messages from {topic_partition}")
        
        if time.time() - start_time > 10:
            print(f"⚠ Timeout after {poll_count} polls")
            break
    
    consume_time = time.time() - start_time
    consume_rate = consumed / consume_time
    
    consumer.close()
    
    print(f"✓ Consumed {consumed} events in {consume_time:.2f}s ({consume_rate:.0f} events/sec)")
    
    return consumed == num_events  # Should consume exactly all our test events

def main():
    print("=== Sentry Kafka Compatibility Test ===")
    print("Testing Rustka with Sentry-specific patterns...\n")
    
    tests = [
        ("Event Processing with Groups", test_sentry_event_processing),
        ("Offset Tracking", test_sentry_offset_tracking),
        ("High Throughput", test_sentry_high_throughput),
    ]
    
    passed = 0
    failed = 0
    
    for name, test_func in tests:
        print(f"\n{'='*50}")
        print(f"Running: {name}")
        print('='*50)
        
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
    
    print(f"\n{'='*50}")
    print(f"SUMMARY: {passed} passed, {failed} failed")
    print('='*50)
    
    if passed == len(tests):
        print("\n🎉 Rustka is compatible with Sentry's Kafka usage!")
    
    return failed == 0

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
