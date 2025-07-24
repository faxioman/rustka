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
    from confluent_kafka import Producer, Consumer, KafkaError, TopicPartition
    from confluent_kafka.admin import AdminClient, NewTopic
except ImportError:
    print("Please install confluent-kafka: pip install confluent-kafka")
    sys.exit(1)

def ensure_topics(topics):
    admin = AdminClient({'bootstrap.servers': '127.0.0.1:9092'})
    
    new_topics = []
    for topic in topics:
        new_topics.append(NewTopic(topic, num_partitions=3, replication_factor=1))
    
    fs = admin.create_topics(new_topics)
    for topic, f in fs.items():
        try:
            f.result()
        except Exception as e:
            pass

def test_sentry_event_processing():
    print("\n1. Testing Sentry-style event processing...")
    topics = ['events', 'transactions', 'outcomes']
    ensure_topics(topics)
    
    events_processed = defaultdict(list)
    stop_flag = threading.Event()
    
    group_id = f'sentry-processors-{int(time.time() * 1000)}'
    test_run_id = f'test-{int(time.time() * 1000)}'
    print(f"  Using consumer group: {group_id}")
    print(f"  Test run ID: {test_run_id}")
    
    def sentry_worker(worker_id, topics_to_consume):
        print(f"  Worker {worker_id} starting...")
        try:
            consumer = Consumer({
                'bootstrap.servers': '127.0.0.1:9092',
                'group.id': group_id,
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False,
            })
            
            consumer.subscribe(topics_to_consume)
            print(f"  Worker {worker_id} connected and subscribed to {topics_to_consume}")
            consumer.poll(timeout=0.1)
            assignment = consumer.assignment()
            print(f"  Worker {worker_id} assigned partitions: {assignment}")
        except Exception as e:
            print(f"  Worker {worker_id} failed to connect: {e}")
            return
        
        while not stop_flag.is_set():
            msg = consumer.poll(0.5)
            
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"  Worker {worker_id} error: {msg.error()}")
                    continue
            
            try:
                value = json.loads(msg.value().decode('utf-8'))
                if value.get('test_run_id') == test_run_id:
                    event_data = {
                        'topic': msg.topic(),
                        'partition': msg.partition(),
                        'offset': msg.offset(),
                        'value': value
                    }
                    events_processed[worker_id].append(event_data)
                    print(f"  Worker {worker_id} processed: topic={msg.topic()}, partition={msg.partition()}, offset={msg.offset()}")
                    consumer.commit(msg)
            except Exception as e:
                print(f"  Worker {worker_id} decode error: {e}")
        
        consumer.close()
    producer = Producer({
        'bootstrap.servers': '127.0.0.1:9092',
    })
    
    delivered = 0
    def delivery_report(err, msg):
        nonlocal delivered
        if err is None:
            delivered += 1
    
    for i in range(10):
        producer.produce('events', 
                         key=f'project-{i % 3}'.encode('utf-8'),
                         value=json.dumps({
                             'test_run_id': test_run_id,
                             'type': 'error',
                             'project_id': i % 3,
                             'message': f'Error {i}',
                             'timestamp': time.time()
                         }).encode('utf-8'),
                         callback=delivery_report)
        producer.produce('transactions',
                         key=f'project-{i % 3}'.encode('utf-8'),
                         value=json.dumps({
                             'test_run_id': test_run_id,
                             'type': 'transaction',
                             'project_id': i % 3,
                             'name': f'/api/endpoint/{i}',
                             'duration': 100 + i * 10
                         }).encode('utf-8'),
                         callback=delivery_report)
        producer.produce('outcomes',
                         value=json.dumps({
                             'test_run_id': test_run_id,
                             'type': 'outcome',
                             'outcome': 'accepted',
                             'category': 'error'
                         }).encode('utf-8'),
                         callback=delivery_report)
    
    producer.flush()
    print(f"✓ Produced {delivered} events")
    time.sleep(1)
    workers = []
    for i in range(3):
        t = threading.Thread(
            target=sentry_worker,
            args=(f'worker-{i}', topics),
            daemon=True
        )
        t.start()
        workers.append(t)
        time.sleep(0.5)
    
    time.sleep(10)
    stop_flag.set()
    
    for t in workers:
        t.join(timeout=2)
    total_processed = sum(len(events) for events in events_processed.values())
    workers_that_processed = len(events_processed)
    
    print(f"✓ {total_processed} events processed by {workers_that_processed} workers")
    if workers_that_processed > 1:
        print("✓ Work distributed across multiple workers")
    else:
        print("⚠ All work went to a single worker (might be normal for small dataset)")
    
    if total_processed != 30:
        print(f"✗ MISSING EVENTS: Expected 30, got {total_processed}")
    
    return total_processed == 30

def test_sentry_offset_tracking():
    print("\n2. Testing Sentry-style offset tracking...")
    
    group_id = f'sentry-offset-test-{int(time.time() * 1000)}'
    topic = 'events'
    test_run_id = f'offset-test-{int(time.time() * 1000)}'
    producer = Producer({
        'bootstrap.servers': '127.0.0.1:9092',
    })
    
    delivered = 0
    def delivery_report(err, msg):
        nonlocal delivered
        if err is None:
            delivered += 1
    
    print("  Producing test messages...")
    for i in range(10):
        producer.produce(topic, value=json.dumps({
            'test_run_id': test_run_id,
            'message_id': i,
            'content': f'Test message {i}'
        }).encode('utf-8'), callback=delivery_report)
    
    producer.flush()
    print(f"  Produced {delivered} messages")
    time.sleep(0.5)
    consumer1 = Consumer({
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': group_id,
        'enable.auto.commit': False,
        'auto.offset.reset': 'earliest',
    })
    
    consumer1.subscribe([topic])
    
    processed_messages = []
    print("  First consumer processing messages...")
    
    start_time = time.time()
    while len(processed_messages) < 5 and time.time() - start_time < 10:
        msg = consumer1.poll(0.5)
        
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"  Consumer1 error: {msg.error()}")
                continue
        
        try:
            value = json.loads(msg.value().decode('utf-8'))
            if value.get('test_run_id') == test_run_id:
                processed_messages.append(value['message_id'])
                print(f"  - Processed message {value['message_id']}")
                consumer1.commit()
                
                if len(processed_messages) >= 5:
                    break
        except Exception as e:
            print(f"  Decode error: {e}")
    
    consumer1.close()
    print(f"✓ First consumer processed {len(processed_messages)} messages: {processed_messages}")
    consumer2 = Consumer({
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': group_id,
        'enable.auto.commit': False,
    })
    
    consumer2.subscribe([topic])
    
    duplicate_messages = []
    new_messages = []
    
    print("\n  Second consumer checking for duplicates...")
    start_time = time.time()
    while time.time() - start_time < 5:
        msg = consumer2.poll(0.5)
        
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                continue
        
        try:
            value = json.loads(msg.value().decode('utf-8'))
            if value.get('test_run_id') == test_run_id:
                msg_id = value['message_id']
                
                if msg_id in processed_messages:
                    duplicate_messages.append(msg_id)
                    print(f"  ✗ DUPLICATE: Second consumer got already-processed message {msg_id}")
                else:
                    new_messages.append(msg_id)
                    print(f"  - Second consumer got new message {msg_id}")
        except Exception as e:
            print(f"  Decode error: {e}")
    
    consumer2.close()
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
    print("\n3. Testing high throughput scenario...")
    
    num_events = 100
    test_run_id = f'throughput-test-{int(time.time() * 1000)}'
    start_time = time.time()
    producer = Producer({
        'bootstrap.servers': '127.0.0.1:9092',
        'batch.size': 16384,
        'linger.ms': 10,
    })
    
    delivered = 0
    def delivery_report(err, msg):
        nonlocal delivered
        if err is None:
            delivered += 1
    
    for i in range(num_events):
        producer.produce('events', value=json.dumps({
            'test_run_id': test_run_id,
            'event_id': i,
            'timestamp': time.time(),
            'data': 'x' * 1000
        }).encode('utf-8'), callback=delivery_report)
    
    producer.flush()
    
    produce_time = time.time() - start_time
    events_per_second = delivered / produce_time if produce_time > 0 else 0
    
    print(f"✓ Produced {delivered} events in {produce_time:.2f}s ({events_per_second:.0f} events/sec)")
    consumer = Consumer({
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': f'perf-test-{int(time.time())}',
        'auto.offset.reset': 'earliest',
        'fetch.min.bytes': 1,
        'fetch.wait.max.ms': 10,
    })
    
    consumer.subscribe(['events'])
    
    start_time = time.time()
    consumed = 0
    
    poll_count = 0
    while consumed < num_events:
        poll_count += 1
        msg = consumer.poll(0.1)
        
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                continue
        
        try:
            value = json.loads(msg.value().decode('utf-8'))
            if value.get('test_run_id') == test_run_id:
                consumed += 1
                if consumed % 10 == 0:
                    print(f"  Consumed {consumed} messages")
        except:
            pass
        
        if time.time() - start_time > 10:
            print(f"⚠ Timeout after {poll_count} polls")
            break
    
    consume_time = time.time() - start_time
    consume_rate = consumed / consume_time if consume_time > 0 else 0
    
    consumer.close()
    
    print(f"✓ Consumed {consumed} events in {consume_time:.2f}s ({consume_rate:.0f} events/sec)")
    
    return consumed == num_events

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