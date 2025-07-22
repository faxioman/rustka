#!/usr/bin/env python3
"""
Test that simulates Kafka usage by Sentry
"""
from confluent_kafka import Producer, Consumer, KafkaError
import json
import time
import threading
import random

# Typical Sentry topics
SENTRY_TOPICS = {
    'events': 4,           # Error events
    'transactions': 4,     # Performance monitoring
    'attachments': 2,      # File attachments
    'outcomes': 2,         # Event outcomes
    'sessions': 2,         # Session tracking
}

def ensure_topics():
    """Ensure all Sentry topics exist"""
    # Skip topic creation - let Rustka auto-create them
    # This avoids issues with AdminClient
    print("Skipping topic creation - Rustka will auto-create them")

def produce_sentry_events():
    """Simulates Sentry event production"""
    producer = Producer({
        'bootstrap.servers': '127.0.0.1:9092',
    })
    
    event_types = ['error', 'transaction', 'session', 'attachment', 'outcome']
    projects = ['frontend', 'backend', 'mobile', 'api']
    
    print("Starting Sentry event producer...")
    
    delivered = 0
    def delivery_report(err, msg):
        nonlocal delivered
        if err is None:
            delivered += 1
    
    for i in range(50):
        event_type = random.choice(event_types)
        project = random.choice(projects)
        
        # Simulate different types of Sentry events
        if event_type == 'error':
            event = {
                'type': 'error',
                'project': project,
                'timestamp': time.time(),
                'exception': {
                    'type': 'ValueError',
                    'value': f'Test error {i}'
                },
                'tags': {'environment': 'production'}
            }
            topic = 'events'
        elif event_type == 'transaction':
            event = {
                'type': 'transaction',
                'project': project,
                'timestamp': time.time(),
                'transaction': f'/api/endpoint/{i}',
                'duration': random.uniform(10, 1000)
            }
            topic = 'transactions'
        else:
            event = {
                'type': event_type,
                'project': project,
                'timestamp': time.time(),
                'data': f'Test {event_type} {i}'
            }
            topic = event_type + 's'
        
        # Use project as key for partitioning
        producer.produce(topic, 
                        key=project.encode('utf-8'), 
                        value=json.dumps(event).encode('utf-8'),
                        callback=delivery_report)
        
        if i % 10 == 0:
            producer.poll(0)  # Trigger delivery reports
            
        time.sleep(0.2)
    
    producer.flush()
    print(f"Producer finished - delivered {delivered} events")

def sentry_consumer_worker(worker_id, topics, group_id='sentry-consumers', stop_event=None):
    """Simulates a Sentry worker that processes events"""
    consumer = Consumer({
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'auto.commit.interval.ms': 1000,
    })
    
    consumer.subscribe(topics)
    print(f"Worker {worker_id} started, consuming topics: {topics}")
    
    # Simulate different processing times per event type
    processing_times = {
        'error': 0.05,
        'transaction': 0.02,
        'session': 0.01,
        'attachment': 0.1,
        'outcome': 0.01
    }
    
    processed = 0
    try:
        while not (stop_event and stop_event.is_set()):
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Worker {worker_id} error: {msg.error()}")
                    continue
            
            try:
                event = json.loads(msg.value().decode('utf-8'))
                event_type = event.get('type', 'unknown')
                project = msg.key().decode('utf-8') if msg.key() else 'unknown'
                
                # Simulate processing
                time.sleep(processing_times.get(event_type, 0.01))
                
                processed += 1
                print(f"Worker {worker_id} processed {event_type} from {project} "
                      f"(topic: {msg.topic()}, partition: {msg.partition()}, offset: {msg.offset()})")
                
                # Simulate saving to database, sending notifications, etc.
                if event_type == 'error':
                    if 'exception' in event and 'value' in event['exception']:
                        print(f"  -> Stored error: {event['exception']['value']}")
                elif event_type == 'transaction':
                    if 'transaction' in event and 'duration' in event:
                        print(f"  -> Recorded transaction: {event['transaction']} ({event['duration']:.2f}ms)")
                        
            except Exception as e:
                print(f"Worker {worker_id} processing error: {e}")
                
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print(f"Worker {worker_id} stopped - processed {processed} events")

def test_sentry_setup():
    """Test that simulates a Sentry setup with Rustka"""
    print("=== Sentry-like Kafka Usage Test ===\n")
    print("This simulates how Sentry uses Kafka with:")
    print("- Multiple topics (events, transactions, attachments, etc.)")
    print("- Consumer groups for horizontal scaling")
    print("- Project-based partitioning\n")
    
    # Ensure topics exist
    ensure_topics()
    
    # Start producer
    producer_thread = threading.Thread(target=produce_sentry_events, daemon=True)
    producer_thread.start()
    
    # Wait a bit to let topics be created
    time.sleep(2)
    
    # Start specialized workers by type
    workers = []
    stop_event = threading.Event()
    
    # 2 workers for events and errors
    for i in range(2):
        t = threading.Thread(
            target=sentry_consumer_worker,
            args=(f"error-worker-{i}", ['events'], 'sentry-consumers', stop_event),
            daemon=True
        )
        t.start()
        workers.append(t)
    
    # 2 workers for transactions
    for i in range(2):
        t = threading.Thread(
            target=sentry_consumer_worker,
            args=(f"transaction-worker-{i}", ['transactions'], 'sentry-consumers', stop_event),
            daemon=True
        )
        t.start()
        workers.append(t)
    
    # 1 generic worker for other topics
    t = threading.Thread(
        target=sentry_consumer_worker,
        args=("generic-worker", ['attachments', 'outcomes', 'sessions'], 'sentry-consumers', stop_event),
        daemon=True
    )
    t.start()
    workers.append(t)
    
    print(f"\n{len(workers)} workers started. Waiting for events...\n")
    
    try:
        # Wait for the producer to finish
        producer_thread.join()
        print("\nAll events produced. Waiting for workers to finish processing...")
        time.sleep(5)
        
        # Stop workers
        stop_event.set()
        for t in workers:
            t.join(timeout=2)
            
    except KeyboardInterrupt:
        print("\nStopping test...")
        stop_event.set()

if __name__ == "__main__":
    test_sentry_setup()