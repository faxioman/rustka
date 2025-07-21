#!/usr/bin/env python3
"""
Test that simulates Kafka usage by Sentry
"""
from kafka import KafkaProducer, KafkaConsumer
from kafka.structs import TopicPartition
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

def produce_sentry_events():
    """Simulates Sentry event production"""
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        api_version=(0, 10, 0)
    )
    
    event_types = ['error', 'transaction', 'session', 'attachment', 'outcome']
    projects = ['frontend', 'backend', 'mobile', 'api']
    
    print("Starting Sentry event producer...")
    
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
        producer.send(topic, key=project, value=event)
        print(f"Produced {event_type} event for project {project}")
        time.sleep(0.2)
    
    producer.flush()
    producer.close()
    print("Producer finished")

def sentry_consumer_worker(worker_id, topics, group_id='sentry-consumers'):
    """Simulates a Sentry worker that processes events"""
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=['localhost:9092'],
        group_id=group_id,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None,
        api_version=(0, 10, 0)
    )
    
    print(f"Worker {worker_id} started, consuming topics: {topics}")
    
    # Simulate different processing times per event type
    processing_times = {
        'error': 0.05,
        'transaction': 0.02,
        'session': 0.01,
        'attachment': 0.1,
        'outcome': 0.01
    }
    
    try:
        for message in consumer:
            event = message.value
            event_type = event.get('type', 'unknown')
            project = message.key
            
            # Simulate processing
            time.sleep(processing_times.get(event_type, 0.01))
            
            print(f"Worker {worker_id} processed {event_type} from {project} "
                  f"(topic: {message.topic}, partition: {message.partition}, offset: {message.offset})")
            
            # Simulate saving to database, sending notifications, etc.
            if event_type == 'error':
                if 'exception' in event and 'value' in event['exception']:
                    print(f"  -> Stored error: {event['exception']['value']}")
            elif event_type == 'transaction':
                if 'transaction' in event and 'duration' in event:
                    print(f"  -> Recorded transaction: {event['transaction']} ({event['duration']:.2f}ms)")
                
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print(f"Worker {worker_id} stopped")

def test_sentry_setup():
    """Test that simulates a Sentry setup with Rustka"""
    print("=== Sentry-like Kafka Usage Test ===\n")
    print("This simulates how Sentry uses Kafka with:")
    print("- Multiple topics (events, transactions, attachments, etc.)")
    print("- Consumer groups for horizontal scaling")
    print("- Project-based partitioning\n")
    
    # Start producer
    producer_thread = threading.Thread(target=produce_sentry_events, daemon=True)
    producer_thread.start()
    
    # Wait a bit to let topics be created
    time.sleep(2)
    
    # Start specialized workers by type
    workers = []
    
    # 2 workers for events and errors
    for i in range(2):
        t = threading.Thread(
            target=sentry_consumer_worker,
            args=(f"error-worker-{i}", ['events']),
            daemon=True
        )
        t.start()
        workers.append(t)
    
    # 2 workers for transactions
    for i in range(2):
        t = threading.Thread(
            target=sentry_consumer_worker,
            args=(f"transaction-worker-{i}", ['transactions']),
            daemon=True
        )
        t.start()
        workers.append(t)
    
    # 1 generic worker for other topics
    t = threading.Thread(
        target=sentry_consumer_worker,
        args=("generic-worker", ['attachments', 'outcomes', 'sessions']),
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
    except KeyboardInterrupt:
        print("\nStopping test...")

if __name__ == "__main__":
    test_sentry_setup()
