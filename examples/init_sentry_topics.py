#!/usr/bin/env python3
"""Initialize essential Sentry topics in Rustka by triggering auto-creation"""

from kafka import KafkaProducer, KafkaConsumer
import json
import time

print("Initializing Sentry topics in Rustka...")

# Essential topics for Sentry to process events
essential_topics = [
    'ingest-events',      # Where Relay sends raw events
    'events',             # Processed events
    'errors',             # Error events
    'transactions',       # Transaction events
    'ingest-transactions',# Where Relay sends transactions
    'ingest-attachments', # Attachments
    'eventstream',        # Event stream for real-time processing
    'snuba-errors',       # Snuba consumer for errors
    'snuba-transactions', # Snuba consumer for transactions
    'snuba-outcomes',     # Outcomes
    '__commit_log',       # Kafka internal commit log
]

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8') if isinstance(v, dict) else v
)

# Create topics by sending a dummy message
for topic in essential_topics:
    try:
        # Send an empty/dummy message to trigger topic creation
        if topic == '__commit_log':
            # Special handling for commit log
            producer.send(topic, b'')
        else:
            producer.send(topic, {'init': True, 'timestamp': time.time()})
        print(f"✓ Initialized topic: {topic}")
    except Exception as e:
        print(f"✗ Error with topic {topic}: {e}")

producer.flush()
producer.close()

print("\nVerifying topics...")
# Create a consumer to list topics
consumer = KafkaConsumer(bootstrap_servers='localhost:9092')
topics = consumer.topics()
consumer.close()

print(f"\nCurrent topics in Rustka: {sorted(topics)}")

# Check if critical topics exist
missing = []
for topic in ['ingest-events', 'events', 'errors']:
    if topic not in topics:
        missing.append(topic)

if missing:
    print(f"\n⚠️  Missing critical topics: {missing}")
else:
    print("\n✅ All critical topics created!")
    
print("\nNow restart Sentry consumers to pick up the new topics:")
print("  docker-compose -f ~/path/to/sentry/docker-compose.yml restart")
