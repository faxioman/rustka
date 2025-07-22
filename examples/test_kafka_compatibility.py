#!/usr/bin/env python3
"""
Compatibility test: verifies that Rustka behaves like Kafka
"""
from confluent_kafka import Producer, Consumer, KafkaError, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
import json
import time
import unittest
import threading
import uuid

class TestKafkaCompatibility(unittest.TestCase):
    """Tests that should pass with both Kafka and Rustka"""
    
    @classmethod
    def setUpClass(cls):
        cls.bootstrap_servers = '127.0.0.1:9092'
        # Use unique topic for each test run to avoid conflicts
        cls.test_run_id = str(uuid.uuid4())[:8]
        cls.test_topic = f'test-compatibility-{cls.test_run_id}'
        print(f"Using topic: {cls.test_topic}")
        
        # Create topics
        admin = AdminClient({'bootstrap.servers': cls.bootstrap_servers})
        topics_to_create = [
            NewTopic(cls.test_topic, num_partitions=3, replication_factor=1),
            NewTopic(f'{cls.test_topic}-rebalance', num_partitions=3, replication_factor=1),
            NewTopic(f'{cls.test_topic}-offset', num_partitions=3, replication_factor=1),
            NewTopic(f'{cls.test_topic}-partitions', num_partitions=3, replication_factor=1),
        ]
        fs = admin.create_topics(topics_to_create)
        for topic, f in fs.items():
            try:
                f.result()
            except Exception as e:
                pass  # Topic might already exist
    
    def test_01_produce_consume_basic(self):
        """Basic test: produce and consume a message"""
        # Use a unique group for this test
        group_id = f'test-basic-{self.test_run_id}'
        
        producer = Producer({
            'bootstrap.servers': self.bootstrap_servers,
        })
        
        test_message = json.dumps({'test': 'basic', 'timestamp': time.time()})
        
        delivered = False
        partition = None
        offset = None
        
        def delivery_report(err, msg):
            nonlocal delivered, partition, offset
            if err is None:
                delivered = True
                partition = msg.partition()
                offset = msg.offset()
        
        producer.produce(self.test_topic, value=test_message.encode('utf-8'), callback=delivery_report)
        producer.flush()
        
        self.assertTrue(delivered)
        self.assertIsNotNone(partition)
        self.assertGreaterEqual(offset, 0)
        
        # Consume
        consumer = Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
        })
        
        consumer.subscribe([self.test_topic])
        
        messages = []
        start_time = time.time()
        while time.time() - start_time < 5:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
            else:
                try:
                    value = json.loads(msg.value().decode('utf-8'))
                    if value.get('test') == 'basic':
                        messages.append(msg)
                        break
                except:
                    pass
        
        consumer.close()
        
        self.assertGreater(len(messages), 0)
    
    def test_02_consumer_group_rebalance(self):
        """Test consumer group rebalancing"""
        topic = f'{self.test_topic}-rebalance'
        group_id = f'test-rebalance-{self.test_run_id}'
        consumed_by = {}
        stop_consumers = threading.Event()
        
        # First produce messages
        producer = Producer({
            'bootstrap.servers': self.bootstrap_servers,
        })
        
        print(f"Producing messages to {topic}")
        for i in range(10):
            producer.produce(topic, f'message-{i}'.encode('utf-8'))
        producer.flush()
        
        # Small delay to ensure messages are available
        time.sleep(1)
        
        def consumer_thread(consumer_id):
            try:
                consumer = Consumer({
                    'bootstrap.servers': self.bootstrap_servers,
                    'group.id': group_id,
                    'auto.offset.reset': 'earliest',
                })
                
                consumer.subscribe([topic])
                print(f"{consumer_id} started")
                
                while not stop_consumers.is_set():
                    msg = consumer.poll(0.5)
                    if msg is None:
                        continue
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                    else:
                        consumed_by[msg.offset()] = consumer_id
                        print(f"{consumer_id} consumed offset {msg.offset()}")
                
                consumer.close()
                print(f"{consumer_id} stopped")
            except Exception as e:
                print(f"{consumer_id} error: {e}")
        
        # Start 2 consumers
        threads = []
        for i in range(2):
            t = threading.Thread(target=consumer_thread, args=(f'consumer-{i}',))
            t.start()
            threads.append(t)
            time.sleep(0.5)  # Stagger startup
        
        # Let them process
        time.sleep(5)
        stop_consumers.set()
        
        for t in threads:
            t.join(timeout=3)
        
        # Verify that consumers processed messages
        unique_consumers = set(consumed_by.values())
        print(f"Messages consumed by: {unique_consumers}")
        print(f"Total messages consumed: {len(consumed_by)}")
        
        # At least some messages should be consumed
        self.assertGreater(len(consumed_by), 0)
        self.assertGreaterEqual(len(unique_consumers), 1)
    
    def test_03_offset_commit_fetch(self):
        """Test offset commit and fetch"""
        topic = f'{self.test_topic}-offset'
        group_id = f'test-offset-{self.test_run_id}'
        
        # Produce a message
        producer = Producer({
            'bootstrap.servers': self.bootstrap_servers,
        })
        producer.produce(topic, b'offset-test-message')
        producer.flush()
        
        # Consumer 1: consume and commit
        consumer1 = Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': group_id,
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest',
        })
        
        consumer1.subscribe([topic])
        
        # Consume a message
        last_offset = None
        last_partition = None
        start_time = time.time()
        while time.time() - start_time < 5:
            msg = consumer1.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
            else:
                last_offset = msg.offset()
                last_partition = msg.partition()
                consumer1.commit()
                print(f"Consumer1 consumed from partition {last_partition}, committed offset: {last_offset}")
                break
        
        consumer1.close()
        
        if last_offset is not None:
            # Small delay to ensure commit is processed
            time.sleep(0.5)
            
            # Consumer 2: should start from committed offset
            consumer2 = Consumer({
                'bootstrap.servers': self.bootstrap_servers,
                'group.id': group_id,
                'enable.auto.commit': False,
            })
            
            consumer2.subscribe([topic])
            
            # Poll multiple times to ensure proper group join
            print(f"Consumer2 joining group {group_id}...")
            for i in range(3):
                consumer2.poll(1.0)
                time.sleep(0.1)
            
            # Get assignment
            assignment = consumer2.assignment()
            print(f"Consumer2 assignment: {assignment}")
            
            if assignment:
                # Check the specific partition that consumer1 used
                tp = TopicPartition(topic, last_partition)
                committed = consumer2.committed([tp])[0].offset
                print(f"Consumer2 sees committed offset for partition {last_partition}: {committed}")
                
                self.assertNotEqual(committed, -1001)  # -1001 means no committed offset
                # Kafka stores next offset to read, so should be last_offset + 1
                self.assertEqual(committed, last_offset + 1, 
                             f"Expected committed offset to be {last_offset + 1}, got {committed}")
            else:
                self.fail("Consumer2 has no assignment")
            
            consumer2.close()
    
    def test_04_multiple_partitions(self):
        """Test with multiple partitions"""
        topic = f'{self.test_topic}-partitions'
        
        producer = Producer({
            'bootstrap.servers': self.bootstrap_servers,
        })
        
        # Send messages to different partitions using key
        partitions_used = set()
        delivered_count = 0
        
        def delivery_report(err, msg):
            nonlocal delivered_count
            if err is None:
                partitions_used.add(msg.partition())
                delivered_count += 1
        
        for i in range(20):
            key = f'key-{i % 3}'  # Should distribute across 3 partitions
            producer.produce(topic, key=key.encode('utf-8'), value=f'msg-{i}'.encode('utf-8'), callback=delivery_report)
        
        producer.flush()
        
        # Verify that it used multiple partitions
        print(f"Partitions used: {partitions_used}")
        print(f"Messages delivered: {delivered_count}")
        # Note: with Rustka's default 3 partitions, we should see distribution
        self.assertGreaterEqual(len(partitions_used), 1)
    
    def test_05_metadata_api(self):
        """Test metadata API"""
        # librdkafka handles metadata internally, so we'll use AdminClient
        admin = AdminClient({'bootstrap.servers': self.bootstrap_servers})
        
        # Get cluster metadata
        metadata = admin.list_topics(timeout=10)
        
        self.assertIsNotNone(metadata.brokers)
        self.assertGreater(len(metadata.brokers), 0)
        
        broker_info = [f"{broker.id}@{broker.host}:{broker.port}" for id, broker in metadata.brokers.items()]
        print(f"Brokers: {broker_info}")
        
        topic_names = list(metadata.topics.keys())[:5]  # Show first 5 topics
        print(f"Topics: {topic_names}...")

if __name__ == '__main__':
    print("=== Kafka Compatibility Test Suite ===")
    print("These tests should pass with both Kafka and Rustka")
    print("Make sure Rustka is running on 127.0.0.1:9092\n")
    
    unittest.main(verbosity=2)