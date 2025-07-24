#!/usr/bin/env python3

from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
import json
import time
from threading import Thread, Event

TOPIC_NAME = f'test-producer-consumer-threads-{int(time.time())}'
admin = AdminClient({'bootstrap.servers': '127.0.0.1:9092'})
new_topic = NewTopic(TOPIC_NAME, num_partitions=3, replication_factor=1)
fs = admin.create_topics([new_topic])
for topic, f in fs.items():
    try:
        f.result()
    except Exception as e:
        pass

def producer_thread():
    producer = Producer({
        'bootstrap.servers': '127.0.0.1:9092',
    })
    
    delivered = 0
    def delivery_report(err, msg):
        nonlocal delivered
        if err is None:
            delivered += 1
            if delivered % 10 == 0:
                print(f"Produced {delivered} messages")
    
    for i in range(100):
        data = {"counter": i, "timestamp": time.time()}
        producer.produce(TOPIC_NAME, value=json.dumps(data).encode(), callback=delivery_report)
        if i % 10 == 0:
            producer.poll(0)
        time.sleep(0.1)
    
    producer.flush()
    print(f"Producer finished - delivered {delivered} messages")

def consumer_thread(stop_event):
    time.sleep(2)
    
    consumer = Consumer({
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': f'test-consumer-group-{int(time.time())}',
        'auto.offset.reset': 'earliest',
    })
    
    consumer.subscribe([TOPIC_NAME])
    
    count = 0
    while not stop_event.is_set():
        msg = consumer.poll(1.0)
        
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Consumer error: {msg.error()}")
                continue
        
        try:
            value = json.loads(msg.value().decode())
            count += 1
            if count % 10 == 0:
                print(f"Consumed {count} messages")
            if count >= 50:
                break
        except Exception as e:
            print(f"Error decoding message: {e}")
    
    consumer.close()
    print(f"Consumer finished - consumed {count} messages")

if __name__ == "__main__":
    stop_event = Event()
    
    p_thread = Thread(target=producer_thread)
    c_thread = Thread(target=consumer_thread, args=(stop_event,))
    
    p_thread.start()
    c_thread.start()
    
    p_thread.join()
    c_thread.join(timeout=20)
    
    if c_thread.is_alive():
        print("Consumer thread still running, stopping...")
        stop_event.set()
        c_thread.join(timeout=5)
    
    print("Test completed")