#!/usr/bin/env python3

from kafka import KafkaProducer, KafkaConsumer
import json
import time
from threading import Thread

TOPIC_NAME = f'test-producer-consumer-threads-{int(time.time())}'

def producer_thread():
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode()
    )
    
    for i in range(100):
        data = {"counter": i, "timestamp": time.time()}
        producer.send(TOPIC_NAME, value=data)
        if i % 10 == 0:
            print(f"Produced {i} messages")
        time.sleep(0.1)
    
    producer.flush()
    producer.close()
    print("Producer finished")

def consumer_thread():
    time.sleep(2)
    
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=['localhost:9092'],
        group_id=f'test-consumer-group-{int(time.time())}',
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode()) if m else None
    )
    
    count = 0
    for message in consumer:
        if message.value is not None:
            count += 1
            if count % 10 == 0:
                print(f"Consumed {count} messages")
            if count >= 50:
                break
    
    consumer.close()
    print("Consumer finished")

if __name__ == "__main__":
    p_thread = Thread(target=producer_thread)
    c_thread = Thread(target=consumer_thread)
    
    p_thread.start()
    c_thread.start()
    
    p_thread.join()
    c_thread.join()
    
    print("Test completed")