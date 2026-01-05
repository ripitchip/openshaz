import pika
import json
import time
import os
import socket

# Identity of this specific worker instance
WORKER_ID = socket.gethostname()
RABBITMQ_URL = os.getenv("RABBITMQ_URL")

def callback(ch, method, properties, body):
    task = json.loads(body)
    print(f"[{WORKER_ID}] Received Task {task['task_index']} for Job {task['job_id']}")
    
    # Simulate heavy characteristic extraction work
    time.sleep(2) 
    
    print(f"[{WORKER_ID}] Finished Task {task['task_index']}")
    ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    params = pika.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    
    channel.queue_declare(queue='music_tasks', durable=True)
    
    # IMPORTANT: Don't give a worker more than 1 task at a time
    # This ensures work is spread across all 3 replicas!
    channel.basic_qos(prefetch_count=1)
    
    channel.basic_consume(queue='music_tasks', on_message_callback=callback)
    print(f" [*] Worker {WORKER_ID} waiting for tasks...")
    channel.start_consuming()

if __name__ == "__main__":
    main()