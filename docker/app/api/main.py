from fastapi import FastAPI
import pika
import json
import uuid
import os

app = FastAPI()

# Get RabbitMQ URL from Environment (defined in your compose)
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672//")

def get_rabbitmq_channel():
    params = pika.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue='music_tasks', durable=True)
    return connection, channel

@app.post("/create-job/{music_name}")
async def create_job(music_name: str, n_tasks: int = 10):
    """
    Creates a master job and splits it into 'n' parallel tasks.
    """
    job_id = str(uuid.uuid4())
    conn, channel = get_rabbitmq_channel()

    for i in range(n_tasks):
        task_payload = {
            "job_id": job_id,
            "task_index": i,
            "total_tasks": n_tasks,
            "music_name": music_name,
            "payload_data": f"Chunk_{i}_data" 
        }
        
        channel.basic_publish(
            exchange='',
            routing_key='music_tasks',
            body=json.dumps(task_payload),
            properties=pika.BasicProperties(delivery_mode=2) # Persistent
        )

    conn.close()
    return {"status": "success", "job_id": job_id, "tasks_created": n_tasks}