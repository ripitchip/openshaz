from fastapi import FastAPI
import pika
import json
import uuid
import os
import random

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
    import random # Add this at the top


@app.post("/create_fake_tasks")
async def create_fake_tasks(count: int = 20):
    """
    Generates a bulk amount of random tasks to test RabbitMQ and Worker scaling.
    """
    fake_musics = ["Bohemian_Rhapsody", "Stairway_to_Heaven", "Imagine", "Like_a_Rolling_Stone"]
    job_id = f"fake-{uuid.uuid4().hex[:8]}"
    
    conn, channel = get_rabbitmq_channel()

    for i in range(count):
        music_name = random.choice(fake_musics)
        task_payload = {
            "job_id": job_id,
            "task_index": i,
            "total_tasks": count,
            "music_name": music_name,
            "payload_data": f"Fake_Data_Load_{random.getrandbits(32)}" 
        }
        
        # Correct routing key to 'music_tasks' so the worker can consume the tasks
        channel.basic_publish(
            exchange='',
            routing_key='music_tasks',  # Use the correct queue name here
            body=json.dumps(task_payload),
            properties=pika.BasicProperties(delivery_mode=2)  # Persistent message
        )

    conn.close()
    return {
        "status": "success", 
        "message": f"Sent {count} fake tasks to RabbitMQ",
        "batch_id": job_id
    }
