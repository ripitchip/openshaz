import pika
import time
import os
from loguru import logger

# Configure loguru logger (optional - you can customize it as needed)
logger.add("/worker_logs/worker.log", rotation="500 MB", compression="zip", level="INFO")

def on_message(ch, method, properties, body):
    # Log that a task was received
    logger.info(f"Received task: {body.decode()}")
    
    # Simulating work by sleeping for 5 seconds
    time.sleep(5)  # Simulate task processing
    
    # Log that the task was finished
    logger.info("Task finished")
    
    # Acknowledge the task was completed
    ch.basic_ack(delivery_tag=method.delivery_tag)

def start_worker():
    # Get RabbitMQ URL from environment variables
    rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://guest:guest@rabbitmq:5672/')  # Default to localhost if not set
    
    # Log the connection attempt
    logger.info(f"Connecting to RabbitMQ at {rabbitmq_url}")
    
    # Establish connection to RabbitMQ using the URL from the environment variable
    try:
        connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_url))
        channel = connection.channel()
        logger.info("Successfully connected to RabbitMQ")
    except Exception as e:
        logger.error(f"Failed to connect to RabbitMQ: {e}")
        return
    
    # Declare the queue (make sure the queue exists in RabbitMQ)
    queue_name = 'music_tasks'
    channel.queue_declare(queue=queue_name, durable=True)
    logger.info(f"Declared queue: {queue_name}")
    
    # Define the callback that will be called for each received message
    channel.basic_consume(
        queue=queue_name, 
        on_message_callback=on_message
    )
    
    logger.info("Worker is waiting for messages. To exit press CTRL+C")
    channel.start_consuming()

if __name__ == '__main__':
    start_worker()
