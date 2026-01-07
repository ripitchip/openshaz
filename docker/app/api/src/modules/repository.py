import json
import os
import time
import uuid
from typing import Any, BinaryIO, Dict

import boto3
import pika
from botocore.client import Config
from botocore.exceptions import ClientError
from loguru import logger

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
EXTRACTION_QUEUE = "audio_extraction_tasks"
SIMILARITY_QUEUE = "audio_similarity_tasks"

OBJECT_STORAGE_URL = os.getenv("OBJECT_STORAGE_URL", "http://localhost:9000")
OBJECT_STORAGE_ACCESS_KEY = os.getenv("OBJECT_STORAGE_ACCESS_KEY")
OBJECT_STORAGE_SECRET_KEY = os.getenv("OBJECT_STORAGE_SECRET_KEY")
OBJECT_STORAGE_REGION = os.getenv("OBJECT_STORAGE_REGION", "eu-west-1")

OPENSOURCE_BUCKET = os.getenv("EXTRACTION_BUCKET_NAME", "opensource-songs")
QUERY_BUCKET = os.getenv("SIMILARITY_BUCKET_NAME", "query-songs")


def _get_s3_client():
    """Initialize and return S3 client for RustFS/MinIO."""
    if not OBJECT_STORAGE_ACCESS_KEY or not OBJECT_STORAGE_SECRET_KEY:
        raise ValueError(
            "OBJECT_STORAGE_ACCESS_KEY and OBJECT_STORAGE_SECRET_KEY must be set"
        )

    return boto3.client(
        "s3",
        endpoint_url=OBJECT_STORAGE_URL,
        aws_access_key_id=OBJECT_STORAGE_ACCESS_KEY,
        aws_secret_access_key=OBJECT_STORAGE_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name=OBJECT_STORAGE_REGION,
    )


def upload_to_object_storage(
    file_obj: BinaryIO, file_name: str, bucket_name: str
) -> str:
    """Upload a file to object storage and return the URL/path.

    :param file_obj: File object to upload
    :param file_name: Name of the file
    :param bucket_name: Target bucket
    :return: Object storage URL/path
    """
    s3_client = _get_s3_client()

    try:
        try:
            s3_client.head_bucket(Bucket=bucket_name)
        except ClientError:
            logger.info(f"Bucket {bucket_name} doesn't exist, creating it")
            s3_client.create_bucket(Bucket=bucket_name)

        object_url = f"{OBJECT_STORAGE_URL}/{bucket_name}/{file_name}"
        try:
            s3_client.head_object(Bucket=bucket_name, Key=file_name)
            logger.info(f"File already exists in storage: {object_url}")
            return object_url
        except ClientError as e:
            if e.response["Error"]["Code"] != "404":
                raise

        logger.info(f"Uploading {file_name} to bucket {bucket_name}")
        s3_client.upload_fileobj(file_obj, bucket_name, file_name)
        logger.info(f"File uploaded successfully: {object_url}")
        return object_url

    except ClientError as e:
        logger.error(f"Failed to upload to object storage: {e}")
        raise


def upload_file_from_path(file_path: str, file_name: str, bucket_name: str) -> str:
    """Upload a file from local path to object storage.

    :param file_path: Local path to file
    :param file_name: Name to use in object storage
    :param bucket_name: Target bucket
    :return: Object storage URL/path
    """
    with open(file_path, "rb") as f:
        return upload_to_object_storage(f, file_name, bucket_name)


def delete_from_object_storage(file_name: str, bucket_name: str) -> None:
    """Delete a file from object storage.

    :param file_name: Name of the file to delete
    :param bucket_name: Bucket containing the file
    """
    s3_client = _get_s3_client()

    try:
        logger.info(f"Deleting {file_name} from bucket {bucket_name}")
        s3_client.delete_object(Bucket=bucket_name, Key=file_name)
        logger.info(f"File deleted successfully: {bucket_name}/{file_name}")
    except ClientError as e:
        logger.error(f"Failed to delete from object storage: {e}")
        raise


def _rpc_call(
    queue_name: str, payload: Dict[str, Any], timeout: int = 30
) -> Dict[str, Any]:
    """Send a message and wait for the worker reply using RabbitMQ RPC pattern."""
    connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)

    result = channel.queue_declare(queue="", exclusive=True)
    callback_queue = result.method.queue
    correlation_id = str(uuid.uuid4())
    response: Dict[str, Any] | None = None

    def on_response(ch, method, props, body):
        nonlocal response
        if props.correlation_id == correlation_id:
            response = json.loads(body)
            ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(
        queue=callback_queue, on_message_callback=on_response, auto_ack=False
    )

    channel.basic_publish(
        exchange="",
        routing_key=queue_name,
        properties=pika.BasicProperties(
            reply_to=callback_queue,
            correlation_id=correlation_id,
            delivery_mode=2,
        ),
        body=json.dumps(payload),
    )

    start = time.time()
    try:
        while response is None:
            connection.process_data_events(time_limit=1)
            if time.time() - start > timeout:
                raise TimeoutError(
                    f"Timeout waiting for response on queue {queue_name}"
                )
    finally:
        if connection.is_open:
            connection.close()
    return response


def send_extraction_task(
    music_name: str, bucket_url: str, timeout: int = 45
) -> Dict[str, Any]:
    """Send extraction task with object storage URL."""
    payload = {
        "job_id": str(uuid.uuid4()),
        "type": "extraction",
        "music_name": music_name,
        "bucket_url": bucket_url,
    }
    return _rpc_call(EXTRACTION_QUEUE, payload, timeout=timeout)


def send_similarity_task(
    music_name: str, bucket_url: str, top_k: int = 5, timeout: int = 60
) -> Dict[str, Any]:
    """Send similarity task with query song URL (extract features + search)."""
    payload = {
        "job_id": str(uuid.uuid4()),
        "type": "similarity",
        "music_name": music_name,
        "bucket_url": bucket_url,
        "top_k": top_k,
    }
    return _rpc_call(SIMILARITY_QUEUE, payload, timeout=timeout)
