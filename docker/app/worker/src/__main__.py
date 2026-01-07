import json
import os
import signal
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pika
from loguru import logger
from models.audio import audio
from modules.database import (
    get_all_opensource_songs,
    get_query_song_by_name,
    init_database,
    store_opensource_song,
    store_query_song,
    wipe_all_tables,
)
from modules.dataset import convert_list_of_dicts_to_dataframe, create_dataframe
from modules.extraction import get_features
from modules.parser import parse_arguments
from modules.similarity import compare_different_metrics, measure_similarity
from modules.storage import check_connection as check_storage_connection
from modules.storage import (
    cleanup_downloaded_file,
    download_from_object_storage,
    wipe_all_buckets,
)
from sqlalchemy.exc import SQLAlchemyError

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
EXTRACTION_BUCKET_NAME = os.getenv("EXTRACTION_BUCKET_NAME", "opensource-songs")
SIMILARITY_BUCKET_NAME = os.getenv("SIMILARITY_BUCKET_NAME", "query-songs")

MAX_JOB_RETRIES = 3  # Maximum number of times a job can be requeued

cached_dataframe: pd.DataFrame = None


def get_retry_count(properties: Any) -> int:
    """Get the current retry count from message headers.

    :param properties: Message properties
    :return: Current retry count (0 if not set)
    """
    if properties.headers and "x-retry-count" in properties.headers:
        return int(properties.headers["x-retry-count"])
    return 0


def should_requeue(properties: Any) -> bool:
    """Check if a message should be requeued based on retry count.

    :param properties: Message properties
    :return: True if should requeue, False if max retries exceeded
    """
    retry_count = get_retry_count(properties)
    return retry_count < MAX_JOB_RETRIES


def requeue_with_retry_count(
    ch: Any, method: Any, properties: Any, body: bytes
) -> None:
    """Requeue a message with incremented retry count.

    :param ch: Channel
    :param method: Delivery method
    :param properties: Message properties
    :param body: Message body
    """
    retry_count = get_retry_count(properties) + 1

    if retry_count > MAX_JOB_RETRIES:
        logger.critical(
            f"Job exceeded maximum retries ({MAX_JOB_RETRIES}). Discarding message."
        )
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    else:
        logger.warning(f"Requeuing job (attempt {retry_count}/{MAX_JOB_RETRIES})")
        # Create new properties with updated retry count
        headers = properties.headers or {}
        headers["x-retry-count"] = retry_count

        new_properties = pika.BasicProperties(
            headers=headers,
            correlation_id=properties.correlation_id,
            reply_to=properties.reply_to,
            delivery_mode=2,
        )

        # Republish with updated headers
        ch.basic_publish(
            exchange="",
            routing_key=method.routing_key,
            properties=new_properties,
            body=body,
        )
        # Acknowledge original message
        ch.basic_ack(delivery_tag=method.delivery_tag)


def start_logging(is_debug: bool, is_worker: bool) -> None:
    """Initialize logging configuration.

    :param is_debug: Whether to enable debug logging
    :param is_worker: Whether the application is running in worker mode
    """
    logger.remove()
    log_level = "DEBUG" if is_debug else "INFO"
    logger.add(sys.stderr, level=log_level, colorize=True)
    if is_worker:
        log_dir = (
            Path("/app/worker_logs")
            if os.path.exists("/app")
            else Path("./worker_logs")
        )
        log_dir.mkdir(parents=True, exist_ok=True)
        logger.add(
            str(log_dir / "worker.log"),
            rotation="500 MB",
            compression="zip",
            level="DEBUG",
        )
    logger.info("Starting OpenShaz, Open-source audio similarity tool.")


def extract_features(ch: Any, method: Any, properties: Any, body: bytes) -> None:
    """Process audio feature extraction tasks from RabbitMQ.

    :param ch: Channel
    :param method: Delivery method
    :param properties: Message properties
    :param body: Message body
    """
    try:
        payload = json.loads(body)
        logger.info(f"Received extraction task: {payload}")

        music_name = payload.get("music_name")
        bucket_url = payload.get("bucket_url")

        audio_path = download_from_object_storage(bucket_url)

        test_audio = audio(name=Path(audio_path).stem, path=audio_path)
        extracted_features = get_features(test_audio)

        try:
            stored_song = store_opensource_song(
                name=music_name,
                bucket_url=bucket_url,
                features=extracted_features.tolist()
                if hasattr(extracted_features, "tolist")
                else list(extracted_features),
            )
            logger.info(f"Stored features for {music_name} with id {stored_song.id}")
        except SQLAlchemyError as db_error:
            logger.error(
                f"Database error storing opensource song: {db_error}. Requeuing job."
            )
            cleanup_downloaded_file(audio_path)
            requeue_with_retry_count(ch, method, properties, body)
            return
        except (ValueError, TypeError) as data_error:
            logger.error(f"Data validation error: {data_error}. Requeuing job.")
            cleanup_downloaded_file(audio_path)
            requeue_with_retry_count(ch, method, properties, body)
            return

        cleanup_downloaded_file(audio_path)

        result = {
            "job_id": payload.get("job_id"),
            "music_name": music_name,
            "bucket_url": bucket_url,
            "status": "extracted",
            "features": extracted_features.tolist()
            if hasattr(extracted_features, "tolist")
            else list(extracted_features),
        }

        if properties.reply_to:
            ch.basic_publish(
                exchange="",
                routing_key=properties.reply_to,
                properties=pika.BasicProperties(
                    correlation_id=properties.correlation_id,
                    delivery_mode=2,
                ),
                body=json.dumps(result),
            )

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Error processing extraction task: {e}")
        logger.info("Requeuing the extraction task")
        requeue_with_retry_count(ch, method, properties, body)


def process_similarity(ch: Any, method: Any, properties: Any, body: bytes) -> None:
    """Process audio similarity comparison tasks from RabbitMQ.

    :param ch: Channel
    :param method: Delivery method
    :param properties: Message properties
    :param body: Message body
    """
    try:
        payload = json.loads(body)
        logger.info(f"Received similarity task: {payload}")

        music_name = payload.get("music_name")
        bucket_url = payload.get("bucket_url")
        top_k = payload.get("top_k", 5)

        existing_query = get_query_song_by_name(music_name)

        if existing_query:
            logger.info(
                f"Query song {music_name} already exists, using cached features"
            )
            query_features = np.array(existing_query.features)
        else:
            logger.info(f"Query song {music_name} not found, extracting features")
            query_audio_path = download_from_object_storage(bucket_url)

            query_audio = audio(name=Path(query_audio_path).stem, path=query_audio_path)
            query_features = get_features(query_audio)

            try:
                stored_query = store_query_song(
                    name=music_name,
                    bucket_url=bucket_url,
                    features=query_features.tolist()
                    if hasattr(query_features, "tolist")
                    else list(query_features),
                )
                logger.info(f"Stored query song {music_name} with id {stored_query.id}")
            except SQLAlchemyError as db_error:
                logger.error(
                    f"Database error storing query song: {db_error}. Requeuing job."
                )
                cleanup_downloaded_file(query_audio_path)
                requeue_with_retry_count(ch, method, properties, body)
                return
            except (ValueError, TypeError) as data_error:
                logger.error(
                    f"Data validation error for query song: {data_error}. Requeuing job."
                )
                cleanup_downloaded_file(query_audio_path)
                requeue_with_retry_count(ch, method, properties, body)
                return

            cleanup_downloaded_file(query_audio_path)

        try:
            global cached_dataframe
            if cached_dataframe is None:
                cached_dataframe = convert_list_of_dicts_to_dataframe(
                    get_all_opensource_songs()
                )

            if cached_dataframe is None or cached_dataframe.empty:
                logger.warning("No opensource songs in database for comparison")
                similar = []
            else:
                similar = measure_similarity(
                    df=cached_dataframe,
                    audio=query_features,
                    metric="cosine",
                    top_k=top_k,
                )
        except SQLAlchemyError as db_error:
            logger.error(
                f"Database error fetching opensource songs: {db_error}. Requeuing job."
            )
            requeue_with_retry_count(ch, method, properties, body)
            return
        except (pd.errors.EmptyDataError, KeyError, ValueError) as data_error:
            logger.error(f"Dataframe processing error: {data_error}. Requeuing job.")
            requeue_with_retry_count(ch, method, properties, body)
            return
        except Exception as similarity_error:
            logger.error(
                f"Similarity computation error: {similarity_error}. Requeuing job."
            )
            requeue_with_retry_count(ch, method, properties, body)
            return

        result = {
            "job_id": payload.get("job_id"),
            "query_song": music_name,
            "bucket_url": bucket_url,
            "status": "completed",
            "similar": similar,
        }

        if properties.reply_to:
            ch.basic_publish(
                exchange="",
                routing_key=properties.reply_to,
                properties=pika.BasicProperties(
                    correlation_id=properties.correlation_id,
                    delivery_mode=2,
                ),
                body=json.dumps(result),
            )

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Error processing similarity task: {e}")
        logger.info("Requeuing the similarity task")
        requeue_with_retry_count(ch, method, properties, body)


@logger.catch
def main():
    args = parse_arguments()

    if args.command == "manual":
        start_logging(is_debug=args.debug, is_worker=False)
        logger.info("Starting manual mode...")
        start_time = time.time()
        df = create_dataframe(
            limit=args.limit,
            log_level="DEBUG" if args.debug else "INFO",
            multi=args.multi,
            recreate=args.recreate,
            source=args.source,
            fma_size=args.fma_size,
            force=args.force,
        )

        if args.compare_metrics:
            compare_different_metrics(
                df=df,
                test_size=0.2,
                top_k=args.top_k,
                random_state=42,
            )

        if args.test_audio_path is not None:
            test_audio_path = Path(args.test_audio_path)
            test_audio = audio(name=test_audio_path.stem, path=test_audio_path)
            test_audio.features = get_features(test_audio)
            measure_similarity(
                df=df,
                audio=test_audio.features,
                metric=args.metric,
                top_k=args.top_k,
            )
        elapsed_time = time.time() - start_time
        logger.info(f"Execution completed in {elapsed_time:.2f} seconds.")

    elif args.command == "worker":
        start_logging(is_debug=args.debug, is_worker=True)
        logger.info("Starting worker mode...")

        max_retries = 3
        wait_seconds_before_retry = 5

        for attempt in range(1, max_retries + 1):
            try:
                init_database()
                logger.info("Successfully connected to the database")
                break
            except Exception as e:
                logger.warning(
                    f"Database initialization attempt {attempt}/{max_retries} failed: {e}"
                )
                if attempt == max_retries:
                    logger.critical(
                        f"Failed to initialize database after {max_retries} attempts. Cannot continue."
                    )
                    return
                time.sleep(wait_seconds_before_retry)

        connection = None
        for attempt in range(1, max_retries + 1):
            try:
                connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
                channel = connection.channel()
                logger.info("Successfully connected to RabbitMQ")
                break
            except Exception as e:
                logger.warning(
                    f"RabbitMQ connection attempt {attempt}/{max_retries} failed: {e}"
                )
                if attempt == max_retries:
                    logger.critical(
                        f"Failed to connect to RabbitMQ after {max_retries} attempts. Cannot continue."
                    )
                    return
                time.sleep(wait_seconds_before_retry)

        for attempt in range(1, max_retries + 1):
            try:
                if check_storage_connection():
                    logger.info("Successfully connected to object storage")
                    break
                else:
                    raise ConnectionError("Object storage health check failed")
            except Exception as e:
                logger.warning(
                    f"Object storage connection attempt {attempt}/{max_retries} failed: {e}"
                )
                if attempt == max_retries:
                    logger.critical(
                        f"Failed to connect to object storage after {max_retries} attempts. Cannot continue."
                    )
                    if connection and connection.is_open:
                        connection.close()
                    return
                time.sleep(wait_seconds_before_retry)

        if args.wipe_database:
            wipe_all_tables()
        if args.wipe_storage:
            wipe_all_buckets()

        extraction_queue_name = "audio_extraction_tasks"
        similarity_queue_name = "audio_similarity_tasks"

        channel.queue_declare(queue=extraction_queue_name, durable=True)
        channel.queue_declare(queue=similarity_queue_name, durable=True)
        logger.info(
            f"Declared queues: {extraction_queue_name}, {similarity_queue_name}"
        )

        channel.basic_qos(prefetch_count=1)

        channel.basic_consume(
            queue=extraction_queue_name,
            on_message_callback=extract_features,
            auto_ack=False,
        )
        channel.basic_consume(
            queue=similarity_queue_name,
            on_message_callback=process_similarity,
            auto_ack=False,
        )

        def signal_handler(signum, frame):
            logger.info("Received shutdown signal, closing connection...")
            if connection and connection.is_open:
                connection.close()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        logger.info("Worker is waiting for messages. To exit press CTRL+C")
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        finally:
            if connection and connection.is_open:
                connection.close()
                logger.info("Connection closed")


if __name__ == "__main__":
    main()
