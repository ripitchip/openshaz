import os
import signal
import sys
import time
from pathlib import Path
from typing import Any

import pika
from loguru import logger
from models.audio import audio
from modules.dataset import create_dataframe
from modules.extraction import get_features
from modules.parser import parse_arguments
from modules.similarity import compare_different_metrics, measure_similarity


def start_logging(is_debug: bool, is_worker: bool) -> None:
    """Initialize logging configuration.

    :param is_debug: Whether to enable debug logging
    :param is_worker: Whether the application is running in worker mode
    """
    logger.remove()
    log_level = "DEBUG" if is_debug else "INFO"
    logger.add(sys.stderr, level=log_level)
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
        logger.info(f"Received extraction task: {body.decode()}")
        # TODO: fetch mp3 / wav

        # TODO: extract features

        # IF ADD
        # TODO: add to DB postgres, debug will be a mockup only logging result
        # IF QUERY
        # TODO: add new task in pool to measure similarity, with metrics into the task itself, maybe a call to the api to create the new job
        logger.warning("Feature extraction not yet implemented")

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Error processing extraction task: {e}")
        logger.info("Requeuing the extraction task")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def process_similarity(ch: Any, method: Any, properties: Any, body: bytes) -> None:
    """Process audio similarity comparison tasks from RabbitMQ.

    :param ch: Channel
    :param method: Delivery method
    :param properties: Message properties
    :param body: Message body
    """
    try:
        logger.info(f"Received similarity task: {body.decode()}")
        # TODO: Get audio metrics from task

        # TODO: fetch audio features from DB postgres

        # TODO: process similarity

        # TODO: return the n-similar audio references (ids)
        logger.warning("Similarity processing not yet implemented")

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Error processing similarity task: {e}")
        logger.info("Requeuing the similarity task")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


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

        RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")

        connection = None
        try:
            connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
            channel = connection.channel()
            logger.info("Successfully connected to RabbitMQ")
        except Exception as e:
            logger.critical(f"Failed to connect to RabbitMQ: {e}")
            return

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
