from loguru import logger
import time
import os

# Configure logger
log_path = os.getenv("WORKER_LOG_PATH", "/worker_logs/worker.log")
logger.add(log_path, rotation="10 MB", retention="7 days", level="INFO", backtrace=True, diagnose=True)

counter = 0

while True:
    counter += 1
    logger.info(f"Worker value: {counter}")
    time.sleep(1)
