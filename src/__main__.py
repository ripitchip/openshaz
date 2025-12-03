import sys
import time
import argparse
from loguru import logger

from modules.dataset import create_dataset


@logger.catch
def __main__():
    start_time = time.time()

    parser = argparse.ArgumentParser(description="Audio Feature Extraction Module")
    parser.add_argument("--debug", action="store_true", help="Run with debug console")
    parser.add_argument("--multi", action="store_true", help="Enable multiprocessing")
    parser.add_argument(
        "--limit", action="store_true", help="Limit number of audio files imported"
    )
    args = parser.parse_args()

    logger.remove()
    log_level = "DEBUG" if args.debug else "INFO"
    logger.add(sys.stderr, level=log_level)

    logger.info("Starting audio feature extraction module.")
    dataset = create_dataset(
        limit=30 if args.limit else None,
        log_level=log_level,
        use_multiprocessing=args.multi,
    )
    logger.info(f"Imported {len(dataset)} audio files from dataset.")

    elapsed_time = time.time() - start_time
    logger.info(f"Execution completed in {elapsed_time:.2f} seconds.")


if __name__ == "__main__":
    __main__()
