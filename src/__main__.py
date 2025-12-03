import argparse
import sys
from loguru import logger

from modules.dataset import create_dataset


@logger.catch
def __main__():
    parser = argparse.ArgumentParser(description="Audio Feature Extraction Module")
    parser.add_argument("--debug", action="store_true", help="Run with debug console")
    parser.add_argument(
        "--limit", action="store_true", help="Limit number of audio files imported"
    )
    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.debug else "INFO")

    logger.info("Starting audio feature extraction module.")
    dataset = create_dataset(limit=10 if args.limit else None)
    logger.info(f"Imported {len(dataset)} audio files from dataset.")


if __name__ == "__main__":
    __main__()
