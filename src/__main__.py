import sys
from loguru import logger

from modules.utils import create_dataset


@logger.catch
def __main__():
    logger.remove()
    logger.add(sys.stderr, level="DEBUG")

    logger.info("Starting audio feature extraction module.")
    dataset = create_dataset(limit=15)
    logger.info(f"Imported {len(dataset)} audio files from dataset.")


if __name__ == "__main__":
    __main__()
