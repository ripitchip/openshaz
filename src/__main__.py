from loguru import logger

from modules.utils import create_dataset


@logger.catch
def __main__():
    logger.info("Starting audio feature extraction module.")

    dataset = create_dataset()
    logger.info(f"Imported {len(dataset)} audio files from dataset.")


if __name__ == "__main__":
    __main__()
