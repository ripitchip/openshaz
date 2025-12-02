from loguru import logger

from extraction import Extraction


@logger.catch
def __main__():
    logger.info("Starting audio feature extraction module.")
    Extraction.extract_features("path/to/audio/file.wav")


if __name__ == "__main__":
    __main__()
