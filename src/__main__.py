from loguru import logger

from modules.extraction import get_features
from models.audio import audio


@logger.catch
def __main__():
    logger.info("Starting audio feature extraction module.")
    audio_test = audio("path/to/audio/file.wav")
    audio_test.features = get_features(audio_test)
    logger.info(f"Extracted features: {audio_test.features}")


if __name__ == "__main__":
    __main__()
