from loguru import logger

from models.audio import audio
from modules.extraction import get_features
from modules.utils import create_dataset


@logger.catch
def __main__():
    logger.info("Starting audio feature extraction module.")
    
    dataset = create_dataset()
    logger.info(f"Imported {len(dataset)} audio files from dataset.")
    
    audio_test = audio("path/to/audio/file.wav")
    audio_test.features = get_features(audio_test)
    logger.info(f"Extracted features: {audio_test.features}")


if __name__ == "__main__":
    __main__()
