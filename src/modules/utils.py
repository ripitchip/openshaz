import shutil
import kagglehub
from pathlib import Path
from loguru import logger


def create_dataset():
    """Create the GTZAN dataset from Kaggle if not already present."""
    target_dir = Path(__file__).parent.parent / "data" / "raw"
    if not target_dir.exists() or not any(target_dir.iterdir()):
        logger.info("GTZAN dataset not found locally. Downloading from Kaggle...")
        _download_dataset()
    else:
        logger.info("GTZAN dataset already exists locally. Skipping download.")


def _download_dataset():
    """Download the GTZAN dataset from Kaggle and store it in the data/raw directory."""
    target_dir = Path(__file__).parent.parent / "data" / "raw"
    target_dir.mkdir(parents=True, exist_ok=True)

    path = kagglehub.dataset_download(
        "andradaolteanu/gtzan-dataset-music-genre-classification"
    )
    logger.debug("Path to dataset files:", path)

    shutil.copytree(path, target_dir, dirs_exist_ok=True)
    logger.info("Dataset copied to:", target_dir)


def _list_audio_files():
    """List all audio files in the GTZAN dataset."""
    target_dir = Path(__file__).parent.parent / "data" / "raw"
    audio_files = list(target_dir.rglob("*.wav"))
    logger.info(f"Found {len(audio_files)} audio files in the dataset.")
    return audio_files


def _import_audio_from_dataset():
    """Import audio files from the GTZAN dataset."""
    audio_files = _list_audio_files()
    audio_data = []
    # for file in audio_files:
    # y, sr = librosa.load(file)
    # y, _ = librosa.effects.trim(y)
    # audio_data.append((file, y, sr))
    # return audio_data
