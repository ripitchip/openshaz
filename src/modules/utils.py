import shutil
import kagglehub
from pathlib import Path
from loguru import logger

from models.audio import audio
from modules.extraction import get_features


def _download_dataset():
    """Download the GTZAN dataset from Kaggle and store it in the data/raw directory."""
    target_dir = Path(__file__).parent.parent.parent / "data" / "raw"
    target_dir.mkdir(parents=True, exist_ok=True)

    path = kagglehub.dataset_download(
        "andradaolteanu/gtzan-dataset-music-genre-classification"
    )
    logger.debug("Path to dataset files:", path)

    shutil.copytree(path, target_dir, dirs_exist_ok=True)
    logger.info("Dataset copied to:", target_dir)


def _list_audio_filepaths() -> list[Path]:
    """List all audio files in the GTZAN dataset.

    :return: List of Paths to audio files
    """
    target_dir = Path(__file__).parent.parent.parent / "data" / "raw"
    audio_files = list(target_dir.rglob("*.wav"))
    logger.info(f"Found {len(audio_files)} audio files in the dataset.")
    return audio_files


def _import_audio_from_dataset() -> list[audio]:
    """Import audio files from the GTZAN dataset.

    :return: List of audio dataclass instances
    """
    audio_paths = _list_audio_filepaths()
    audio_files = []
    id_counter = 0
    for path in audio_paths:
        audio_file = audio(
            id=id_counter,
            name=path.stem,
            path=path,
        )
        audio_file.y, audio_file.sr = get_features(audio_file)
        audio_files.append(audio_file)
        id_counter += 1
    return audio_files


def create_dataset():
    """Create the GTZAN dataset from Kaggle if not already present."""
    target_dir = Path(__file__).parent.parent / "data" / "raw"
    if not target_dir.exists() or not any(target_dir.iterdir()):
        logger.info("GTZAN dataset not found locally. Downloading from Kaggle...")
        _download_dataset()
    else:
        logger.info("GTZAN dataset already exists locally. Skipping download.")

    return _import_audio_from_dataset()
