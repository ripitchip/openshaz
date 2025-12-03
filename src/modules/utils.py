import shutil
import kagglehub
from pathlib import Path
from loguru import logger

from models.audio import audio
from modules.extraction import get_features
from tqdm import tqdm


def _download_dataset():
    """Download the GTZAN dataset from Kaggle and store it in the data/raw directory."""
    target_dir = Path(__file__).parent.parent.parent / "data" / "raw"
    logger.debug(f"Target directory for dataset: {target_dir.as_posix()}")

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
    logger.debug(f"Listing audio files in directory: {target_dir.as_posix()}")

    audio_files = list(target_dir.rglob("*.wav"))
    logger.info(f"Found {len(audio_files)} audio files in the dataset.")
    return audio_files


def _import_audio_from_dataset(limit: int) -> list[audio]:
    """Import audio files from the GTZAN dataset.

    :param limit: Optional limit on the number of audio files to import
    :return: List of audio dataclass instances
    """

    audio_paths = _list_audio_filepaths()
    if limit is not None:
        audio_paths = audio_paths[:limit]
        logger.info(f"Limiting import to first {limit} audio files.")
    audio_files = []
    id_counter = 0
    for path in tqdm(audio_paths, desc="Processing audio files"):
        audio_file = audio(
            id=id_counter,
            name=path.stem,
            path=path,
        )
        audio_file.features = get_features(audio_file)
        audio_files.append(audio_file)
        logger.debug(
            f"Imported audio file: {audio_file.name}, assigned ID: {audio_file.id}"
        )
        id_counter += 1
    return audio_files


def create_dataset(limit: int | None = None) -> list[audio]:
    """Create the GTZAN dataset from Kaggle if not already present.

    :param limit: Optional limit on the number of audio files to import
    :return: List of audio dataclass instances
    """
    target_dir = Path(__file__).parent.parent.parent / "data" / "raw"
    logger.debug(f"Checking for dataset in directory: {target_dir.as_posix()}")

    if not target_dir.exists() or not any(target_dir.iterdir()):
        logger.info("GTZAN dataset not found locally. Downloading from Kaggle...")
        _download_dataset()
    else:
        logger.info("GTZAN dataset already exists locally. Skipping download.")

    return _import_audio_from_dataset(limit)
