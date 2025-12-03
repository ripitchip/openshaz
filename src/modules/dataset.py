import sys
import shutil
import kagglehub
from tqdm import tqdm
from pathlib import Path
from loguru import logger
from multiprocessing import Pool, cpu_count

from models.audio import audio
from modules.extraction import get_features


def _init_worker(log_level: str):
    """Initialize worker process with proper logger configuration.

    :param log_level: Log level to set (DEBUG or INFO)
    """
    logger.remove()
    logger.add(sys.stderr, level=log_level)


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

    audio_files = sorted(target_dir.rglob("*.wav"))
    logger.info(f"Found {len(audio_files)} audio files in the dataset.")
    return audio_files


def _process_single_audio(args: tuple[int, Path]) -> audio:
    """Process a single audio file (for multiprocessing).

    :param args: Tuple of (id, path)
    :return: audio dataclass instance with features
    """
    id_counter, path = args
    audio_file = audio(
        id=id_counter,
        name=path.stem,
        path=path,
    )
    audio_file.features = get_features(audio_file)
    return audio_file


def _import_audio_from_dataset(
    limit: int, log_level: str = "INFO", use_multiprocessing: bool = True
) -> list[audio]:
    """Import audio files from the GTZAN dataset.

    :param limit: Optional limit on the number of audio files to import
    :param log_level: Log level for worker processes (default: INFO)
    :param use_multiprocessing: Whether to use parallel processing (default: True)
    :return: List of audio dataclass instances
    """

    audio_paths = _list_audio_filepaths()
    if limit is not None:
        audio_paths = audio_paths[:limit]
        logger.info(f"Limiting import to first {limit} audio files.")

    if use_multiprocessing:
        num_processes = cpu_count()
        logger.info(f"Using {num_processes} processes for parallel extraction")

        with Pool(
            processes=num_processes, initializer=_init_worker, initargs=(log_level,)
        ) as pool:
            args = [(i, path) for i, path in enumerate(audio_paths)]
            audio_files = list(
                tqdm(
                    pool.imap(_process_single_audio, args),
                    total=len(audio_paths),
                    desc="Processing audio files",
                )
            )
    else:
        audio_files = []
        for id_counter, path in enumerate(
            tqdm(audio_paths, desc="Processing audio files")
        ):
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

    return audio_files


def create_dataset(
    limit: int | None = None, log_level: str = "INFO", use_multiprocessing: bool = True
) -> list[audio]:
    """Create the GTZAN dataset from Kaggle if not already present.

    :param limit: Optional limit on the number of audio files to import
    :param log_level: Log level for worker processes (default: INFO)
    :param use_multiprocessing: Whether to use parallel processing (default: True)
    :return: List of audio dataclass instances
    """
    target_dir = Path(__file__).parent.parent.parent / "data" / "raw"
    logger.debug(f"Checking for dataset in directory: {target_dir.as_posix()}")

    if not target_dir.exists() or not any(target_dir.iterdir()):
        logger.info("GTZAN dataset not found locally. Downloading from Kaggle...")
        _download_dataset()
    else:
        logger.info("GTZAN dataset already exists locally. Skipping download.")

    return _import_audio_from_dataset(
        limit, log_level=log_level, use_multiprocessing=use_multiprocessing
    )
