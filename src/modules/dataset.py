import json
import shutil
import sys
from multiprocessing import Pool, cpu_count
from pathlib import Path

import kagglehub
import numpy as np
import pandas as pd
from loguru import logger
from tqdm import tqdm

from models.audio import audio
from modules.extraction import get_features

DEFAULT_DATAFRAME_COLUMNS = [
    "id",
    "name",
    "length",
    "chroma_stft_mean",
    "chroma_stft_var",
    "rms_mean",
    "rms_var",
    "spectral_centroid_mean",
    "spectral_centroid_var",
    "spectral_bandwidth_mean",
    "spectral_bandwidth_var",
    "rolloff_mean",
    "rolloff_var",
    "zero_crossing_rate_mean",
    "zero_crossing_rate_var",
    "harmony_mean",
    "perceptr_mean",
    "harmony_var",
    "perceptr_var",
    "tempo",
    "mfcc1_mean",
    "mfcc1_var",
    "mfcc2_mean",
    "mfcc2_var",
    "mfcc3_mean",
    "mfcc3_var",
    "mfcc4_mean",
    "mfcc4_var",
    "mfcc5_mean",
    "mfcc5_var",
    "mfcc6_mean",
    "mfcc6_var",
    "mfcc7_mean",
    "mfcc7_var",
    "mfcc8_mean",
    "mfcc8_var",
    "mfcc9_mean",
    "mfcc9_var",
    "mfcc10_mean",
    "mfcc10_var",
    "mfcc11_mean",
    "mfcc11_var",
    "mfcc12_mean",
    "mfcc12_var",
    "mfcc13_mean",
    "mfcc13_var",
    "mfcc14_mean",
    "mfcc14_var",
    "mfcc15_mean",
    "mfcc15_var",
    "mfcc16_mean",
    "mfcc16_var",
    "mfcc17_mean",
    "mfcc17_var",
    "mfcc18_mean",
    "mfcc18_var",
    "mfcc19_mean",
    "mfcc19_var",
    "mfcc20_mean",
    "mfcc20_var",
]


def _download_dataset() -> None:
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


def _init_worker(log_level: str) -> None:
    """Initialize worker process with proper logger configuration.

    :param log_level: Log level to set (DEBUG or INFO)
    """
    logger.remove()
    logger.add(sys.stderr, level=log_level)


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


def _get_dataframe_cache_path(limit: int | None = None) -> Path:
    """Get the path to the cached dataframe CSV file.

    :param limit: Optional limit used to differentiate cache files
    :return: Path to CSV cache file
    """
    cache_dir = Path(__file__).parent.parent.parent / "data" / "processed"
    cache_dir.mkdir(parents=True, exist_ok=True)

    if limit is not None:
        return cache_dir / f"dataset_features_{limit}.csv"
    return cache_dir / "dataset_features_full.csv"


def _save_dataframe(dataframe: pd.DataFrame, limit: int | None = None) -> None:
    """Save the dataframe to a CSV file.

    :param dataframe: Pandas DataFrame containing audio features
    :param limit: Optional limit used to differentiate cache files
    """
    csv_path = _get_dataframe_cache_path(limit)
    dataframe.to_csv(csv_path, index=False)
    logger.info(f"Dataframe cached to: {csv_path}")


def _get_dataset_cache_path(limit: int | None = None) -> Path:
    """Get the path to the cached dataset file.

    :param limit: Optional limit used to differentiate cache files
    :return: Path to cache file
    """
    cache_dir = Path(__file__).parent.parent.parent / "data" / "processed"
    cache_dir.mkdir(parents=True, exist_ok=True)

    if limit is not None:
        return cache_dir / f"dataset_cache_{limit}.json"
    return cache_dir / "dataset_cache_full.json"


def _save_dataset(dataset: list[audio], limit: int | None = None) -> None:
    """Save the dataset to a JSON file and dataframe to CSV.

    :param dataset: List of audio dataclass instances
    :param limit: Optional limit used to differentiate cache files
    """
    cache_path = _get_dataset_cache_path(limit)

    json_data = []
    for audio_file in dataset:
        json_data.append(
            {
                "id": audio_file.id,
                "name": audio_file.name,
                "path": str(audio_file.path),
                "features": audio_file.features.tolist()
                if audio_file.features is not None
                else None,
            }
        )

    with open(cache_path, "w") as f:
        json.dump(json_data, f, indent=2)
    logger.info(f"Dataset cached to: {cache_path}")


def _load_dataset(limit: int | None = None) -> list[audio] | None:
    """Load the dataset from a JSON file if it exists.

    :param limit: Optional limit used to differentiate cache files
    :return: List of audio dataclass instances or None if cache doesn't exist
    """
    cache_path = _get_dataset_cache_path(limit)
    if cache_path.exists():
        with open(cache_path, "r") as f:
            json_data = json.load(f)

        dataset = []
        for item in json_data:
            audio_file = audio(
                id=item["id"],
                name=item["name"],
                path=Path(item["path"]),
                features=np.array(item["features"])
                if item["features"] is not None
                else None,
            )
            dataset.append(audio_file)

        logger.info(f"Loaded cached dataset from: {cache_path}")
        return dataset
    return None


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


def get_audio_dataset(
    limit: int | None = None,
    log_level: str = "INFO",
    use_multiprocessing: bool = True,
    recreate: bool = False,
) -> list[audio]:
    """Create the GTZAN dataset from Kaggle if not already present.

    :param limit: Optional limit on the number of audio files to import
    :param log_level: Log level for worker processes (default: INFO)
    :param use_multiprocessing: Whether to use parallel processing (default: True)
    :param recreate: Force recreation of dataset even if cache exists (default: False)
    :return: List of audio dataclass instances
    """
    if not recreate:
        cached_dataset = _load_dataset(limit)
        if cached_dataset is not None:
            return cached_dataset

    target_dir = Path(__file__).parent.parent.parent / "data" / "raw"
    logger.debug(f"Checking for dataset in directory: {target_dir.as_posix()}")

    if not target_dir.exists() or not any(target_dir.iterdir()):
        logger.info("GTZAN dataset not found locally. Downloading from Kaggle...")
        _download_dataset()
    else:
        logger.info("GTZAN dataset already exists locally. Skipping download.")

    dataset = _import_audio_from_dataset(
        limit, log_level=log_level, use_multiprocessing=use_multiprocessing
    )

    _save_dataset(dataset, limit)
    return dataset


def get_features_dataframe(
    dataset: list[audio] = None, limit: int | None = None, recreate: bool = False
) -> pd.DataFrame:
    """Convert the dataset into a pandas DataFrame.

    :param dataset: List of audio dataclass instances
    :param limit: Optional limit used to differentiate cache files
    :param recreate: Force recreation of dataframe even if cache exists (default: False)
    :return: DataFrame containing audio features
    """
    if not recreate:
        csv_path = _get_dataframe_cache_path(limit)
        if csv_path.exists():
            df = pd.read_csv(csv_path)
            logger.info(f"Loaded cached DataFrame from: {csv_path}")
            return df

    data = []
    for audio_file in dataset:
        if audio_file.features is not None:
            row = [audio_file.id, audio_file.name] + audio_file.features.tolist()
            data.append(row)

    df = pd.DataFrame(data, columns=DEFAULT_DATAFRAME_COLUMNS)
    logger.info("Converted dataset to pandas DataFrame.")

    _save_dataframe(df, limit)
    return df
