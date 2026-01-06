import json
import shutil
import sys
import zipfile
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


def _download_gtzan() -> Path:
    """Download the GTZAN dataset from Kaggle.

    :return: Path to the dataset directory
    """
    target_dir = Path(__file__).parent.parent.parent / "data" / "raw" / "gtzan"
    target_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Downloading GTZAN dataset from Kaggle...")
    path = kagglehub.dataset_download(
        "andradaolteanu/gtzan-dataset-music-genre-classification"
    )
    logger.debug(f"Downloaded to: {path}")

    shutil.copytree(path, target_dir, dirs_exist_ok=True)
    logger.info(f"GTZAN dataset copied to: {target_dir}")
    return target_dir


def _download_fma(size: str = "small", force: bool = False) -> Path:
    """Download the FMA (Free Music Archive) dataset.

    :param size: Dataset size - 'small' (8GB), 'medium' (25GB), 'large' (93GB), or 'full' (879GB)
    :param force: Force download of 'full' dataset (requires explicit confirmation)
    :return: Path to the dataset directory
    """
    import requests

    if size not in ["small", "medium", "large", "full"]:
        raise ValueError(
            f"Invalid size: {size}. Must be 'small', 'medium', 'large', or 'full'"
        )

    if size == "full" and not force:
        raise ValueError(
            "FMA 'full' dataset is 879GB! Use --force flag to confirm download. "
            "Command: python src --source fma --fma-size full --force"
        )

    target_dir = Path(__file__).parent.parent.parent / "data" / "raw" / f"fma_{size}"
    target_dir.mkdir(parents=True, exist_ok=True)

    zip_file = target_dir.parent / f"fma_{size}.zip"

    if not zip_file.exists():
        url = f"https://os.unil.cloud.switch.ch/fma/fma_{size}.zip"
        logger.info(f"Downloading FMA {size} dataset from {url}...")

        try:
            response = requests.get(url, stream=True, allow_redirects=True)
            response.raise_for_status()

            total_size = int(response.headers.get("content-length", 0))

            with (
                open(zip_file, "wb") as f,
                tqdm(
                    desc=f"Downloading fma_{size}.zip",
                    total=total_size,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                ) as pbar,
            ):
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))

            logger.info(f"Downloaded to: {zip_file}")
        except requests.exceptions.RequestException as e:
            if zip_file.exists():
                zip_file.unlink()  # Remove incomplete download
            raise RuntimeError(f"Failed to download FMA dataset: {e}")
    else:
        logger.info(f"Using existing download: {zip_file}")

    if not (target_dir / "fma_metadata").exists() and not list(target_dir.glob("*")):
        logger.info(f"Extracting {zip_file}...")
        with zipfile.ZipFile(zip_file, "r") as zip_ref:
            members = zip_ref.namelist()
            with tqdm(
                desc=f"Extracting fma_{size}.zip", total=len(members), unit="file"
            ) as pbar:
                for member in members:
                    zip_ref.extract(member, target_dir)
                    pbar.update(1)
        logger.info(f"FMA {size} dataset extracted to: {target_dir}")
    else:
        logger.info(f"Dataset already extracted at: {target_dir}")

    return target_dir


def _download_dataset(
    source: str = "gtzan", fma_size: str = "small", force: bool = False
) -> Path:
    """Download a music dataset.

    :param source: Dataset source - 'gtzan' or 'fma'
    :param fma_size: FMA dataset size (only used if source='fma')
    :param force: Force download of large datasets
    :return: Path to the dataset directory
    """
    if source == "gtzan":
        return _download_gtzan()
    elif source == "fma":
        return _download_fma(size=fma_size, force=force)
    else:
        raise ValueError(f"Unknown source: {source}. Must be 'gtzan' or 'fma'")


def _get_audio_directory(source: str = "gtzan", fma_size: str = "small") -> Path:
    """Get the audio directory path for the specified dataset.

    :param source: Dataset source - 'gtzan' or 'fma'
    :param fma_size: FMA dataset size (only used if source='fma')
    :return: Path to the audio directory
    """
    base_dir = Path(__file__).parent.parent.parent / "data" / "raw"

    if source == "gtzan":
        return base_dir / "Data" / "genres_original"
    elif source == "fma":
        return base_dir / f"fma_{fma_size}"
    else:
        raise ValueError(f"Unknown source: {source}. Must be 'gtzan' or 'fma'")


def _is_dataset_extracted(source: str = "gtzan", fma_size: str = "small") -> bool:
    """Check if the dataset is already extracted by looking for audio files.

    :param source: Dataset source - 'gtzan' or 'fma'
    :param fma_size: FMA dataset size (only used if source='fma')
    :return: True if audio files are found, False otherwise
    """
    target_dir = _get_audio_directory(source, fma_size)

    if not target_dir.exists():
        return False

    if source == "gtzan":
        # Check for .wav files in GTZAN
        return bool(list(target_dir.rglob("*.wav")))
    elif source == "fma":
        # Check for .mp3 files in FMA
        return bool(list(target_dir.rglob("*.mp3")))
    else:
        return False


def _list_audio_filepaths(source: str = "gtzan", fma_size: str = "small") -> list[Path]:
    """List all audio files in the specified dataset.

    :param source: Dataset source - 'gtzan' or 'fma'
    :param fma_size: FMA dataset size (only used if source='fma')
    :return: List of Paths to audio files
    """
    target_dir = _get_audio_directory(source, fma_size)
    file_pattern = "*.wav" if source == "gtzan" else "*.mp3"

    logger.debug(f"Listing audio files in directory: {target_dir.as_posix()}")

    if not target_dir.exists():
        logger.warning(f"Target directory does not exist: {target_dir}")
        return []

    audio_files = sorted(target_dir.rglob(file_pattern))
    logger.info(
        f"Found {len(audio_files)} audio files in the {source.upper()} dataset."
    )
    return audio_files


def _init_worker(log_level: str) -> None:
    """Initialize worker process with proper logger configuration.

    :param log_level: Log level to set (DEBUG or INFO)
    """
    logger.remove()
    logger.add(sys.stderr, level=log_level)


def _process_single_audio(args: tuple[int, Path]) -> audio | None:
    """Process a single audio file (for multiprocessing).

    :param args: Tuple of (id, path)
    :return: audio dataclass instance with features, or None if processing failed
    """
    id_counter, path = args
    audio_file = audio(
        id=id_counter,
        name=path.stem,
        path=path,
    )

    try:
        audio_file.features = get_features(audio_file)
        return audio_file
    except Exception as e:
        logger.warning(f"Failed to process {path.name}: {type(e).__name__} - {str(e)}")
        return None


def _get_dataframe_cache_path(
    source: str = "gtzan", fma_size: str | None = None, limit: int | None = None
) -> Path:
    """Get the path to the cached dataframe CSV file.

    :param source: Dataset source ('gtzan' or 'fma')
    :param fma_size: FMA dataset size (only used if source='fma')
    :param limit: Optional limit used to differentiate cache files
    :return: Path to CSV cache file
    """
    cache_dir = Path(__file__).parent.parent.parent / "data" / "processed"
    cache_dir.mkdir(parents=True, exist_ok=True)

    if source == "fma":
        source_suffix = f"{source}_{fma_size}"
    else:
        source_suffix = source

    if limit is not None:
        return cache_dir / f"dataset_features_{source_suffix}_{limit}.csv"
    return cache_dir / f"dataset_features_{source_suffix}_full.csv"


def _save_dataframe(
    dataframe: pd.DataFrame,
    source: str = "gtzan",
    fma_size: str | None = None,
    limit: int | None = None,
) -> None:
    """Save the dataframe to a CSV file.

    :param dataframe: Pandas DataFrame containing audio features
    :param source: Dataset source ('gtzan' or 'fma')
    :param fma_size: FMA dataset size (only used if source='fma')
    :param limit: Optional limit used to differentiate cache files
    """
    csv_path = _get_dataframe_cache_path(source, fma_size, limit)
    dataframe.to_csv(csv_path, index=False)
    logger.info(f"Dataframe cached to: {csv_path}")


def _get_dataset_cache_path(
    source: str = "gtzan", fma_size: str | None = None, limit: int | None = None
) -> Path:
    """Get the path to the cached dataset file.

    :param source: Dataset source ('gtzan' or 'fma')
    :param fma_size: FMA dataset size (only used if source='fma')
    :param limit: Optional limit used to differentiate cache files
    :return: Path to cache file
    """
    cache_dir = Path(__file__).parent.parent.parent / "data" / "processed"
    cache_dir.mkdir(parents=True, exist_ok=True)

    if source == "fma":
        source_suffix = f"{source}_{fma_size}"
    else:
        source_suffix = source

    if limit is not None:
        return cache_dir / f"dataset_cache_{source_suffix}_{limit}.json"
    return cache_dir / f"dataset_cache_{source_suffix}_full.json"


def _save_dataset(
    dataset: list[audio],
    source: str = "gtzan",
    fma_size: str | None = None,
    limit: int | None = None,
    append: bool = False,
) -> None:
    """Save the dataset to a JSON file and dataframe to CSV.

    :param dataset: List of audio dataclass instances
    :param source: Dataset source ('gtzan' or 'fma')
    :param fma_size: FMA dataset size (only used if source='fma')
    :param limit: Optional limit used to differentiate cache files
    :param append: If True, append to existing dataset; if False, overwrite
    """
    cache_path = _get_dataset_cache_path(source, fma_size, limit)

    json_data = []

    # Load existing data if appending
    if append and cache_path.exists():
        with open(cache_path, "r") as f:
            json_data = json.load(f)
        logger.debug(f"Loaded {len(json_data)} existing records for appending")

    # Add new data
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
    logger.info(f"Dataset cached to: {cache_path} (total records: {len(json_data)})")


def _load_dataset(
    source: str = "gtzan", fma_size: str | None = None, limit: int | None = None
) -> list[audio] | None:
    """Load the dataset from a JSON file if it exists.

    :param source: Dataset source ('gtzan' or 'fma')
    :param fma_size: FMA dataset size (only used if source='fma')
    :param limit: Optional limit used to differentiate cache files
    :return: List of audio dataclass instances or None if cache doesn't exist
    """
    cache_path = _get_dataset_cache_path(source, fma_size, limit)
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
    limit: int,
    log_level: str = "INFO",
    use_multiprocessing: bool = True,
    source: str = "gtzan",
    fma_size: str = "small",
    save_interval: int = 100,
    resume: bool = True,
) -> list[audio]:
    """Import audio files from the specified dataset.

    :param limit: Optional limit on the number of audio files to import
    :param log_level: Log level for worker processes (default: INFO)
    :param use_multiprocessing: Whether to use parallel processing (default: True)
    :param source: Dataset source - 'gtzan' or 'fma' (default: 'gtzan')
    :param fma_size: FMA dataset size (only used if source='fma')
    :param save_interval: Save progress every N files (default: 100)
    :param resume: Resume from existing cache if available (default: True)
    :return: List of audio dataclass instances
    """

    audio_paths = _list_audio_filepaths(source=source, fma_size=fma_size)
    if limit is not None:
        audio_paths = audio_paths[:limit]
        logger.info(f"Limiting import to first {limit} audio files.")

    # Try to resume from existing cache
    audio_files = []
    start_index = 0
    processed_paths = set()

    if resume:
        existing_dataset = _load_dataset(source, fma_size, limit)
        if existing_dataset:
            audio_files = existing_dataset
            processed_paths = {str(af.path) for af in existing_dataset}
            start_index = len(audio_files)
            logger.info(f"Resuming from {start_index} already processed files")

            # Filter out already processed files
            audio_paths = [p for p in audio_paths if str(p) not in processed_paths]

    if not audio_paths:
        logger.info("All files already processed!")
        return audio_files

    failed_count = 0

    if use_multiprocessing:
        num_processes = cpu_count()
        logger.info(f"Using {num_processes} processes for parallel extraction")

        with Pool(
            processes=num_processes, initializer=_init_worker, initargs=(log_level,)
        ) as pool:
            # Adjust indices to account for already processed files
            args = [(start_index + i, path) for i, path in enumerate(audio_paths)]

            # Process results as they come, save periodically to avoid memory issues
            with tqdm(total=len(audio_paths), desc="Processing audio files") as pbar:
                for result in pool.imap(_process_single_audio, args):
                    if result is not None:
                        audio_files.append(result)
                    else:
                        failed_count += 1

                    pbar.update(1)

                    # Save progress periodically
                    if len(audio_files) % save_interval == 0:
                        _save_dataset(
                            audio_files, source, fma_size, limit, append=False
                        )
                        logger.info(
                            f"Progress saved: {len(audio_files)} total files processed"
                        )

            if failed_count > 0:
                logger.warning(f"Failed to process {failed_count} audio file(s)")
    else:
        audio_files = []
        failed_count = 0
        for id_counter, path in enumerate(
            tqdm(audio_paths, desc="Processing audio files")
        ):
            try:
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

                # Save progress periodically
                if (id_counter + 1) % save_interval == 0:
                    _save_dataset(audio_files, source, fma_size, limit, append=False)
                    logger.info(
                        f"Progress saved: {id_counter + 1}/{len(audio_paths)} files"
                    )

            except Exception as e:
                logger.warning(
                    f"Failed to process {path.name}: {type(e).__name__} - {str(e)}"
                )
                failed_count += 1

        if failed_count > 0:
            logger.warning(f"Failed to process {failed_count} audio file(s)")

    return audio_files


def get_audio_dataset(
    limit: int | None = None,
    log_level: str = "INFO",
    use_multiprocessing: bool = True,
    recreate: bool = False,
    source: str = "gtzan",
    fma_size: str = "small",
    force: bool = False,
    save_interval: int = 100,
) -> list[audio]:
    """Create a music dataset from the specified source.

    :param limit: Optional limit on the number of audio files to import
    :param log_level: Log level for worker processes (default: INFO)
    :param use_multiprocessing: Whether to use parallel processing (default: True)
    :param recreate: Force recreation of dataset even if cache exists (default: False)
    :param source: Dataset source - 'gtzan' or 'fma' (default: 'gtzan')
    :param fma_size: FMA dataset size - 'small', 'medium', 'large', or 'full' (default: 'small')
    :param force: Force download of large datasets like fma_full (default: False)
    :param save_interval: Save progress every N files (default: 100)
    :return: List of audio dataclass instances
    """
    # Only return cached if not recreating and cache is complete
    if not recreate:
        cached_dataset = _load_dataset(source, fma_size, limit)
        if cached_dataset is not None:
            # Check if cache is complete
            audio_paths = _list_audio_filepaths(source=source, fma_size=fma_size)
            if limit is not None:
                expected_count = min(limit, len(audio_paths))
            else:
                expected_count = len(audio_paths)

            if len(cached_dataset) >= expected_count:
                logger.info(
                    f"Using complete cached dataset with {len(cached_dataset)} files"
                )
                return cached_dataset
            else:
                logger.info(
                    f"Cache incomplete ({len(cached_dataset)}/{expected_count}), will resume processing"
                )

    logger.debug(f"Checking if {source.upper()} dataset is extracted...")

    if not _is_dataset_extracted(source, fma_size):
        logger.info(f"{source.upper()} dataset not found locally. Downloading...")
        _download_dataset(source, fma_size, force)
    else:
        logger.info(
            f"{source.upper()} dataset already exists locally. Skipping download."
        )

    dataset = _import_audio_from_dataset(
        limit,
        log_level=log_level,
        use_multiprocessing=use_multiprocessing,
        source=source,
        fma_size=fma_size,
        save_interval=save_interval,
        resume=not recreate,  # Resume unless recreating
    )

    _save_dataset(dataset, source, fma_size, limit)
    return dataset


def get_features_dataframe(
    dataset: list[audio] = None,
    limit: int | None = None,
    recreate: bool = False,
    source: str = "gtzan",
    fma_size: str | None = None,
) -> pd.DataFrame:
    """Convert the dataset into a pandas DataFrame.

    :param dataset: List of audio dataclass instances
    :param limit: Optional limit used to differentiate cache files
    :param recreate: Force recreation of dataframe even if cache exists (default: False)
    :param source: Dataset source - 'gtzan' or 'fma' (default: 'gtzan')
    :param fma_size: FMA dataset size (only used if source='fma')
    :return: DataFrame containing audio features
    """
    if not recreate:
        csv_path = _get_dataframe_cache_path(source, fma_size, limit)
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

    _save_dataframe(df, source, fma_size, limit)
    return df


def create_dataframe(
    limit: int | None,
    log_level: str,
    multi: bool,
    recreate: bool,
    source: str = "gtzan",
    fma_size: str = "small",
    force: bool = False,
) -> None:
    """Create a DataFrame with audio features from the dataset.

    :param limit: Limit number of audio files to import
    :param log_level: Logging level
    :param multi: Use multiprocessing for feature extraction
    :param recreate: Force recreation of dataset cache
    :param source: Dataset source ('gtzan' or 'fma')
    :param fma_size: FMA dataset size (only used if source='fma')
    :param force: Force download of large datasets
    :return: DataFrame with audio features
    """
    logger.debug("Importing audio dataset.")
    dataset = get_audio_dataset(
        limit=30 if limit else None,
        log_level=log_level,
        use_multiprocessing=multi,
        recreate=recreate,
        source=source,
        fma_size=fma_size,
        force=force,
    )
    logger.info(f"Imported {len(dataset)} audio files from dataset.")

    logger.debug("Extracting features and creating DataFrame.")
    df = get_features_dataframe(
        dataset=dataset,
        limit=30 if limit else None,
        recreate=recreate,
    )
    logger.info(f"Created DataFrame with shape: {df.shape}")
    return df
