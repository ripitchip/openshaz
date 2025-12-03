import os
import shutil
import kagglehub
from pathlib import Path
from loguru import logger


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
