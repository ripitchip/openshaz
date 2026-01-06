import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger
from models.audio import audio
from modules.parser import parse_arguments
from modules.dataset import get_audio_dataset, get_features_dataframe
from modules.extraction import get_features
from modules.similarity import SimilarityEngine, compare_metrics


def start_logging(is_debug: bool, is_worker: bool) -> None:
    """Initialize logging configuration."""
    logger.remove()
    log_level = "DEBUG" if is_debug else "INFO"
    logger.add(sys.stderr, level=log_level)
    if is_worker:
        logger.add(
            "./worker_logs/worker.log",
            rotation="500 MB",
            compression="zip",
            level="DEBUG",
        )
    logger.info("Starting OpenShaz, Open-source audio similarity tool.")


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


def compare_different_metrics(
    df, test_size: float = 0.2, top_k: int = 5, random_state: int = 42
):
    """Compare different similarity metrics on the same dataset.

    :param df: DataFrame with audio features
    :param test_size: Proportion of dataset to use for testing
    :param top_k: Number of top matches to consider
    :param random_state: Random seed for reproducibility
    :return: DataFrame comparing metrics
    """
    logger.info("Comparing similarity metrics...")
    comparison_df = compare_metrics(
        df=df, test_size=test_size, top_k=top_k, random_state=random_state
    )
    logger.info(f"\nComparison results:\n{comparison_df}")


def measure_similarity(
    df: pd.DataFrame, audio: np.ndarray, metric: str = "cosine", top_k: int = 5
) -> None:
    """Measure similarity using the specified metric.

    :param df: DataFrame with audio features
    :param audio: Audio data to compare
    :param metric: Similarity metric to use
    :param top_k: Number of top similar results to return
    :return: List of similarity results
    """
    engine = SimilarityEngine(metric=metric, normalize=True)
    engine.fit(df)

    results = engine.find_similar(audio, top_k=top_k)
    logger.info(f"Top {top_k} similar results using {metric} metric:")
    for rank, result in enumerate(results, start=1):
        name = result.get("name", "unknown")
        similarity = result["similarity"]
        logger.info(f"{rank}. {name} - Similarity: {similarity:.4f}")


@logger.catch
def __main__():
    args = parse_arguments()

    if args.command == "manual":
        start_logging(is_debug=args.debug, is_worker=False)
        start_time = time.time()
        df = create_dataframe(
            limit=args.limit,
            log_level="DEBUG" if args.debug else "INFO",
            multi=args.multi,
            recreate=args.recreate,
            source=args.source,
            fma_size=args.fma_size,
            force=args.force,
        )

        if args.compare_metrics:
            compare_different_metrics(
                df=df,
                test_size=0.2,
                top_k=args.top_k,
                random_state=42,
            )

        if args.test_audio_path is not None:
            test_audio_path = Path(args.test_audio_path)
            test_audio = audio(name=test_audio_path.stem, path=test_audio_path)
            test_audio.features = get_features(test_audio)
            measure_similarity(
                df=df,
                audio=test_audio.features,
                metric=args.metric,
                top_k=args.top_k,
            )
        elapsed_time = time.time() - start_time
        logger.info(f"Execution completed in {elapsed_time:.2f} seconds.")

    elif args.command == "worker":
        start_logging(is_debug=args.debug, is_worker=True)
        logger.info("Starting worker mode...")
        logger.info(f"Worker ID: {args.worker_id or 'auto-generated'}")
        logger.info(f"Queue URL: {args.queue_url or 'default'}")
        logger.info(f"Batch size: {args.batch_size}")

        # TODO: Implement worker logic here
        logger.warning("Worker mode not yet implemented")


if __name__ == "__main__":
    __main__()
