"""Module for audio similarity detection using feature vectors."""

from typing import Literal

import numpy as np
import pandas as pd
from loguru import logger
from sklearn.metrics.pairwise import cosine_similarity, euclidean_distances
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler


class SimilarityEngine:
    """Engine for computing audio similarity using various distance metrics."""

    def __init__(
        self,
        metric: Literal["cosine", "euclidean", "manhattan"] = "cosine",
        normalize: bool = True,
    ):
        """Initialize the similarity engine.

        :param metric: Distance metric to use (cosine, euclidean, manhattan)
        :param normalize: Whether to normalize features before computing similarity
        """
        self.metric = metric
        self.normalize = normalize
        self.scaler = StandardScaler() if normalize else None
        self.feature_matrix = None
        self.df = None

    def fit(self, df: pd.DataFrame) -> None:
        """Fit the similarity engine with a DataFrame of audio features.

        :param df: DataFrame with columns ['id', 'name', ...feature columns...]
        """
        logger.info(f"Fitting similarity engine with {len(df)} audio files")

        self.df = df.copy()

        feature_columns = [col for col in df.columns if col not in ["id", "name"]]
        self.feature_matrix = df[feature_columns].values

        if self.normalize:
            self.feature_matrix = self.scaler.fit_transform(self.feature_matrix)
            logger.debug("Features normalized using StandardScaler")

        logger.info(f"Feature matrix shape: {self.feature_matrix.shape}")

    def _compute_similarity(self, query_features: np.ndarray) -> np.ndarray:
        """Compute similarity scores between query and all stored features.

        :param query_features: Feature vector for query audio
        :return: Array of similarity scores
        """
        query_features = query_features.reshape(1, -1)

        if self.normalize and self.scaler is not None:
            query_features = self.scaler.transform(query_features)

        if self.metric == "cosine":
            # Cosine similarity (higher is more similar)
            similarities = cosine_similarity(query_features, self.feature_matrix)[0]
        elif self.metric == "euclidean":
            # Euclidean distance (lower is more similar, so we invert)
            distances = euclidean_distances(query_features, self.feature_matrix)[0]
            similarities = 1 / (1 + distances)  # Convert to similarity
        elif self.metric == "manhattan":
            # Manhattan distance (L1 norm)
            distances = np.sum(np.abs(query_features - self.feature_matrix), axis=1)
            similarities = 1 / (1 + distances)  # Convert to similarity
        else:
            raise ValueError(f"Unknown metric: {self.metric}")

        return similarities

    def find_similar(self, query_features: np.ndarray, top_k: int = 5) -> list[dict]:
        """Find the most similar audio files to the query based on feature vector.

        :param query_features: Feature vector as np.ndarray (must match feature dimension)
        :param top_k: Number of top matches to return
        :return: List of dicts with id, name, similarity score
        """
        if self.feature_matrix is None:
            raise ValueError("Engine not fitted. Call fit() first.")

        if not isinstance(query_features, np.ndarray):
            raise ValueError("query_features must be a numpy ndarray")

        similarities = self._compute_similarity(query_features)

        top_indices = np.argsort(similarities)[::-1][:top_k]

        results = []
        for idx in top_indices:
            result = {
                "index": int(idx),
                "similarity": float(similarities[idx]),
            }

            if self.df is not None:
                result["id"] = int(self.df.iloc[idx]["id"])
                result["name"] = self.df.iloc[idx]["name"]

            results.append(result)

        logger.debug(f"Found {len(results)} similar audio files")
        return results

    def evaluate(self, test_df: pd.DataFrame, top_k: int = 5) -> dict:
        """Evaluate the similarity engine on a test dataset.

        Assumes that audio files with the same genre prefix should be similar.

        :param test_df: DataFrame with test samples
        :param top_k: Number of top matches to consider
        :return: Dictionary with evaluation metrics
        """
        logger.info(f"Evaluating similarity engine on {len(test_df)} test samples")

        correct_predictions = 0
        total_predictions = 0
        genre_accuracies = {}

        feature_columns = [col for col in test_df.columns if col not in ["id", "name"]]

        for idx, row in test_df.iterrows():
            true_genre = row["name"].split(".")[0]

            query_features = row[feature_columns].values

            similar = self.find_similar(query_features=query_features, top_k=top_k)

            predicted_genres = [s["name"].split(".")[0] for s in similar]

            if true_genre in predicted_genres:
                correct_predictions += 1

            total_predictions += 1

            if true_genre not in genre_accuracies:
                genre_accuracies[true_genre] = {"correct": 0, "total": 0}

            genre_accuracies[true_genre]["total"] += 1
            if true_genre in predicted_genres:
                genre_accuracies[true_genre]["correct"] += 1

        overall_accuracy = (
            correct_predictions / total_predictions if total_predictions > 0 else 0
        )

        for genre in genre_accuracies:
            total = genre_accuracies[genre]["total"]
            correct = genre_accuracies[genre]["correct"]
            genre_accuracies[genre]["accuracy"] = correct / total if total > 0 else 0

        results = {
            "overall_accuracy": overall_accuracy,
            "correct_predictions": correct_predictions,
            "total_predictions": total_predictions,
            "top_k": top_k,
            "metric": self.metric,
            "genre_accuracies": genre_accuracies,
        }

        logger.info(f"Overall accuracy (top-{top_k}): {overall_accuracy:.2%}")
        return results


def train_test_similarity(
    df: pd.DataFrame,
    test_size: float = 0.2,
    metric: Literal["cosine", "euclidean", "manhattan"] = "cosine",
    top_k: int = 5,
    random_state: int = 42,
) -> dict:
    """Train and test a similarity engine with train/test split.

    :param df: DataFrame with audio features
    :param test_size: Proportion of dataset to use for testing
    :param metric: Distance metric to use
    :param top_k: Number of top matches to consider for evaluation
    :param random_state: Random seed for reproducibility
    :return: Dictionary with evaluation results
    """
    logger.info(f"Performing train/test split with test_size={test_size}")

    train_df, test_df = train_test_split(
        df, test_size=test_size, random_state=random_state, shuffle=True
    )

    logger.info(f"Train set: {len(train_df)} samples, Test set: {len(test_df)} samples")

    engine = SimilarityEngine(metric=metric, normalize=True)
    engine.fit(train_df)

    results = engine.evaluate(test_df, top_k=top_k)
    results["train_size"] = len(train_df)
    results["test_size"] = len(test_df)

    return results


def compare_metrics(
    df: pd.DataFrame, test_size: float = 0.2, top_k: int = 5, random_state: int = 42
) -> pd.DataFrame:
    """Compare different similarity metrics on the same dataset.

    :param df: DataFrame with audio features
    :param test_size: Proportion of dataset to use for testing
    :param top_k: Number of top matches to consider
    :param random_state: Random seed for reproducibility
    :return: DataFrame comparing metrics
    """
    logger.info("Comparing similarity metrics: cosine, euclidean, manhattan")

    metrics = ["cosine", "euclidean", "manhattan"]
    results = []

    for metric in metrics:
        logger.info(f"Testing metric: {metric}")
        result = train_test_similarity(
            df=df,
            test_size=test_size,
            metric=metric,
            top_k=top_k,
            random_state=random_state,
        )

        results.append(
            {
                "metric": metric,
                "accuracy": result["overall_accuracy"],
                "correct": result["correct_predictions"],
                "total": result["total_predictions"],
            }
        )

    comparison_df = pd.DataFrame(results)
    logger.info(f"\nMetric comparison:\n{comparison_df.to_string()}")

    return comparison_df
