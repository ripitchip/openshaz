"""This module contains the Audio dataclass for audio feature extraction."""

from pathlib import Path
import numpy as np
from dataclasses import dataclass


@dataclass
class audio:
    """
    This class contains methods to extract various audio features from a given audio signal.
    """
    path: Path = None
    y: np.ndarray
    sr: int
    features: np.ndarray = None
