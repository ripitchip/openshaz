"""Module for extracting audio features using librosa."""

import librosa
import numpy as np
from loguru import logger
from pathlib import Path

from models.audio import audio


def _import_audio_from_path(path: Path) -> tuple[np.ndarray, int]:
    """Import an audio file and return the audio time series and sampling rate.

    :param path: Path to the audio file
    :return: Tuple of audio time series and sampling rate
    """
    if not path.name.endswith((".wav", ".mp3", ".flac", ".ogg", ".m4a")):
        raise ValueError("Unsupported audio file format.")
    elif not path.exists():
        raise FileNotFoundError(f"Audio file not found at path: {path}")
    y, sr = librosa.load(path)
    y, _ = librosa.effects.trim(y)
    return y, sr


def _get_length(y: np.ndarray) -> int:
    """Get the length of the audio signal in samples.

    :param y: Audio time series
    :return: Length of the audio signal
    """
    return np.shape(y)[0]


def _get_chroma_stft_mean_var(
    y: np.ndarray, sr: int, hop_length: int = 5000
) -> tuple[float, float]:
    """Get the mean and variance of the chroma STFT feature.

    :param y: Audio time series
    :param sr: Sampling rate of the audio signal
    :param hop_length: Hop length for STFT
    :return: Mean and variance of chroma STFT
    """
    chroma_stft = librosa.feature.chroma_stft(y=y, sr=sr, hop_length=hop_length)
    return np.mean(chroma_stft), np.var(chroma_stft)


def _get_rms_mean_var(y: np.ndarray) -> tuple[float, float]:
    """Get the mean and variance of the RMS energy feature.

    :param y: Audio time series
    :return: Mean and variance of RMS energy
    """
    rms = librosa.feature.rms(y=y)[0]
    return np.mean(rms), np.var(rms)


def _get_spectral_centroid_mean_var(y: np.ndarray, sr: int) -> tuple:
    """Get the mean and variance of the spectral centroid feature.

    :param y: Audio time series
    :param sr: Sampling rate of the audio signal
    :return: Mean and variance of spectral centroid
    """
    spectral_centroids = librosa.feature.spectral_centroid(y=y, sr=sr)[0]
    return np.mean(spectral_centroids), np.var(spectral_centroids)


def _get_spectral_bandwidth_mean_var(y: np.ndarray, sr: int) -> tuple:
    """Get the mean and variance of the spectral bandwidth feature.

    :param y: Audio time series
    :param sr: Sampling rate of the audio signal
    :return: Mean and variance of spectral bandwidth
    """
    spectral_bandwidth = librosa.feature.spectral_bandwidth(y=y, sr=sr)[0]
    return np.mean(spectral_bandwidth), np.var(spectral_bandwidth)


def _get_spectral_rolloff_mean_var(y: np.ndarray, sr: int) -> tuple:
    """Get the mean and variance of the spectral rolloff feature.

    :param y: Audio time series
    :param sr: Sampling rate of the audio signal
    :return: Mean and variance of spectral rolloff
    """
    spectral_rolloff = librosa.feature.spectral_rolloff(y=y, sr=sr)[0]
    return np.mean(spectral_rolloff), np.var(spectral_rolloff)


def _get_zero_crossing_rate_mean_var(y: np.ndarray) -> tuple:
    """Get the mean and variance of the zero crossing rate feature.

    :param y: Audio time series
    :return: Mean and variance of zero crossing rate
    """
    zero_crossing_rate = librosa.feature.zero_crossing_rate(y)[0]
    return np.mean(zero_crossing_rate), np.var(zero_crossing_rate)


def _get_harmonics_perceptrual_mean_var(y: np.ndarray) -> tuple:
    """Get the mean and variance of the harmonic and percussive components.

    :param y: Audio time series
    :return: Mean and variance of harmonic and percussive components
    """
    # Use margin=2 for faster processing (lower quality but much faster)
    y_harm, y_perc = librosa.effects.hpss(y, margin=2)
    return np.mean(y_harm), np.mean(y_perc), np.var(y_harm), np.var(y_perc)


def _get_tempo(y: np.ndarray, sr: int) -> float:
    """Get the tempo of the audio signal.

    :param y: Audio time series
    :param sr: Sampling rate of the audio signal
    :return: Tempo of the audio signal
    """
    tempo, _ = librosa.beat.beat_track(y=y, sr=sr)
    return float(tempo) if np.isscalar(tempo) else float(tempo[0])


def _get_mfcc_mean_var(y: np.ndarray, sr: int) -> np.ndarray:
    """Get the mean and variance of the MFCC features.

    :param y: Audio time series
    :param sr: Sampling rate of the audio signal
    :return: Array of mean and variance of MFCC features
    """
    mfcc = librosa.feature.mfcc(y=y, sr=sr)
    means = np.mean(mfcc, axis=1)
    variances = np.var(mfcc, axis=1)
    return np.column_stack((means, variances)).ravel()


def _extract_features(audio: audio) -> np.ndarray:
    """Extract all audio features and return them as a single array.

    :param path: Path to the audio file
    :return: Array of extracted audio features
    """
    y = audio.y
    sr = audio.sr

    features = [
        _get_length(y),
        *_get_chroma_stft_mean_var(y, sr),
        *_get_rms_mean_var(y),
        *_get_spectral_centroid_mean_var(y, sr),
        *_get_spectral_bandwidth_mean_var(y, sr),
        *_get_spectral_rolloff_mean_var(y, sr),
        *_get_zero_crossing_rate_mean_var(y),
        *_get_harmonics_perceptrual_mean_var(y),
        _get_tempo(y, sr),
    ]

    return np.concatenate([features, _get_mfcc_mean_var(y, sr)])


def get_features(audio: audio) -> np.ndarray:
    """Extract all audio features and return them as a single array.

    :param audio: Audio dataclass instance containing audio data
    :return: Array of extracted audio features
    """
    if audio.y is None or audio.sr is None:
        logger.debug(f"Loading audio from path: {audio.path}")
        audio.y, audio.sr = _import_audio_from_path(audio.path)
    return _extract_features(audio)
