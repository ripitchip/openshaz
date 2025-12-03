# Multi-Source Dataset Support

## Overview

OpenShaz now supports multiple dataset sources for audio similarity analysis:

- **GTZAN**: Genre classification dataset from Kaggle
- **FMA**: Free Music Archive dataset with multiple size variants

## Dataset Sources

### GTZAN

- **Source**: Kaggle (kagglehub)
- **Size**: ~1.2GB
- **Files**: 1000 audio files (10 genres × 100 files)
- **Storage**: `data/raw/gtzan/`
- **Cache**: `data/processed/dataset_cache_gtzan_full.json`

### FMA (Free Music Archive)

- **Source**: https://os.unil.cloud.switch.ch/fma/
- **Storage**: `data/raw/fma_{size}/`
- **Cache**: `data/processed/dataset_cache_fma_{size}_full.json`

#### Size Variants

- **small**: 8GB (~8,000 tracks, 30s clips)
- **medium**: 25GB (~25,000 tracks, 30s clips)
- **large**: 93GB (~106,574 tracks, 30s clips)
- **full**: 879GB (~106,574 tracks, full length) - requires `--force` flag

## Usage

### Basic Usage

```bash
# Use GTZAN (default)
python src

# Use FMA small
python src --source fma --fma-size small

# Use FMA medium
python src --source fma --fma-size medium

# Use FMA large
python src --source fma --fma-size large

# Use FMA full (requires --force)
python src --source fma --fma-size full --force
```

### With Other Options

```bash
# GTZAN with multiprocessing
python src --source gtzan --multi

# FMA small with limited samples
python src --source fma --fma-size small --limit --multi

# Force recreation of GTZAN cache
python src --source gtzan --recreate

# FMA small with debug logging
python src --source fma --fma-size small --debug --multi
```

### Testing Similarity

```bash
# Test with GTZAN dataset
python src --source gtzan --test-audio-path test.mp3 --metric cosine --top-k 5

# Test with FMA small dataset
python src --source fma --fma-size small --test-audio-path test.mp3 --metric euclidean --top-k 10
```

### Comparing Metrics

```bash
# Compare metrics on GTZAN
python src --source gtzan --compare-metrics --top-k 5

# Compare metrics on FMA small
python src --source fma --fma-size small --compare-metrics --top-k 3
```

## CLI Arguments

### Dataset Selection

- `--source {gtzan,fma}`: Dataset source (default: gtzan)
- `--fma-size {small,medium,large,full}`: FMA dataset size (default: small)
- `--force`: Force download of large datasets like fma_full

### Processing Options

- `--debug`: Enable debug logging
- `--multi`: Enable multiprocessing (8 cores)
- `--limit`: Limit to 30 audio files (for testing)
- `--recreate`: Force recreation of dataset cache

### Similarity Options

- `--metric {cosine,euclidean,manhattan}`: Similarity metric (default: cosine)
- `--top-k N`: Number of top similar results (default: 5)
- `--test-audio-path PATH`: Path to test audio file
- `--compare-metrics`: Compare all similarity metrics

## Cache Files

Each dataset source has separate cache files:

### GTZAN

- Dataset: `data/processed/dataset_cache_gtzan_full.json`
- Features: `data/processed/dataset_features_gtzan_full.csv`
- Limited: `data/processed/dataset_cache_gtzan_30.json`

### FMA

- Dataset: `data/processed/dataset_cache_fma_{size}_full.json`
- Features: `data/processed/dataset_features_fma_{size}_full.csv`
- Limited: `data/processed/dataset_cache_fma_{size}_30.json`

## Download Behavior

### GTZAN

1. Checks if `data/raw/gtzan/` exists
2. If not, downloads from Kaggle using kagglehub
3. Copies files to target directory

### FMA

1. Checks if `data/raw/fma_{size}/` exists
2. If not, downloads ZIP file using curl from Swiss server
3. Extracts ZIP to target directory
4. Reuses existing ZIP if download was interrupted

### Safety Features

- FMA full (879GB) requires explicit `--force` flag
- Downloads only happen if directory is missing or empty
- ZIP extraction only happens if not already extracted
- Existing downloads are reused to save bandwidth

## Examples

### First-time Setup

```bash
# Download and process GTZAN
python src --source gtzan --multi

# Download and process FMA small
python src --source fma --fma-size small --multi
```

### Switching Between Datasets

```bash
# Process GTZAN
python src --source gtzan --multi

# Process FMA small (different cache)
python src --source fma --fma-size small --multi

# Back to GTZAN (uses existing cache)
python src --source gtzan
```

### Testing Both Datasets

```bash
# Test against GTZAN
python src --source gtzan --test-audio-path test.mp3 --top-k 5

# Test against FMA small
python src --source fma --fma-size small --test-audio-path test.mp3 --top-k 5
```

## Notes

- Each dataset is cached independently
- Multiprocessing (8 cores) significantly speeds up feature extraction
- Cache files prevent re-processing on subsequent runs
- Use `--recreate` to force re-processing
- FMA datasets may take significant time to download depending on size
- All audio files must be in .wav format

## Error Handling

The system handles:

- Corrupted audio files (skipped with warning)
- Missing backend (soundfile installation instructions)
- Failed downloads (error message with curl details)
- Large dataset warnings (fma_full requires --force)
