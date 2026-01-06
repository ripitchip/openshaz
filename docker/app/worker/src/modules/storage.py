import os
import re
import tempfile
from pathlib import Path
from typing import Optional

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from loguru import logger

OBJECT_STORAGE_URL = os.getenv("OBJECT_STORAGE_URL", "http://rustfs:9000")
OBJECT_STORAGE_ACCESS_KEY = os.getenv("OBJECT_STORAGE_ACCESS_KEY")
OBJECT_STORAGE_SECRET_KEY = os.getenv("OBJECT_STORAGE_SECRET_KEY")
OBJECT_STORAGE_REGION = os.getenv("OBJECT_STORAGE_REGION", "eu-west-1")


def _parse_s3_url(url: str) -> tuple[str, str]:
    """Parse S3-style URL to extract bucket and key.

    Supports formats:
    - s3://bucket-name/path/to/file
    - http://rustfs:9000/bucket-name/path/to/file

    :param url: S3-style URL
    :return: Tuple of (bucket, key)
    """
    if url.startswith("s3://"):
        parts = url[5:].split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""
        return bucket, key

    # HTTP(S) URL format
    # Assumes format: http(s)://host:port/bucket/key
    path = url.split("://", 1)[1]  # Remove protocol
    path = path.split("/", 1)[1] if "/" in path else ""  # Remove host:port
    parts = path.split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    return bucket, key


def _sanitize_filename(name: str) -> str:
    """Sanitize filename for local filesystem.

    :param name: Original filename
    :return: Sanitized filename
    """
    return re.sub(r"[^a-zA-Z0-9._-]", "_", name)


def download_from_object_storage(
    url: str, dest_dir: Optional[Path] = None, timeout: int = 120
) -> Path:
    """Download a file from RustFS (MinIO) object storage and return the local path.

    :param url: S3-style URL (s3://bucket/key or http://endpoint/bucket/key)
    :param dest_dir: Optional directory to save the file (default: temp dir)
    :param timeout: Request timeout seconds
    :return: Path to the downloaded local file
    """
    if not OBJECT_STORAGE_ACCESS_KEY or not OBJECT_STORAGE_SECRET_KEY:
        raise ValueError(
            "OBJECT_STORAGE_ACCESS_KEY and OBJECT_STORAGE_SECRET_KEY must be set"
        )

    s3_client = boto3.client(
        "s3",
        endpoint_url=OBJECT_STORAGE_URL,
        aws_access_key_id=OBJECT_STORAGE_ACCESS_KEY,
        aws_secret_access_key=OBJECT_STORAGE_SECRET_KEY,
        config=Config(
            signature_version="s3v4", connect_timeout=timeout, read_timeout=timeout
        ),
        region_name=OBJECT_STORAGE_REGION,
    )

    bucket, key = _parse_s3_url(url)
    if not bucket or not key:
        raise ValueError(f"Invalid S3 URL format: {url}")

    if dest_dir is None:
        base_tmp = os.getenv("OPENSHAZ_TMP_DIR", tempfile.gettempdir())
        dest_dir = Path(base_tmp) / "openshaz"
    dest_dir.mkdir(parents=True, exist_ok=True)

    filename = _sanitize_filename(key.split("/")[-1])
    dest_path = dest_dir / filename

    logger.info(f"Downloading from RustFS: s3://{bucket}/{key} -> {dest_path}")

    try:
        s3_client.download_file(bucket, key, str(dest_path))
        logger.info(f"Downloaded file saved to {dest_path}")
        return dest_path
    except ClientError as e:
        logger.error(f"Failed to download from RustFS: {e}")
        raise


def cleanup_downloaded_file(file_path: Path) -> None:
    """Delete a downloaded file to free disk space.

    :param file_path: Path to the file to delete
    """
    try:
        if file_path.exists() and file_path.is_file():
            file_path.unlink()
            logger.info(f"Cleaned up file: {file_path}")
        else:
            logger.warning(f"File not found or not a file: {file_path}")
    except Exception as e:
        logger.error(f"Failed to cleanup file {file_path}: {e}")
        raise
