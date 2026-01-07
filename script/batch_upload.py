#!/usr/bin/env python3
"""Batch upload script for adding multiple songs to OpenShaz.

This script recursively scans a directory for MP3 files and uploads them
with controlled concurrency to avoid overwhelming the system.
"""

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Dict, List, Set
import time
import aiofiles
import aiohttp
from loguru import logger
from tqdm.asyncio import tqdm


class BatchUploader:
    """Handles batch uploading of MP3 files with controlled concurrency."""

    def __init__(
        self,
        api_url: str = "http://localhost:8000",
        max_concurrent: int = 5,
        timeout: int = 120,
        state_file: str = ".upload_state.json",
        dry_run: bool = True,
        limit: int = 30,
    ):
        """Initialize batch uploader.

        :param api_url: Base URL of the OpenShaz API
        :param max_concurrent: Maximum number of concurrent uploads
        :param timeout: Timeout per upload in seconds
        :param state_file: File to track upload progress
        :param dry_run: If True, only simulate uploads without sending
        :param limit: Maximum number of files to upload (0 for unlimited)
        """
        self.api_url = api_url.rstrip("/")
        self.max_concurrent = max_concurrent
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.state_file = Path(state_file)
        self.uploaded: Set[str] = set()
        self.failed: Dict[str, str] = {}
        self.successful: List[str] = []
        self.dry_run = dry_run
        self.limit = limit

    def load_state(self) -> None:
        """Load previous upload state to support resumption."""
        if self.state_file.exists():
            try:
                with open(self.state_file, "r") as f:
                    state = json.load(f)
                    self.uploaded = set(state.get("uploaded", []))
                logger.info(
                    f"Loaded state: {len(self.uploaded)} files already uploaded"
                )
            except Exception as e:
                logger.warning(f"Could not load state file: {e}")

    def save_state(self) -> None:
        """Save current upload state."""
        try:
            with open(self.state_file, "w") as f:
                json.dump({"uploaded": list(self.uploaded)}, f, indent=2)
        except Exception as e:
            logger.warning(f"Warning: Could not save state file: {e}")

    def find_mp3_files(self, directory: Path) -> List[Path]:
        """Recursively find all MP3 files in directory.

        :param directory: Root directory to scan
        :return: List of MP3 file paths
        """
        mp3_files = list(directory.rglob("*.mp3"))
        logger.info(f"Found {len(mp3_files)} MP3 files in {directory}")
        return mp3_files

    async def upload_file(
        self, session: aiohttp.ClientSession, file_path: Path
    ) -> bool:
        """Upload a single file to the API.

        :param session: aiohttp client session
        :param file_path: Path to MP3 file
        :return: True if successful, False otherwise
        """
        file_key = str(file_path.absolute())

        if file_key in self.uploaded:
            return True

        try:
            async with aiofiles.open(file_path, "rb") as f:
                file_content = await f.read()

            form_data = aiohttp.FormData()
            form_data.add_field(
                "file",
                file_content,
                filename=file_path.name,
                content_type="audio/mpeg",
            )

            async with session.post(
                f"{self.api_url}/add-song", data=form_data
            ) as response:
                if response.status == 200:
                    self.successful.append(file_key)
                    self.uploaded.add(file_key)
                    return True
                else:
                    error_text = await response.text()
                    self.failed[file_key] = f"HTTP {response.status}: {error_text}"
                    return False

        except asyncio.TimeoutError:
            self.failed[file_key] = "Request timeout"
            return False
        except Exception as e:
            self.failed[file_key] = str(e)
            return False

    async def upload_batch(self, file_paths: List[Path]) -> None:
        """Upload multiple files with controlled concurrency.

        :param file_paths: List of file paths to upload
        """
        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def bounded_upload(session: aiohttp.ClientSession, file_path: Path):
            async with semaphore:
                return await self.upload_file(session, file_path)

        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            tasks = [bounded_upload(session, fp) for fp in file_paths]

            # Use tqdm for progress bar
            results = []
            for coro in tqdm.as_completed(
                tasks, total=len(tasks), desc="Uploading", unit="file"
            ):
                result = await coro
                results.append(result)

                # Save state periodically (every 10 uploads)
                if len(results) % 10 == 0:
                    self.save_state()

        # Final state save
        self.save_state()

    def print_summary(self, total_files: int) -> None:
        """Print upload summary statistics.

        :param total_files: Total number of files processed
        """
        logger.info("\n" + "=" * 60)
        if self.dry_run:
            logger.info("DRY RUN SUMMARY (No files were actually uploaded)")
        else:
            logger.info("UPLOAD SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total files found:    {total_files}")
        logger.info(
            f"Already uploaded:     {len(self.uploaded) - len(self.successful)}"
        )
        logger.info(f"Newly uploaded:       {len(self.successful)}")
        logger.info(f"Failed:               {len(self.failed)}")
        logger.info("=" * 60)

        if self.failed:
            logger.info("\nFailed uploads:")
            for file_path, error in self.failed.items():
                logger.info(f"  - {Path(file_path).name}: {error}")

        logger.info(f"\nState saved to: {self.state_file.absolute()}")

    async def run(self, directory: Path) -> None:
        """Run the batch upload process.

        :param directory: Directory containing MP3 files
        """
        if not directory.exists():
            logger.critical(f"Error: Directory does not exist: {directory}")
            sys.exit(1)

        if not directory.is_dir():
            logger.critical(f"Error: Not a directory: {directory}")
            sys.exit(1)

        self.load_state()

        mp3_files = self.find_mp3_files(directory)

        if not mp3_files:
            logger.warning("No MP3 files found!")
            return

        files_to_upload = [
            f for f in mp3_files if str(f.absolute()) not in self.uploaded
        ]

        if not files_to_upload:
            logger.info("All files already uploaded!")
            self.print_summary(len(mp3_files))
            return

        # Apply limit if set
        total_available = len(files_to_upload)
        if self.limit > 0 and len(files_to_upload) > self.limit:
            files_to_upload = files_to_upload[: self.limit]
            logger.warning(
                f"Limiting to {self.limit} files (out of {total_available} available)"
            )
            logger.warning("Use --unlimited to upload all files")

        logger.info(f"Files to upload: {len(files_to_upload)}")
        logger.info(f"Max concurrent uploads: {self.max_concurrent}")
        logger.info(f"Timeout per file: {self.timeout.total}s")

        if self.dry_run:
            logger.info("*" * 60)
            logger.warning("DRY RUN MODE - No files will actually be uploaded")
            logger.warning("Use --execute flag to perform actual uploads")
            logger.info("*" * 60)
            return
        else:
            logger.info("!" * 60)
            logger.warning("EXECUTION MODE - Files will be uploaded to the server")
            logger.info("!" * 60)

            try:
                response = input(
                    f"\nAre you sure you want to upload {len(files_to_upload)} files? (yes/no): "
                )
                if response.lower() not in ["yes", "y"]:
                    logger.info("Upload cancelled by user.")
                    return
            except (KeyboardInterrupt, EOFError):
                logger.info("\nUpload cancelled by user.")
                return

        # Start upload
        await self.upload_batch(files_to_upload)

        # logger. summary
        self.print_summary(len(mp3_files))


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Batch upload MP3 files to OpenShaz",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run (default) - see what would be uploaded (max 30 files)
  python batch_upload.py /path/to/music

  # Actually upload files (requires confirmation, max 30 files)
  python batch_upload.py /path/to/music --execute

  # Upload all files without limit
  python batch_upload.py /path/to/music --execute --unlimited

  # Set custom limit (e.g., 50 files)
  python batch_upload.py /path/to/music --execute --limit 50

  # Use 10 concurrent uploads
  python batch_upload.py /path/to/music --execute --max-concurrent 10

  # Connect to remote API
  python batch_upload.py /path/to/music --execute --api-url http://192.168.1.100:8000
        """,
    )

    parser.add_argument(
        "directory", type=Path, help="Directory containing MP3 files to upload"
    )

    parser.add_argument(
        "--execute",
        action="store_true",
        help="Execute actual uploads (default is dry-run mode)",
    )

    parser.add_argument(
        "--unlimited",
        action="store_true",
        help="Upload all files without limit (default: 30 files)",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=30,
        help="Maximum number of files to upload (default: 30, use --unlimited for no limit)",
    )

    parser.add_argument(
        "--api-url",
        type=str,
        default="http://localhost:8000",
        help="OpenShaz API URL (default: http://localhost:8000)",
    )

    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=5,
        help="Maximum number of concurrent uploads (default: 5)",
    )

    parser.add_argument(
        "--timeout",
        type=int,
        default=120,
        help="Timeout per upload in seconds (default: 120)",
    )

    parser.add_argument(
        "--state-file",
        type=str,
        default=".upload_state.json",
        help="State file for tracking progress (default: .upload_state.json)",
    )

    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stdout, level="INFO", colorize=True)
    start_time = time.time()
    uploader = BatchUploader(
        api_url=args.api_url,
        max_concurrent=args.max_concurrent,
        timeout=args.timeout,
        state_file=args.state_file,
        dry_run=not args.execute,  # Dry run unless --execute is specified
        limit=0 if args.unlimited else args.limit,  # 0 means unlimited
    )

    # Run upload
    try:
        asyncio.run(uploader.run(args.directory))
        elapsed_time = time.time() - start_time
        logger.info(f"Execution completed in {elapsed_time:.2f} seconds.")
    except KeyboardInterrupt:
        logger.info("\n\nUpload interrupted by user")
        uploader.save_state()
        logger.info("Progress saved. Run again to resume.")
        elapsed_time = time.time() - start_time
        logger.info(f"Execution completed in {elapsed_time:.2f} seconds.")
        sys.exit(1)


if __name__ == "__main__":
    main()
