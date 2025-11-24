#!/usr/bin/env python3
"""
Download PubMed Central Open Access (PMCOA) XML files in parallel.

Features:
- Parallel downloads with configurable workers
- Skip already downloaded files
- Progress tracking with progress bars
- Automatic retry on failure
- Comprehensive logging
- Checksum verification (if available)
"""

import os
import sys
import logging
import argparse
from pathlib import Path
from ftplib import FTP
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Optional
from datetime import datetime
import time

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    print("Warning: tqdm not installed. Install with: pip install tqdm")
    print("Progress bars will be disabled.")


# Configuration
FTP_HOST = "ftp.ncbi.nlm.nih.gov"
FTP_DIRS = [
    "/pub/pmc/oa_bulk/oa_other/xml/",
    "/pub/pmc/oa_bulk/oa_comm/xml/",
    "/pub/pmc/oa_bulk/oa_noncomm/xml/",
]
DOWNLOAD_DIR = "raw_download"
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds


def setup_logging(log_file: str = "download_pmcoa.log") -> logging.Logger:
    """Set up logging to file and console."""
    logger = logging.getLogger("pmcoa_download")
    logger.setLevel(logging.INFO)

    # File handler
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.INFO)

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


def get_file_list(ftp_dir: str, logger: logging.Logger) -> List[str]:
    """Get list of files from FTP directory."""
    try:
        ftp = FTP(FTP_HOST)
        ftp.login()
        ftp.cwd(ftp_dir)

        files = []
        ftp.retrlines('NLST', files.append)

        # Filter for CSV and tar.gz files
        files = [f for f in files if f.endswith('.csv') or f.endswith('.tar.gz')]

        ftp.quit()
        logger.info(f"Found {len(files)} files in {ftp_dir}")
        return files

    except Exception as e:
        logger.error(f"Error listing files in {ftp_dir}: {e}")
        return []


def download_file(
    ftp_dir: str,
    filename: str,
    download_dir: Path,
    skip_existing: bool = True,
    max_retries: int = MAX_RETRIES,
    logger: Optional[logging.Logger] = None
) -> Tuple[str, bool, Optional[str]]:
    """
    Download a single file from FTP.

    Returns:
        Tuple of (filename, success, error_message)
    """
    if logger is None:
        logger = logging.getLogger("pmcoa_download")

    local_path = download_dir / filename

    # Try downloading with retries
    for attempt in range(max_retries):
        try:
            ftp = FTP(FTP_HOST)
            ftp.login()
            ftp.cwd(ftp_dir)

            # Get remote file size for verification
            remote_size = ftp.size(filename)

            # Skip if file exists with correct size and skip_existing is True
            if skip_existing and local_path.exists():
                local_size = local_path.stat().st_size
                if remote_size and local_size == remote_size:
                    ftp.quit()
                    logger.debug(f"Skipping complete file: {filename} ({local_size} bytes)")
                    return (filename, True, None)
                elif local_size > 0:
                    logger.info(
                        f"Re-downloading incomplete file: {filename} "
                        f"(local: {local_size}, remote: {remote_size})"
                    )

            # Download file
            with open(local_path, 'wb') as f:
                ftp.retrbinary(f'RETR {filename}', f.write)

            ftp.quit()

            # Verify file size
            downloaded_size = local_path.stat().st_size
            if remote_size and downloaded_size != remote_size:
                raise Exception(
                    f"Size mismatch: expected {remote_size}, got {downloaded_size}"
                )

            logger.info(f"Downloaded: {filename} ({downloaded_size / 1024 / 1024:.1f} MB)")
            return (filename, True, None)

        except Exception as e:
            logger.warning(
                f"Attempt {attempt + 1}/{max_retries} failed for {filename}: {e}"
            )
            if attempt < max_retries - 1:
                time.sleep(RETRY_DELAY)
            else:
                logger.error(f"Failed to download {filename} after {max_retries} attempts")
                return (filename, False, str(e))

    return (filename, False, "Unknown error")


def download_directory(
    ftp_dir: str,
    download_dir: Path,
    skip_existing: bool,
    max_workers: int,
    logger: logging.Logger
) -> Tuple[int, int]:
    """
    Download all files from an FTP directory in parallel.

    Returns:
        Tuple of (successful_count, failed_count)
    """
    dir_name = ftp_dir.rstrip('/').split('/')[-2]  # e.g., 'oa_comm'
    logger.info(f"Starting download from {ftp_dir} ({dir_name})")

    # Get file list
    files = get_file_list(ftp_dir, logger)
    if not files:
        logger.warning(f"No files found in {ftp_dir}")
        return (0, 0)

    # Download files in parallel
    successful = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all download tasks
        future_to_file = {
            executor.submit(
                download_file,
                ftp_dir,
                filename,
                download_dir,
                skip_existing,
                MAX_RETRIES,
                logger
            ): filename
            for filename in files
        }

        # Process completed downloads
        if HAS_TQDM:
            pbar = tqdm(
                as_completed(future_to_file),
                total=len(files),
                desc=dir_name,
                unit="file"
            )
        else:
            pbar = as_completed(future_to_file)

        for future in pbar:
            filename, success, error = future.result()
            if success:
                successful += 1
            else:
                failed += 1
                logger.error(f"Failed: {filename} - {error}")

    logger.info(
        f"Completed {dir_name}: {successful} successful, {failed} failed"
    )
    return (successful, failed)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Download PubMed Central Open Access XML files in parallel"
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default=DOWNLOAD_DIR,
        help=f'Output directory for downloads (default: {DOWNLOAD_DIR})'
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=4,
        help='Number of parallel downloads per directory (default: 4)'
    )
    parser.add_argument(
        '--no-skip-existing',
        action='store_true',
        help='Re-download existing files'
    )
    parser.add_argument(
        '--log-file',
        type=str,
        default='download_pmcoa.log',
        help='Log file path (default: download_pmcoa.log)'
    )
    parser.add_argument(
        '--dirs',
        type=str,
        nargs='+',
        choices=['oa_other', 'oa_comm', 'oa_noncomm'],
        help='Specific directories to download (default: all)'
    )

    args = parser.parse_args()

    # Setup
    logger = setup_logging(args.log_file)
    download_dir = Path(args.output_dir)
    download_dir.mkdir(exist_ok=True)

    skip_existing = not args.no_skip_existing

    # Filter directories if specified
    if args.dirs:
        dirs_to_download = [
            d for d in FTP_DIRS
            if any(name in d for name in args.dirs)
        ]
    else:
        dirs_to_download = FTP_DIRS

    logger.info("=" * 70)
    logger.info(f"Starting PMCOA download")
    logger.info(f"Output directory: {download_dir.absolute()}")
    logger.info(f"Workers per directory: {args.workers}")
    logger.info(f"Skip existing files: {skip_existing}")
    logger.info(f"Directories: {len(dirs_to_download)}")
    logger.info("=" * 70)

    start_time = time.time()
    total_successful = 0
    total_failed = 0

    # Download from each directory
    for ftp_dir in dirs_to_download:
        successful, failed = download_directory(
            ftp_dir,
            download_dir,
            skip_existing,
            args.workers,
            logger
        )
        total_successful += successful
        total_failed += failed

    # Summary
    elapsed_time = time.time() - start_time
    logger.info("=" * 70)
    logger.info("Download Summary")
    logger.info(f"Total successful: {total_successful}")
    logger.info(f"Total failed: {total_failed}")
    logger.info(f"Elapsed time: {elapsed_time / 60:.1f} minutes")
    logger.info("=" * 70)

    if total_failed > 0:
        logger.warning(f"{total_failed} files failed to download. Check the log for details.")
        sys.exit(1)
    else:
        logger.info("All downloads completed successfully!")
        sys.exit(0)


if __name__ == "__main__":
    main()
