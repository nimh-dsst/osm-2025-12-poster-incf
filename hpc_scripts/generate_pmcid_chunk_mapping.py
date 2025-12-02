#!/usr/bin/env python3
"""
Generate a static mapping of PMCIDs to chunks and source tar.gz files.

This creates a parquet file that maps each PMCID to:
- chunk_name: The processing chunk (e.g., PMC001xxxxxx_chunk0)
- pmc_dir: The PMC directory (e.g., PMC001xxxxxx)
- source_tarball: The original tar.gz file
- csv_file: The file list CSV

This mapping is used by verify_and_retry_oddpub_extracted.sh for fast lookups.
"""

import argparse
import logging
import os
import sys
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def generate_mapping(xml_base_dir: Path, files_per_job: int = 1000) -> pd.DataFrame:
    """
    Generate PMCID to chunk mapping from CSV file lists.

    Returns DataFrame with columns:
    - pmcid: The PMCID (e.g., PMC12345)
    - chunk_name: The chunk identifier (e.g., PMC001xxxxxx_chunk0)
    - pmc_dir: The PMC directory (e.g., PMC001xxxxxx)
    - source_tarball: The original tar.gz file
    - csv_file: Path to the file list CSV
    - chunk_idx: Index within chunk (0-based)
    """
    records = []

    # Find all CSV file lists
    csv_files = sorted(xml_base_dir.glob("*.baseline.*.filelist.csv"))
    logger.info(f"Found {len(csv_files)} CSV file lists")

    for csv_file in csv_files:
        # Extract PMC directory name from filename
        # Format: PMC001xxxxxx.baseline.2024-09-25.filelist.csv
        basename = csv_file.name
        parts = basename.split('.')
        if len(parts) < 4:
            logger.warning(f"Unexpected filename format: {basename}")
            continue

        pmc_dir = parts[0]  # e.g., PMC001xxxxxx
        source_tarball = f"oa_comm_xml.{pmc_dir}.baseline.{parts[2]}.tar.gz"

        # Read CSV file
        try:
            df = pd.read_csv(csv_file)
            if 'file' not in df.columns and len(df.columns) > 0:
                # Assume first column is file path
                df.columns = ['file'] + list(df.columns[1:])
        except Exception as e:
            logger.warning(f"Error reading {csv_file}: {e}")
            continue

        if df.empty:
            continue

        total_files = len(df)
        num_chunks = (total_files + files_per_job - 1) // files_per_job

        for idx, row in enumerate(df.itertuples()):
            file_path = getattr(row, 'file', None) or getattr(row, df.columns[0])

            # Extract PMCID from file path
            # Format: PMC001xxxxxx/PMC12345.xml
            pmcid = Path(file_path).stem
            if not pmcid.startswith('PMC'):
                pmcid = f'PMC{pmcid}'

            # Determine chunk
            chunk_num = idx // files_per_job

            if num_chunks == 1:
                chunk_name = pmc_dir
            else:
                chunk_name = f"{pmc_dir}_chunk{chunk_num}"

            records.append({
                'pmcid': pmcid,
                'chunk_name': chunk_name,
                'pmc_dir': pmc_dir,
                'source_tarball': source_tarball,
                'csv_file': str(csv_file),
                'chunk_idx': idx % files_per_job
            })

        logger.info(f"Processed {csv_file.name}: {total_files} files, {num_chunks} chunks")

    df = pd.DataFrame(records)
    logger.info(f"Generated mapping for {len(df):,} PMCIDs across {df['chunk_name'].nunique():,} chunks")

    return df


def main():
    parser = argparse.ArgumentParser(
        description='Generate PMCID to chunk mapping'
    )
    parser.add_argument(
        'xml_base_dir',
        type=Path,
        help='Directory containing PMC XML subdirectories and CSV file lists'
    )
    parser.add_argument(
        '-o', '--output',
        type=Path,
        default=Path('pmcid_chunk_mapping.parquet'),
        help='Output parquet file (default: pmcid_chunk_mapping.parquet)'
    )
    parser.add_argument(
        '--files-per-job',
        type=int,
        default=1000,
        help='Files per processing chunk (default: 1000)'
    )
    parser.add_argument(
        '--csv',
        action='store_true',
        help='Also output as CSV'
    )

    args = parser.parse_args()

    if not args.xml_base_dir.exists():
        logger.error(f"XML base directory does not exist: {args.xml_base_dir}")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("PMCID CHUNK MAPPING GENERATOR")
    logger.info("=" * 60)
    logger.info(f"XML base dir: {args.xml_base_dir}")
    logger.info(f"Files per job: {args.files_per_job}")
    logger.info(f"Output: {args.output}")

    # Generate mapping
    df = generate_mapping(args.xml_base_dir, args.files_per_job)

    if df.empty:
        logger.error("No mapping generated")
        sys.exit(1)

    # Save to parquet
    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(args.output, index=False)

    file_size_mb = args.output.stat().st_size / (1024 * 1024)
    logger.info(f"Saved to {args.output} ({file_size_mb:.1f} MB)")

    # Optionally save CSV
    if args.csv:
        csv_output = args.output.with_suffix('.csv')
        df.to_csv(csv_output, index=False)
        logger.info(f"Also saved to {csv_output}")

    # Print summary
    logger.info("")
    logger.info("SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total PMCIDs: {len(df):,}")
    logger.info(f"Total chunks: {df['chunk_name'].nunique():,}")
    logger.info(f"PMC directories: {df['pmc_dir'].nunique():,}")

    # Chunk size distribution
    chunk_sizes = df.groupby('chunk_name').size()
    logger.info(f"Chunk sizes: min={chunk_sizes.min()}, max={chunk_sizes.max()}, mean={chunk_sizes.mean():.0f}")


if __name__ == '__main__':
    main()
