#!/usr/bin/env python3
"""Merge oddpub batch results into single parquet file.

This script combines all batch output files into a single parquet file.
"""

import pandas as pd
from pathlib import Path
import sys
import argparse
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main(input_dir, output_file, check_missing=False):
    """Merge all parquet files in directory."""

    input_dir = Path(input_dir)
    output_file = Path(output_file)

    if not input_dir.exists():
        logger.error(f"Input directory does not exist: {input_dir}")
        sys.exit(1)

    # Find all result files
    parquet_files = sorted(input_dir.glob("batch_*_results.parquet"))

    if len(parquet_files) == 0:
        logger.error(f"No batch result files found in {input_dir}")
        sys.exit(1)

    logger.info(f"Found {len(parquet_files):,} result files")

    # Check for missing batches if requested
    if check_missing:
        batch_numbers = set()
        for pf in parquet_files:
            # Extract batch number from filename like batch_00123_results.parquet
            stem = pf.stem  # batch_00123_results
            parts = stem.split('_')
            if len(parts) >= 2:
                try:
                    batch_num = int(parts[1])
                    batch_numbers.add(batch_num)
                except ValueError:
                    pass

        if batch_numbers:
            expected_batches = set(range(max(batch_numbers) + 1))
            missing = expected_batches - batch_numbers

            if missing:
                logger.warning(f"Missing {len(missing)} batches:")
                missing_list = sorted(list(missing))[:20]  # Show first 20
                logger.warning(f"  {missing_list}")
                if len(missing) > 20:
                    logger.warning(f"  ... and {len(missing) - 20} more")
            else:
                logger.info("All expected batch files are present")

    # Read and concatenate
    dfs = []
    total_records = 0

    for i, pf in enumerate(parquet_files):
        try:
            df = pd.read_parquet(pf)
            dfs.append(df)
            total_records += len(df)

            if (i + 1) % 100 == 0:
                logger.info(f"Loaded {i + 1:,}/{len(parquet_files):,} files ({total_records:,} records so far)...")
        except Exception as e:
            logger.error(f"Error reading {pf}: {e}")
            continue

    if not dfs:
        logger.error("No data frames could be loaded")
        sys.exit(1)

    # Concatenate all
    logger.info("Concatenating all results...")
    combined = pd.concat(dfs, ignore_index=True)

    logger.info(f"Total records: {len(combined):,}")
    logger.info(f"Columns: {list(combined.columns)}")

    # Save
    logger.info(f"Saving to {output_file}...")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(output_file, index=False)

    # Summary stats
    file_size_mb = output_file.stat().st_size / (1024**2)

    logger.info("\n" + "="*70)
    logger.info("SUMMARY")
    logger.info("="*70)
    logger.info(f"Total articles: {len(combined):,}")

    if 'is_open_data' in combined.columns:
        open_data_count = combined['is_open_data'].sum()
        open_data_pct = 100 * combined['is_open_data'].mean()
        logger.info(f"Open data detected: {open_data_count:,} ({open_data_pct:.2f}%)")

    if 'is_open_code' in combined.columns:
        open_code_count = combined['is_open_code'].sum()
        open_code_pct = 100 * combined['is_open_code'].mean()
        logger.info(f"Open code detected: {open_code_count:,} ({open_code_pct:.2f}%)")

    logger.info(f"Output file: {output_file}")
    logger.info(f"File size: {file_size_mb:.1f} MB")
    logger.info("="*70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Merge oddpub batch results into single parquet file'
    )
    parser.add_argument(
        'input_dir',
        help='Directory containing batch result files (batch_*_results.parquet)'
    )
    parser.add_argument(
        'output_file',
        help='Output parquet file path'
    )
    parser.add_argument(
        '--check-missing',
        action='store_true',
        help='Check for missing batch files'
    )

    args = parser.parse_args()

    main(args.input_dir, args.output_file, args.check_missing)
