#!/usr/bin/env python3
"""
Merge oddpub results from multiple output directories.

This script combines oddpub parquet files from:
1. New output directory: PMC*_chunk*_results.parquet
2. Old output directory: oa_*_xml.PMC*.baseline.*_results.parquet

The merged output contains all unique records by pmcid.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import List, Tuple

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def find_parquet_files(directory: Path, pattern: str) -> List[Path]:
    """Find parquet files matching pattern in directory."""
    files = sorted(directory.glob(pattern))
    return files


def load_and_concat_parquets(files: List[Path], source_name: str) -> pd.DataFrame:
    """Load multiple parquet files and concatenate them."""
    if not files:
        logger.warning(f"No files found for {source_name}")
        return pd.DataFrame()

    logger.info(f"Loading {len(files)} files from {source_name}...")

    dfs = []
    errors = 0

    for i, f in enumerate(files, 1):
        try:
            df = pd.read_parquet(f)
            dfs.append(df)

            if i % 500 == 0:
                logger.info(f"  Loaded {i}/{len(files)} files...")
        except Exception as e:
            logger.warning(f"Error loading {f}: {e}")
            errors += 1

    if not dfs:
        logger.error(f"No valid parquet files loaded from {source_name}")
        return pd.DataFrame()

    result = pd.concat(dfs, ignore_index=True)
    logger.info(f"Loaded {len(result):,} records from {source_name} ({errors} errors)")

    return result


def normalize_pmcid(pmcid: str) -> str:
    """Normalize PMCID to consistent format (PMCxxxxxxx)."""
    if pd.isna(pmcid) or pmcid == '':
        return ''
    pmcid = str(pmcid).strip()
    if not pmcid.startswith('PMC'):
        pmcid = f'PMC{pmcid}'
    return pmcid


def merge_oddpub_results(
    new_dir: Path,
    old_dir: Path,
    output_file: Path
) -> Tuple[int, int, int]:
    """
    Merge oddpub results from new and old directories.

    Returns:
        Tuple of (new_records, old_records, total_unique)
    """
    # Find files in each directory
    new_files = find_parquet_files(new_dir, "PMC*_results.parquet")
    new_files.extend(find_parquet_files(new_dir, "PMC*_chunk*_results.parquet"))

    old_files = find_parquet_files(old_dir, "oa_*_xml.PMC*_results.parquet")
    old_files.extend(find_parquet_files(old_dir, "oa_*_xml.PMC*_chunk*_results.parquet"))

    logger.info(f"Found {len(new_files)} new-style files")
    logger.info(f"Found {len(old_files)} old-style files")

    # Load and concatenate
    new_df = load_and_concat_parquets(new_files, "new directory")
    old_df = load_and_concat_parquets(old_files, "old directory")

    new_count = len(new_df)
    old_count = len(old_df)

    # Combine both sources
    if new_df.empty and old_df.empty:
        logger.error("No records found in either directory")
        return 0, 0, 0
    elif new_df.empty:
        combined = old_df
    elif old_df.empty:
        combined = new_df
    else:
        combined = pd.concat([new_df, old_df], ignore_index=True)

    logger.info(f"Combined: {len(combined):,} total records (before dedup)")

    # Normalize pmcid for deduplication
    combined['pmcid_norm'] = combined['pmcid'].apply(normalize_pmcid)

    # Check for duplicates
    dup_count = combined.duplicated(subset=['pmcid_norm'], keep='first').sum()
    if dup_count > 0:
        logger.info(f"Found {dup_count:,} duplicate PMCIDs - keeping first occurrence")
        combined = combined.drop_duplicates(subset=['pmcid_norm'], keep='first')

    # Remove normalization column
    combined = combined.drop(columns=['pmcid_norm'])

    # Sort by pmcid for consistent output
    combined = combined.sort_values('pmcid').reset_index(drop=True)

    total_unique = len(combined)
    logger.info(f"Final: {total_unique:,} unique records")

    # Save output
    output_file.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(output_file, index=False)

    file_size_mb = output_file.stat().st_size / (1024 * 1024)
    logger.info(f"Saved to {output_file} ({file_size_mb:.1f} MB)")

    return new_count, old_count, total_unique


def main():
    parser = argparse.ArgumentParser(
        description='Merge oddpub results from multiple output directories'
    )
    parser.add_argument(
        '--new-dir',
        type=Path,
        default=Path('/data/NIMH_scratch/adamt/osm/oddpub_output'),
        help='Directory with new-style outputs (PMC*_results.parquet)'
    )
    parser.add_argument(
        '--old-dir',
        type=Path,
        default=Path('/data/NIMH_scratch/adamt/osm/osm-2025-12-poster-incf/output'),
        help='Directory with old-style outputs (oa_*_xml.PMC*_results.parquet)'
    )
    parser.add_argument(
        '-o', '--output',
        type=Path,
        required=True,
        help='Output parquet file path'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Only count files, do not merge'
    )

    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Merging oddpub results")
    logger.info("=" * 60)
    logger.info(f"New directory: {args.new_dir}")
    logger.info(f"Old directory: {args.old_dir}")
    logger.info(f"Output file: {args.output}")

    if not args.new_dir.exists():
        logger.error(f"New directory does not exist: {args.new_dir}")
        sys.exit(1)

    if not args.old_dir.exists():
        logger.error(f"Old directory does not exist: {args.old_dir}")
        sys.exit(1)

    if args.dry_run:
        new_files = list(args.new_dir.glob("PMC*_results.parquet"))
        new_files.extend(args.new_dir.glob("PMC*_chunk*_results.parquet"))
        old_files = list(args.old_dir.glob("oa_*_xml.PMC*_results.parquet"))
        old_files.extend(args.old_dir.glob("oa_*_xml.PMC*_chunk*_results.parquet"))

        logger.info(f"Dry run - would merge:")
        logger.info(f"  {len(new_files)} new-style files")
        logger.info(f"  {len(old_files)} old-style files")
        return

    new_count, old_count, total = merge_oddpub_results(
        args.new_dir,
        args.old_dir,
        args.output
    )

    logger.info("")
    logger.info("=" * 60)
    logger.info("MERGE SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Records from new directory: {new_count:,}")
    logger.info(f"Records from old directory: {old_count:,}")
    logger.info(f"Total unique records: {total:,}")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()
