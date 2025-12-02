#!/usr/bin/env python3
"""
Build funder corpus totals from rtrans parquet files.

This script scans ALL rtrans files to count how many articles acknowledge each funder.
These totals serve as denominators for calculating funder open data percentages.

Usage:
    python analysis/build_funder_corpus_totals.py \
        --rtrans-dir ~/claude/pmcoaXMLs/rtrans_out_full_parquets \
        --funders-csv results/openss_explore_v2/all_potential_funders.csv \
        --output results/funder_corpus_totals.parquet \
        --min-count 100

Author: INCF 2025 Poster Analysis
Date: 2025-12-02
"""

import argparse
import gc
import glob
import logging
import re
from collections import Counter
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def load_funders(funders_csv: Path, min_count: int = 100) -> list:
    """
    Load funders from the discovery CSV.
    Only include funders with count >= min_count in open data subset.
    """
    df = pd.read_csv(funders_csv)

    # Filter by minimum count
    df = df[df['count'] >= min_count]

    # Sort by count descending
    df = df.sort_values('count', ascending=False)

    funders = df['name'].tolist()
    logger.info(f"Loaded {len(funders)} funders with count >= {min_count}")

    return funders


def search_funder_in_text(text: str, funder: str) -> bool:
    """
    Search for a funder name in funding text.
    Uses case-insensitive matching with word boundaries for short names.
    """
    if pd.isna(text) or not text:
        return False

    text = str(text)

    # For short names (<=5 chars), use word boundary matching
    if len(funder) <= 5:
        pattern = r'\b' + re.escape(funder) + r'\b'
        return bool(re.search(pattern, text, re.IGNORECASE))
    else:
        # For longer names, simple case-insensitive contains
        return funder.lower() in text.lower()


def count_funders_in_rtrans(rtrans_dir: Path, funders: list, limit: int = None) -> dict:
    """
    Count how many articles acknowledge each funder across all rtrans files.

    Returns dict mapping funder -> count
    """
    parquet_files = sorted(glob.glob(f'{rtrans_dir}/*.parquet'))

    if limit:
        parquet_files = parquet_files[:limit]
        logger.info(f"Limited to first {limit} files for testing")

    logger.info(f"Processing {len(parquet_files)} rtrans parquet files")
    logger.info(f"Searching for {len(funders)} funders")

    # Initialize counters
    funder_counts = Counter()
    total_records = 0
    records_with_funding = 0

    funding_cols = ['fund_text', 'fund_pmc_institute', 'fund_pmc_source', 'fund_pmc_anysource']

    for i, pf in enumerate(parquet_files):
        try:
            df = pd.read_parquet(pf)
            total_records += len(df)

            # Get available funding columns
            available_cols = [c for c in funding_cols if c in df.columns]

            if not available_cols:
                continue

            # Combine all funding text into one column for searching
            df['combined_fund'] = ''
            for col in available_cols:
                df['combined_fund'] = df['combined_fund'] + ' ' + df[col].fillna('').astype(str)

            # Count records with any funding text
            has_funding = df['combined_fund'].str.len() > 10
            records_with_funding += has_funding.sum()

            # For each funder, count matches
            for funder in funders:
                matches = df['combined_fund'].apply(lambda x: search_funder_in_text(x, funder))
                funder_counts[funder] += matches.sum()

            del df
            gc.collect()

            if (i + 1) % 50 == 0:
                logger.info(f"  Processed {i+1}/{len(parquet_files)} files, {total_records:,} total records")

        except Exception as e:
            logger.warning(f"Error processing {Path(pf).name}: {e}")

    logger.info(f"Finished processing {len(parquet_files)} files")
    logger.info(f"Total records: {total_records:,}")
    logger.info(f"Records with funding text: {records_with_funding:,} ({100*records_with_funding/total_records:.1f}%)")

    return {
        'funder_counts': funder_counts,
        'total_records': total_records,
        'records_with_funding': records_with_funding
    }


def main():
    parser = argparse.ArgumentParser(
        description='Build funder corpus totals from rtrans files',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--rtrans-dir', type=Path, required=True,
                        help='Directory containing rtrans parquet files')
    parser.add_argument('--funders-csv', type=Path, required=True,
                        help='CSV with funder names (from discovery)')
    parser.add_argument('--output', type=Path, required=True,
                        help='Output parquet file')
    parser.add_argument('--min-count', type=int, default=100,
                        help='Minimum count in open data subset to include funder')
    parser.add_argument('--limit', type=int, default=None,
                        help='Limit number of rtrans files (for testing)')

    args = parser.parse_args()

    logger.info("=" * 70)
    logger.info("BUILD FUNDER CORPUS TOTALS")
    logger.info("=" * 70)

    # Load funders
    funders = load_funders(args.funders_csv, args.min_count)

    # Count funders in all rtrans files
    results = count_funders_in_rtrans(args.rtrans_dir, funders, args.limit)

    # Create output DataFrame
    rows = []
    for funder, count in results['funder_counts'].most_common():
        rows.append({
            'funder': funder,
            'corpus_total': count,
        })

    df = pd.DataFrame(rows)

    # Add metadata
    df.attrs['total_records'] = results['total_records']
    df.attrs['records_with_funding'] = results['records_with_funding']

    # Save output
    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(args.output, index=False)
    logger.info(f"Saved {len(df)} funder totals to {args.output}")

    # Also save as CSV
    csv_path = args.output.with_suffix('.csv')
    df.to_csv(csv_path, index=False)
    logger.info(f"Also saved to {csv_path}")

    # Print summary
    print("\n" + "=" * 70)
    print("TOP FUNDERS BY CORPUS TOTAL")
    print("=" * 70)
    for _, row in df.head(30).iterrows():
        print(f"  {row['corpus_total']:>10,}  {row['funder']}")

    print(f"\nTotal records in corpus: {results['total_records']:,}")
    print(f"Records with funding text: {results['records_with_funding']:,}")


if __name__ == '__main__':
    main()
