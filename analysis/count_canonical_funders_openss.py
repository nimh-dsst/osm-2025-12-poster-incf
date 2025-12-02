#!/usr/bin/env python3
"""
Count canonical funders in open data subset (OpenSS).

This script counts how many open data articles acknowledge each canonical funder,
using alias mapping to merge variants and avoid double-counting.

Usage:
    python analysis/count_canonical_funders_openss.py \
        --oddpub-file ~/claude/pmcoaXMLs/oddpub_merged/oddpub_v7.2.3_all.parquet \
        --rtrans-dir ~/claude/pmcoaXMLs/rtrans_out_full_parquets \
        --output results/canonical_funder_openss_counts.parquet

Author: INCF 2025 Poster Analysis
Date: 2025-12-02
"""

import argparse
import gc
import glob
import logging
import sys
from collections import Counter
from pathlib import Path

import pandas as pd

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from funder_analysis.normalize_funders import FunderNormalizer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def normalize_pmcid(pmcid: str) -> str:
    """Normalize PMCID to PMC######### format.

    Handles various formats:
    - PMC12345
    - 12345
    - PMCPMC12345.txt (from oddpub article column)
    """
    if pd.isna(pmcid):
        return None
    pmcid = str(pmcid).strip().upper()

    # Handle PMCPMC12345.txt format from oddpub
    if pmcid.startswith('PMCPMC'):
        pmcid = pmcid[3:]  # Remove first "PMC" prefix

    # Remove .txt suffix
    if pmcid.endswith('.TXT'):
        pmcid = pmcid[:-4]

    # Ensure PMC prefix
    if not pmcid.startswith('PMC'):
        pmcid = 'PMC' + pmcid

    return pmcid


def load_open_data_pmcids(oddpub_file: Path) -> set:
    """
    Load PMCIDs of articles with open data detected by oddpub v7.2.3.
    """
    logger.info(f"Loading open data PMCIDs from {oddpub_file}")
    df = pd.read_parquet(oddpub_file)
    logger.info(f"Loaded {len(df):,} records")

    # Filter for open data
    open_data_df = df[df['is_open_data'] == True]
    logger.info(f"Found {len(open_data_df):,} with is_open_data=true ({100*len(open_data_df)/len(df):.2f}%)")

    # Extract PMCIDs - check article column first (has PMCPMC format)
    pmcids = set()
    for col in ['article', 'pmcid', 'filename']:
        if col in open_data_df.columns:
            for val in open_data_df[col].dropna().unique():
                pmcid = normalize_pmcid(val)
                if pmcid:
                    pmcids.add(pmcid)

    logger.info(f"Extracted {len(pmcids):,} unique PMCIDs with open data")
    return pmcids


def count_canonical_funders_in_openss(rtrans_dir: Path,
                                       normalizer: FunderNormalizer,
                                       open_data_pmcids: set,
                                       limit: int = None) -> dict:
    """
    Count canonical funders in open data articles only.

    Returns dict with funder_counts, total_matched, records_with_funding
    """
    parquet_files = sorted(glob.glob(f'{rtrans_dir}/*.parquet'))

    if limit:
        parquet_files = parquet_files[:limit]
        logger.info(f"Limited to first {limit} files for testing")

    canonical_funders = normalizer.get_all_canonical_names()
    logger.info(f"Processing {len(parquet_files)} rtrans parquet files")
    logger.info(f"Searching for {len(canonical_funders)} canonical funders in {len(open_data_pmcids):,} open data articles")

    # Initialize counters
    funder_counts = Counter()
    total_matched = 0
    records_with_funding = 0

    funding_cols = ['fund_text', 'fund_pmc_institute', 'fund_pmc_source', 'fund_pmc_anysource']

    for i, pf in enumerate(parquet_files):
        try:
            df = pd.read_parquet(pf)

            # Normalize PMCID for matching - apply same normalization as oddpub PMCIDs
            if 'pmcid_pmc' in df.columns:
                df['pmcid_norm'] = df['pmcid_pmc'].apply(normalize_pmcid)
            elif 'pmcid' in df.columns:
                df['pmcid_norm'] = df['pmcid'].apply(normalize_pmcid)
            else:
                continue

            # Filter to open data articles only
            df = df[df['pmcid_norm'].isin(open_data_pmcids)]

            if len(df) == 0:
                continue

            total_matched += len(df)

            # Get available funding columns
            available_cols = [c for c in funding_cols if c in df.columns]

            if not available_cols:
                continue

            # Combine all funding text into one column
            df['combined_fund'] = ''
            for col in available_cols:
                df['combined_fund'] = df['combined_fund'] + ' ' + df[col].fillna('').astype(str)

            # Count records with any funding text
            has_funding = df['combined_fund'].str.len() > 10
            records_with_funding += has_funding.sum()

            # For each canonical funder, count matches
            for canonical in canonical_funders:
                pattern = normalizer.search_patterns.get(canonical)
                if pattern:
                    matches = df['combined_fund'].str.contains(
                        pattern.pattern, case=False, na=False, regex=True
                    )
                    funder_counts[canonical] += matches.sum()

            del df
            gc.collect()

            if (i + 1) % 100 == 0:
                logger.info(f"  Processed {i+1}/{len(parquet_files)} files, {total_matched:,} open data articles matched")

        except Exception as e:
            logger.warning(f"Error processing {Path(pf).name}: {e}")

    logger.info(f"Finished processing {len(parquet_files)} files")
    logger.info(f"Total open data articles matched: {total_matched:,}")
    logger.info(f"Of those, with funding text: {records_with_funding:,} ({100*records_with_funding/max(total_matched,1):.1f}%)")

    return {
        'funder_counts': funder_counts,
        'total_matched': total_matched,
        'records_with_funding': records_with_funding
    }


def main():
    parser = argparse.ArgumentParser(
        description='Count canonical funders in open data articles',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--oddpub-file', type=Path, required=True,
                        help='Merged oddpub parquet file')
    parser.add_argument('--rtrans-dir', type=Path, required=True,
                        help='Directory containing rtrans parquet files')
    parser.add_argument('--aliases-csv', type=Path, default=None,
                        help='Path to funder_aliases.csv (uses default if not specified)')
    parser.add_argument('--output', type=Path, required=True,
                        help='Output parquet file')
    parser.add_argument('--limit', type=int, default=None,
                        help='Limit number of rtrans files (for testing)')

    args = parser.parse_args()

    logger.info("=" * 70)
    logger.info("COUNT CANONICAL FUNDERS IN OPEN DATA ARTICLES")
    logger.info("=" * 70)

    # Load open data PMCIDs
    open_data_pmcids = load_open_data_pmcids(args.oddpub_file)

    # Initialize normalizer
    normalizer = FunderNormalizer(args.aliases_csv)
    logger.info(f"Loaded {len(normalizer.get_all_canonical_names())} canonical funders with alias mapping")

    # Count funders in open data articles
    results = count_canonical_funders_in_openss(
        args.rtrans_dir, normalizer, open_data_pmcids, args.limit
    )

    # Create output DataFrame
    rows = []
    for funder, count in results['funder_counts'].most_common():
        variants = normalizer.get_variants(funder)
        rows.append({
            'funder': funder,
            'open_data_count': count,
            'num_variants': len(variants),
            'variants': '; '.join(sorted(variants))
        })

    df = pd.DataFrame(rows)

    # Add metadata
    df.attrs['total_open_data_articles'] = int(len(open_data_pmcids))
    df.attrs['total_matched'] = int(results['total_matched'])
    df.attrs['records_with_funding'] = int(results['records_with_funding'])

    # Save output
    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(args.output, index=False)
    logger.info(f"Saved {len(df)} canonical funder counts to {args.output}")

    # Also save as CSV
    csv_path = args.output.with_suffix('.csv')
    df.to_csv(csv_path, index=False)
    logger.info(f"Also saved to {csv_path}")

    # Print summary
    print("\n" + "=" * 70)
    print("CANONICAL FUNDERS IN OPEN DATA ARTICLES")
    print("=" * 70)
    for _, row in df.head(30).iterrows():
        print(f"  {row['open_data_count']:>10,}  {row['funder']}")

    print(f"\nTotal open data PMCIDs: {len(open_data_pmcids):,}")
    print(f"Matched in rtrans files: {results['total_matched']:,}")
    print(f"With funding text: {results['records_with_funding']:,}")


if __name__ == '__main__':
    main()
