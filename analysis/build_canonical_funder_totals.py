#!/usr/bin/env python3
"""
Build corpus totals for canonical funders from rtrans parquet files.

This script scans ALL rtrans files to count how many articles acknowledge each
canonical funder (using alias mapping to merge variants).

Key difference from build_funder_corpus_totals.py:
- Uses FunderNormalizer with alias mapping (43 funders)
- Searches for ALL variants of each funder at article level
- Avoids double-counting when article mentions both "NSF" and "National Science Foundation"

Usage:
    python analysis/build_canonical_funder_totals.py \
        --rtrans-dir ~/claude/pmcoaXMLs/rtrans_out_full_parquets \
        --output results/canonical_funder_corpus_totals.parquet

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


def count_canonical_funders_in_rtrans(rtrans_dir: Path,
                                      normalizer: FunderNormalizer,
                                      limit: int = None) -> dict:
    """
    Count how many articles acknowledge each canonical funder.

    Uses normalizer to search for all variants at article level,
    counting each article only once per funder.

    Returns dict with funder_counts, total_records, records_with_funding
    """
    parquet_files = sorted(glob.glob(f'{rtrans_dir}/*.parquet'))

    if limit:
        parquet_files = parquet_files[:limit]
        logger.info(f"Limited to first {limit} files for testing")

    canonical_funders = normalizer.get_all_canonical_names()
    logger.info(f"Processing {len(parquet_files)} rtrans parquet files")
    logger.info(f"Searching for {len(canonical_funders)} canonical funders")

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

            # For each canonical funder, count articles mentioning any variant
            # Use vectorized str.contains with regex pattern instead of .apply()
            for canonical in canonical_funders:
                pattern = normalizer.search_patterns.get(canonical)
                if pattern:
                    # Use the compiled pattern's string form for str.contains
                    matches = df['combined_fund'].str.contains(
                        pattern.pattern, case=False, na=False, regex=True
                    )
                    funder_counts[canonical] += matches.sum()

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
        description='Build canonical funder corpus totals from rtrans files',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
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
    logger.info("BUILD CANONICAL FUNDER CORPUS TOTALS")
    logger.info("=" * 70)

    # Initialize normalizer
    normalizer = FunderNormalizer(args.aliases_csv)
    logger.info(f"Loaded {len(normalizer.get_all_canonical_names())} canonical funders with alias mapping")

    # Count funders in all rtrans files
    results = count_canonical_funders_in_rtrans(args.rtrans_dir, normalizer, args.limit)

    # Create output DataFrame
    rows = []
    for funder, count in results['funder_counts'].most_common():
        variants = normalizer.get_variants(funder)
        rows.append({
            'funder': funder,
            'corpus_total': count,
            'num_variants': len(variants),
            'variants': '; '.join(sorted(variants))
        })

    df = pd.DataFrame(rows)

    # Add metadata (convert numpy int64 to Python int for JSON serialization)
    df.attrs['total_records'] = int(results['total_records'])
    df.attrs['records_with_funding'] = int(results['records_with_funding'])

    # Save output
    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(args.output, index=False)
    logger.info(f"Saved {len(df)} canonical funder totals to {args.output}")

    # Also save as CSV
    csv_path = args.output.with_suffix('.csv')
    df.to_csv(csv_path, index=False)
    logger.info(f"Also saved to {csv_path}")

    # Print summary
    print("\n" + "=" * 70)
    print("CANONICAL FUNDERS BY CORPUS TOTAL")
    print("=" * 70)
    for _, row in df.head(30).iterrows():
        print(f"  {row['corpus_total']:>10,}  {row['funder']}")

    print(f"\nTotal records in corpus: {results['total_records']:,}")
    print(f"Records with funding text: {results['records_with_funding']:,}")


if __name__ == '__main__':
    main()
