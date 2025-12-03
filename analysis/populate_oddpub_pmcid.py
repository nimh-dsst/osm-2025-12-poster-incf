#!/usr/bin/env python3
"""
Populate the pmcid column in oddpub parquet files.

The oddpub output files have the PMCID embedded in the 'article' column
(format: PMCPMC544856.txt -> PMC544856) but the pmcid column is empty.
This script extracts and normalizes the PMCID.

Usage:
    python populate_oddpub_pmcid.py \
        --input ~/claude/pmcoaXMLs/oddpub_merged/oddpub_v7.2.3_all.parquet \
        --output ~/claude/pmcoaXMLs/oddpub_merged/oddpub_v7.2.3_all_with_pmcid.parquet
"""

import argparse
import re
import sys
from pathlib import Path
from typing import Optional

import pandas as pd


def extract_pmcid(article: str) -> Optional[str]:
    """Extract normalized PMCID from article column.

    Handles formats:
    - PMCPMC544856.txt -> PMC544856
    - PMC544856.txt -> PMC544856
    - PMC544856 -> PMC544856
    """
    if pd.isna(article):
        return None

    article_str = str(article)

    # Format: PMCPMC123456.txt
    match = re.match(r'PMC(PMC\d+)\.txt', article_str)
    if match:
        return match.group(1)

    # Format: PMC123456.txt or PMC123456
    match = re.match(r'(PMC\d+)', article_str)
    if match:
        return match.group(1)

    return None


def main():
    parser = argparse.ArgumentParser(description="Populate pmcid column in oddpub parquet")
    parser.add_argument(
        "--input",
        required=True,
        help="Input oddpub parquet file",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output parquet file with pmcid populated",
    )
    parser.add_argument(
        "--in-place",
        action="store_true",
        help="Update the input file in place (overwrites original)",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output) if not args.in_place else input_path

    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}")
        sys.exit(1)

    print(f"Loading {input_path}...")
    df = pd.read_parquet(input_path)
    print(f"  Loaded {len(df):,} records")
    print(f"  Columns: {df.columns.tolist()}")

    # Check current pmcid column state
    if 'pmcid' in df.columns:
        non_null_count = df['pmcid'].notna().sum()
        print(f"  Current pmcid non-null: {non_null_count:,} ({100*non_null_count/len(df):.2f}%)")
    else:
        print("  No pmcid column exists")

    # Check if article column exists
    if 'article' not in df.columns:
        print("Error: No 'article' column found in the file")
        sys.exit(1)

    # Extract PMCIDs
    print("Extracting PMCIDs from article column...")
    df['pmcid'] = df['article'].apply(extract_pmcid)

    # Report results
    extracted_count = df['pmcid'].notna().sum()
    print(f"  Extracted {extracted_count:,} PMCIDs ({100*extracted_count/len(df):.2f}%)")

    # Show sample of extractions
    print("\nSample extractions:")
    sample = df[df['pmcid'].notna()].head(5)[['article', 'pmcid']]
    for _, row in sample.iterrows():
        print(f"  {row['article']} -> {row['pmcid']}")

    # Show any that failed extraction
    failed = df[df['pmcid'].isna()]
    if len(failed) > 0:
        print(f"\nWarning: {len(failed):,} records failed PMCID extraction")
        print("Sample failed articles:")
        for article in failed['article'].head(5):
            print(f"  {article}")

    # Save output
    print(f"\nSaving to {output_path}...")
    df.to_parquet(output_path, index=False)
    print(f"  Done. Output size: {output_path.stat().st_size / 1024 / 1024:.1f} MB")

    # Verify
    verify = pd.read_parquet(output_path)
    verify_count = verify['pmcid'].notna().sum()
    print(f"  Verification: {verify_count:,} pmcid values populated")


if __name__ == "__main__":
    main()
