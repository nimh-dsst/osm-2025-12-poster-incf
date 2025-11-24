#!/usr/bin/env python3
"""
Merge Parquet Files

This script:
1. Concatenates all parquet files in ~/pmcoaXMLs/extracted_metadata_parquet/
2. Populates blank fields in the concatenated data using matching columns from ~/pmcoaXMLs/rtrans_out/

The merge is done on the 'pmid' column. For each row that has a matching pmid in rtrans_out,
blank/null fields in the metadata will be populated with values from rtrans_out if available.
Only overlapping columns are updated - no new columns are added.

Usage:
    python merge_parquet_files.py [options]

Examples:
    python merge_parquet_files.py
    python merge_parquet_files.py -o merged_output.parquet
    python merge_parquet_files.py --metadata-dir ~/pmcoaXMLs/extracted_metadata_parquet --rtrans-dir ~/pmcoaXMLs/rtrans_out
"""

import argparse
import pandas as pd
from pathlib import Path
from glob import glob
import sys
import time
from datetime import datetime


def concatenate_metadata_files(metadata_dir: Path) -> pd.DataFrame:
    """Concatenate all parquet files in the metadata directory."""
    print("="*70)
    print("STEP 1: Concatenating extracted_metadata_parquet files")
    print("="*70)

    # Find all parquet files
    pattern = str(metadata_dir / "*.parquet")
    files = sorted(glob(pattern))

    if not files:
        raise FileNotFoundError(f"No parquet files found in {metadata_dir}")

    print(f"Found {len(files)} parquet files")

    # Read and concatenate all files
    dfs = []
    total_rows = 0

    for i, file_path in enumerate(files, 1):
        filename = Path(file_path).name
        print(f"  [{i}/{len(files)}] Reading {filename}...", end=" ")
        start = time.time()

        df = pd.read_parquet(file_path)
        rows = len(df)
        total_rows += rows
        dfs.append(df)

        elapsed = time.time() - start
        print(f"{rows:,} rows ({elapsed:.1f}s)")

    print(f"\nConcatenating {len(dfs)} dataframes...")
    start = time.time()
    combined = pd.concat(dfs, ignore_index=True)
    elapsed = time.time() - start

    print(f"✓ Concatenated {total_rows:,} total rows in {elapsed:.1f}s")
    print(f"  Shape: {combined.shape}")
    print(f"  Columns: {len(combined.columns)}")
    print(f"  Memory usage: {combined.memory_usage(deep=True).sum() / 1024**2:.1f} MB")

    return combined


def load_rtrans_file(rtrans_dir: Path) -> pd.DataFrame:
    """Load the rtrans parquet file."""
    print("\n" + "="*70)
    print("STEP 2: Loading rtrans_out parquet file")
    print("="*70)

    # Find parquet file in rtrans_out directory
    pattern = str(rtrans_dir / "*.parquet")
    files = glob(pattern)

    if not files:
        raise FileNotFoundError(f"No parquet files found in {rtrans_dir}")

    if len(files) > 1:
        print(f"Warning: Multiple parquet files found in {rtrans_dir}. Using the first one.")

    file_path = files[0]
    filename = Path(file_path).name

    print(f"Reading {filename}...")
    start = time.time()
    df = pd.read_parquet(file_path)
    elapsed = time.time() - start

    print(f"✓ Loaded {len(df):,} rows in {elapsed:.1f}s")
    print(f"  Shape: {df.shape}")
    print(f"  Columns: {list(df.columns)}")
    print(f"  Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")

    return df


def merge_dataframes(metadata_df: pd.DataFrame, rtrans_df: pd.DataFrame) -> pd.DataFrame:
    """Populate blank fields in metadata using rtrans data where pmid matches."""
    print("\n" + "="*70)
    print("STEP 3: Populating blank fields from rtrans_out")
    print("="*70)

    # Check if pmid exists in both dataframes
    if 'pmid' not in metadata_df.columns:
        raise ValueError("'pmid' column not found in metadata dataframe")
    if 'pmid' not in rtrans_df.columns:
        raise ValueError("'pmid' column not found in rtrans dataframe")

    # Convert pmid to string type in both dataframes to ensure they can be joined
    print("\nConverting pmid to string type for joining...")
    metadata_df = metadata_df.copy()
    rtrans_df = rtrans_df.copy()
    metadata_df['pmid'] = metadata_df['pmid'].astype(str)
    rtrans_df['pmid'] = rtrans_df['pmid'].astype(str)

    # Identify overlapping columns (excluding the join key 'pmid')
    common_cols = list(set(metadata_df.columns) & set(rtrans_df.columns) - {'pmid'})

    if not common_cols:
        print("\nWarning: No overlapping columns found (besides pmid).")
        print("No fields will be populated. Returning original metadata.")
        return metadata_df

    print(f"\nOverlapping columns to populate: {sorted(common_cols)}")
    print(f"\nBefore merge:")
    print(f"  Metadata rows: {len(metadata_df):,}")
    print(f"  rtrans rows:   {len(rtrans_df):,}")

    # Count blank values in metadata before merge
    blank_counts_before = {}
    for col in common_cols:
        blank_count = ((metadata_df[col].isna()) | (metadata_df[col] == '')).sum()
        blank_counts_before[col] = blank_count
        if blank_count > 0:
            print(f"  {col}: {blank_count:,} blank values ({100*blank_count/len(metadata_df):.2f}%)")

    # Perform left join to bring rtrans data
    print("\nPerforming left join on 'pmid'...")
    start = time.time()

    # Only select pmid and overlapping columns from rtrans
    rtrans_subset = rtrans_df[['pmid'] + common_cols].copy()

    merged = pd.merge(
        metadata_df,
        rtrans_subset,
        on='pmid',
        how='left',
        suffixes=('', '_rtrans')
    )
    elapsed = time.time() - start

    print(f"✓ Join completed in {elapsed:.1f}s")

    # Now populate blank fields in original columns using rtrans values
    print("\nPopulating blank fields...")
    populated_counts = {}

    for col in common_cols:
        rtrans_col = f"{col}_rtrans"

        if rtrans_col in merged.columns:
            # Find rows where original column is blank but rtrans column has a value
            is_blank = (merged[col].isna()) | (merged[col] == '')
            has_rtrans_value = merged[rtrans_col].notna() & (merged[rtrans_col] != '')
            to_populate = is_blank & has_rtrans_value

            populated_count = to_populate.sum()
            populated_counts[col] = populated_count

            # Populate the blank values
            if populated_count > 0:
                merged.loc[to_populate, col] = merged.loc[to_populate, rtrans_col]
                print(f"  {col}: populated {populated_count:,} values")

            # Drop the temporary rtrans column
            merged.drop(columns=[rtrans_col], inplace=True)

    print(f"\n✓ Field population completed")
    print(f"\nAfter merge:")
    print(f"  Total rows: {len(merged):,}")
    print(f"  Total columns: {len(merged.columns)}")
    print(f"  Memory usage: {merged.memory_usage(deep=True).sum() / 1024**2:.1f} MB")

    # Statistics
    print(f"\nPopulation statistics:")
    total_populated = sum(populated_counts.values())
    print(f"  Total values populated: {total_populated:,}")

    # Check pmid matches
    pmid_matches = merged['pmid'].isin(rtrans_df['pmid']).sum()
    pmid_no_matches = len(merged) - pmid_matches
    print(f"\nMatch statistics:")
    print(f"  Rows with pmid match in rtrans: {pmid_matches:,} ({100*pmid_matches/len(merged):.2f}%)")
    print(f"  Rows without pmid match: {pmid_no_matches:,} ({100*pmid_no_matches/len(merged):.2f}%)")

    return merged


def save_merged_file(df: pd.DataFrame, output_path: Path):
    """Save the populated dataframe to parquet file."""
    print("\n" + "="*70)
    print("STEP 4: Saving populated metadata file")
    print("="*70)

    print(f"Writing to {output_path}...")
    start = time.time()
    df.to_parquet(output_path, index=False)
    elapsed = time.time() - start

    # Get file size
    file_size_mb = output_path.stat().st_size / 1024**2

    print(f"✓ Saved successfully in {elapsed:.1f}s")
    print(f"  Output file: {output_path}")
    print(f"  File size: {file_size_mb:.1f} MB")
    print(f"  Rows: {len(df):,}")
    print(f"  Columns: {len(df.columns)}")


def main():
    parser = argparse.ArgumentParser(
        description='Populate blank fields in metadata using rtrans data (joined on pmid).',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s
  %(prog)s -o populated_metadata.parquet
  %(prog)s --metadata-dir ~/pmcoaXMLs/extracted_metadata_parquet --rtrans-dir ~/pmcoaXMLs/rtrans_out
        """
    )

    parser.add_argument(
        '--metadata-dir',
        type=str,
        default='~/pmcoaXMLs/extracted_metadata_parquet',
        help='Directory containing extracted metadata parquet files (default: ~/pmcoaXMLs/extracted_metadata_parquet)'
    )

    parser.add_argument(
        '--rtrans-dir',
        type=str,
        default='~/pmcoaXMLs/rtrans_out',
        help='Directory containing rtrans parquet file (default: ~/pmcoaXMLs/rtrans_out)'
    )

    parser.add_argument(
        '-o', '--output',
        type=str,
        default=None,
        help='Output file path (default: populated_metadata_YYYYMMDD_HHMMSS.parquet)'
    )

    args = parser.parse_args()

    # Expand paths
    metadata_dir = Path(args.metadata_dir).expanduser()
    rtrans_dir = Path(args.rtrans_dir).expanduser()

    # Determine output path
    if args.output:
        output_path = Path(args.output).expanduser()
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = Path(f"populated_metadata_{timestamp}.parquet")

    # Validate directories
    if not metadata_dir.exists():
        print(f"Error: Metadata directory does not exist: {metadata_dir}", file=sys.stderr)
        return 1

    if not rtrans_dir.exists():
        print(f"Error: rtrans directory does not exist: {rtrans_dir}", file=sys.stderr)
        return 1

    print("="*70)
    print("PARQUET FILE POPULATOR")
    print("="*70)
    print(f"Metadata directory: {metadata_dir}")
    print(f"rtrans directory:   {rtrans_dir}")
    print(f"Output file:        {output_path}")
    print()

    start_time = time.time()

    try:
        # Step 1: Concatenate metadata files
        metadata_df = concatenate_metadata_files(metadata_dir)

        # Step 2: Load rtrans file
        rtrans_df = load_rtrans_file(rtrans_dir)

        # Step 3: Populate blank fields from rtrans
        populated_df = merge_dataframes(metadata_df, rtrans_df)

        # Step 4: Save populated file
        save_merged_file(populated_df, output_path)

        # Summary
        total_elapsed = time.time() - start_time
        print("\n" + "="*70)
        print("COMPLETED SUCCESSFULLY")
        print("="*70)
        print(f"Total time: {total_elapsed:.1f}s ({total_elapsed/60:.1f} minutes)")
        print(f"Output: {output_path}")

        return 0

    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
