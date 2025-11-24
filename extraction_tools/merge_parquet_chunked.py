#!/usr/bin/env python3
"""
Memory-Efficient Parquet File Merger

Processes metadata files one at a time to avoid loading all 6.47M rows into memory at once.
"""

import argparse
import pandas as pd
from pathlib import Path
from glob import glob
import sys
import time
from datetime import datetime


def load_rtrans_file(rtrans_dir: Path) -> pd.DataFrame:
    """Load the rtrans parquet file."""
    print("="*70)
    print("STEP 1: Loading rtrans_out parquet file")
    print("="*70)

    pattern = str(rtrans_dir / "*.parquet")
    files = glob(pattern)

    if not files:
        raise FileNotFoundError(f"No parquet files found in {rtrans_dir}")

    if len(files) > 1:
        print(f"Warning: Multiple parquet files found. Using the first one.")

    file_path = files[0]
    filename = Path(file_path).name

    print(f"Reading {filename}...")
    start = time.time()
    df = pd.read_parquet(file_path)

    # Convert pmid to string for joining
    df['pmid'] = df['pmid'].astype(str)

    elapsed = time.time() - start

    print(f"✓ Loaded {len(df):,} rows in {elapsed:.1f}s")
    print(f"  Columns: {list(df.columns)}")
    print(f"  Memory: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")

    return df


def populate_metadata_file(metadata_path: Path, rtrans_df: pd.DataFrame,
                           output_path: Path, is_first: bool) -> tuple:
    """Process a single metadata file and append to output."""

    filename = metadata_path.name
    print(f"\n{'='*70}")
    print(f"Processing: {filename}")
    print(f"{'='*70}")

    start = time.time()

    # Load metadata file
    print(f"  Loading...", end=" ")
    metadata_df = pd.read_parquet(metadata_path)
    rows = len(metadata_df)
    print(f"{rows:,} rows")

    # Convert pmid to string
    metadata_df['pmid'] = metadata_df['pmid'].astype(str)

    # Find overlapping columns
    common_cols = list(set(metadata_df.columns) & set(rtrans_df.columns) - {'pmid'})

    if common_cols:
        # Count blanks before
        blank_before = {}
        for col in common_cols:
            blank_before[col] = ((metadata_df[col].isna()) | (metadata_df[col] == '')).sum()

        # Perform left join
        print(f"  Joining on pmid...")
        rtrans_subset = rtrans_df[['pmid'] + common_cols].copy()

        merged = pd.merge(
            metadata_df,
            rtrans_subset,
            on='pmid',
            how='left',
            suffixes=('', '_rtrans')
        )

        # Populate blank fields
        print(f"  Populating fields:")
        populated_total = 0

        for col in common_cols:
            rtrans_col = f"{col}_rtrans"

            if rtrans_col in merged.columns:
                is_blank = (merged[col].isna()) | (merged[col] == '')
                has_rtrans_value = merged[rtrans_col].notna() & (merged[rtrans_col] != '')
                to_populate = is_blank & has_rtrans_value

                populated_count = to_populate.sum()

                if populated_count > 0:
                    merged.loc[to_populate, col] = merged.loc[to_populate, rtrans_col]
                    populated_total += populated_count
                    print(f"    {col}: {populated_count:,} values")

                # Drop temporary column
                merged.drop(columns=[rtrans_col], inplace=True)

        metadata_df = merged

        # Match statistics
        pmid_matches = metadata_df['pmid'].isin(rtrans_df['pmid']).sum()
        print(f"  PMID matches: {pmid_matches:,}/{rows:,} ({100*pmid_matches/rows:.1f}%)")
        print(f"  Total populated: {populated_total:,} values")

    # Append to output file
    print(f"  Writing to output...", end=" ")
    write_start = time.time()

    if is_first:
        # First file - create new parquet
        metadata_df.to_parquet(output_path, index=False)
    else:
        # Append to existing file
        existing_df = pd.read_parquet(output_path)
        combined_df = pd.concat([existing_df, metadata_df], ignore_index=True)
        combined_df.to_parquet(output_path, index=False)

    write_elapsed = time.time() - write_start
    print(f"{write_elapsed:.1f}s")

    elapsed = time.time() - start
    print(f"  ✓ Completed in {elapsed:.1f}s")

    return rows, populated_total if common_cols else 0


def main():
    parser = argparse.ArgumentParser(
        description='Memory-efficient metadata populator (processes files one at a time).',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s
  %(prog)s -o populated_metadata.parquet
        """
    )

    parser.add_argument(
        '--metadata-dir',
        type=str,
        default='~/pmcoaXMLs/extracted_metadata_parquet',
        help='Directory containing metadata parquet files'
    )

    parser.add_argument(
        '--rtrans-dir',
        type=str,
        default='~/pmcoaXMLs/rtrans_out',
        help='Directory containing rtrans parquet file'
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
    print("MEMORY-EFFICIENT PARQUET POPULATOR")
    print("="*70)
    print(f"Metadata directory: {metadata_dir}")
    print(f"rtrans directory:   {rtrans_dir}")
    print(f"Output file:        {output_path}")
    print()

    start_time = time.time()

    try:
        # Step 1: Load rtrans file (keep in memory)
        rtrans_df = load_rtrans_file(rtrans_dir)

        # Step 2: Find all metadata files
        print(f"\n{'='*70}")
        print("STEP 2: Finding metadata files")
        print(f"{'='*70}")

        pattern = str(metadata_dir / "*.parquet")
        files = sorted(glob(pattern))

        if not files:
            raise FileNotFoundError(f"No parquet files found in {metadata_dir}")

        print(f"Found {len(files)} parquet files")

        # Step 3: Process each file
        print(f"\n{'='*70}")
        print("STEP 3: Processing metadata files")
        print(f"{'='*70}")

        total_rows = 0
        total_populated = 0

        for i, file_path in enumerate(files):
            rows, populated = populate_metadata_file(
                Path(file_path),
                rtrans_df,
                output_path,
                is_first=(i == 0)
            )
            total_rows += rows
            total_populated += populated

            print(f"  Progress: {i+1}/{len(files)} files ({100*(i+1)/len(files):.1f}%)")
            print(f"  Cumulative: {total_rows:,} rows, {total_populated:,} values populated")

        # Summary
        total_elapsed = time.time() - start_time
        file_size_mb = output_path.stat().st_size / 1024**2

        print(f"\n{'='*70}")
        print("COMPLETED SUCCESSFULLY")
        print(f"{'='*70}")
        print(f"Total files processed: {len(files)}")
        print(f"Total rows: {total_rows:,}")
        print(f"Total values populated: {total_populated:,}")
        print(f"Processing time: {total_elapsed:.1f}s ({total_elapsed/60:.1f} minutes)")
        print(f"Output file: {output_path}")
        print(f"Output size: {file_size_mb:.1f} MB")

        return 0

    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
