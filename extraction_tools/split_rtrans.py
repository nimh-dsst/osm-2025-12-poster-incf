#!/usr/bin/env python3
"""
Split rtrans_out file into smaller chunks for memory-efficient processing.

Extracts only pmid, is_open_code, is_open_data columns and splits into manageable chunks.
"""

import argparse
import pandas as pd
from pathlib import Path
import sys
import time


def main():
    parser = argparse.ArgumentParser(
        description='Split rtrans parquet file into smaller chunks.',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        '--input',
        type=str,
        required=True,
        help='Input rtrans parquet file'
    )

    parser.add_argument(
        '--output-dir',
        type=str,
        required=True,
        help='Output directory for chunk files'
    )

    parser.add_argument(
        '--chunk-size',
        type=int,
        default=200000,
        help='Number of rows per chunk (default: 200000)'
    )

    args = parser.parse_args()

    input_path = Path(args.input).expanduser()
    output_dir = Path(args.output_dir).expanduser()
    chunk_size = args.chunk_size

    if not input_path.exists():
        print(f"Error: Input file does not exist: {input_path}", file=sys.stderr)
        return 1

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    print("="*70)
    print("RTRANS FILE SPLITTER")
    print("="*70)
    print(f"Input file:   {input_path}")
    print(f"Output dir:   {output_dir}")
    print(f"Chunk size:   {chunk_size:,} rows")
    print()

    start_time = time.time()

    # Load rtrans file
    print("Loading rtrans file...")
    load_start = time.time()
    df = pd.read_parquet(input_path)
    load_elapsed = time.time() - load_start

    print(f"✓ Loaded {len(df):,} rows in {load_elapsed:.1f}s")
    print(f"  Columns: {list(df.columns)}")
    print(f"  Memory: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")

    # Extract only necessary columns
    print("\nExtracting columns: pmid, is_open_code, is_open_data, funder")
    required_cols = ['pmid', 'is_open_code', 'is_open_data', 'funder']

    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"Error: Missing columns: {missing_cols}", file=sys.stderr)
        return 1

    df_subset = df[required_cols].copy()
    print(f"✓ Subset created")
    print(f"  Memory: {df_subset.memory_usage(deep=True).sum() / 1024**2:.1f} MB")

    # Convert pmid to string
    print("\nConverting pmid to string...")
    df_subset['pmid'] = df_subset['pmid'].astype(str)

    # Check if already sorted by pmid
    print("\nChecking sort order...")
    is_sorted = df_subset['pmid'].is_monotonic_increasing
    if is_sorted:
        print("✓ Data is already sorted by pmid")
    else:
        print("  Sorting by pmid...")
        df_subset = df_subset.sort_values('pmid').reset_index(drop=True)
        print("✓ Sorted")

    # Split into chunks
    total_rows = len(df_subset)
    num_chunks = (total_rows + chunk_size - 1) // chunk_size

    print(f"\nSplitting into {num_chunks} chunks...")
    print()

    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx = min(start_idx + chunk_size, total_rows)

        chunk = df_subset.iloc[start_idx:end_idx]
        chunk_num = i + 1

        output_file = output_dir / f"rtrans_chunk_{chunk_num:03d}.parquet"

        print(f"  Chunk {chunk_num}/{num_chunks}: rows {start_idx:,}-{end_idx-1:,} ({len(chunk):,} rows)", end=" ")

        chunk.to_parquet(output_file, index=False)

        file_size_mb = output_file.stat().st_size / 1024**2
        print(f"→ {output_file.name} ({file_size_mb:.1f} MB)")

    total_elapsed = time.time() - start_time

    print(f"\n{'='*70}")
    print("COMPLETED")
    print(f"{'='*70}")
    print(f"Input rows: {total_rows:,}")
    print(f"Chunks created: {num_chunks}")
    print(f"Rows per chunk: ~{chunk_size:,}")
    print(f"Output directory: {output_dir}")
    print(f"Total time: {total_elapsed:.1f}s")

    return 0


if __name__ == '__main__':
    sys.exit(main())
