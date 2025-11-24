#!/usr/bin/env python3
"""
Ultra Memory-Efficient Parquet File Merger

Processes files individually and creates intermediate outputs, then concatenates at the end.
This avoids loading the growing output file repeatedly.
"""

import argparse
import pandas as pd
from pathlib import Path
from glob import glob
import sys
import time
from datetime import datetime
import tempfile
import shutil


def load_rtrans_file(rtrans_dir: Path) -> pd.DataFrame:
    """Load the rtrans parquet file."""
    print("="*70)
    print("STEP 1: Loading rtrans_out parquet file")
    print("="*70)

    pattern = str(rtrans_dir / "*.parquet")
    files = glob(pattern)

    if not files:
        raise FileNotFoundError(f"No parquet files found in {rtrans_dir}")

    file_path = files[0]
    filename = Path(file_path).name

    print(f"Reading {filename}...")
    start = time.time()
    df = pd.read_parquet(file_path)

    # Convert pmid to string for joining
    print(f"Converting pmid to string...")
    df['pmid'] = df['pmid'].astype(str)

    elapsed = time.time() - start

    print(f"✓ Loaded {len(df):,} rows in {elapsed:.1f}s")
    print(f"  Memory: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")

    return df


def populate_metadata_file(metadata_path: Path, rtrans_df: pd.DataFrame,
                           output_path: Path) -> tuple:
    """Process a single metadata file and save independently."""

    filename = metadata_path.name
    print(f"  [{filename}]", end=" ")

    start = time.time()

    # Load metadata file
    metadata_df = pd.read_parquet(metadata_path)
    rows = len(metadata_df)

    # Convert pmid to string
    metadata_df['pmid'] = metadata_df['pmid'].astype(str)

    # Find overlapping columns
    common_cols = list(set(metadata_df.columns) & set(rtrans_df.columns) - {'pmid'})

    populated_total = 0

    if common_cols:
        # Perform left join with only necessary columns
        rtrans_subset = rtrans_df[['pmid'] + common_cols].copy()

        merged = pd.merge(
            metadata_df,
            rtrans_subset,
            on='pmid',
            how='left',
            suffixes=('', '_rtrans')
        )

        # Populate blank fields
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

                # Drop temporary column
                merged.drop(columns=[rtrans_col], inplace=True)

        metadata_df = merged

    # Save to individual output file
    metadata_df.to_parquet(output_path, index=False)

    elapsed = time.time() - start
    print(f"{rows:,} rows, {populated_total:,} populated ({elapsed:.1f}s)")

    return rows, populated_total


def concatenate_parquet_files(file_paths: list, output_path: Path):
    """Concatenate all parquet files efficiently."""
    print(f"\n{'='*70}")
    print("FINAL STEP: Concatenating all processed files")
    print(f"{'='*70}")

    print(f"Concatenating {len(file_paths)} files...")
    start = time.time()

    dfs = []
    for i, fp in enumerate(file_paths, 1):
        if i % 5 == 0:
            print(f"  Reading file {i}/{len(file_paths)}...")
        df = pd.read_parquet(fp)
        dfs.append(df)

    print(f"  Combining dataframes...")
    combined = pd.concat(dfs, ignore_index=True)

    print(f"  Writing final output...")
    combined.to_parquet(output_path, index=False)

    elapsed = time.time() - start
    print(f"✓ Concatenation completed in {elapsed:.1f}s ({elapsed/60:.1f} min)")

    return len(combined)


def main():
    parser = argparse.ArgumentParser(
        description='Ultra memory-efficient metadata populator.',
        formatter_class=argparse.RawDescriptionHelpFormatter
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
        help='Output file path'
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
    print("ULTRA MEMORY-EFFICIENT PARQUET POPULATOR")
    print("="*70)
    print(f"Metadata directory: {metadata_dir}")
    print(f"rtrans directory:   {rtrans_dir}")
    print(f"Output file:        {output_path}")
    print()

    start_time = time.time()

    # Create temporary directory for intermediate files
    temp_dir = Path(tempfile.mkdtemp(prefix="parquet_merge_"))
    print(f"Temporary directory: {temp_dir}\n")

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

        print(f"Found {len(files)} parquet files\n")

        # Step 3: Process each file to temp directory
        print(f"{'='*70}")
        print("STEP 3: Processing metadata files")
        print(f"{'='*70}")

        total_rows = 0
        total_populated = 0
        output_files = []

        for i, file_path in enumerate(files, 1):
            temp_output = temp_dir / f"processed_{i:03d}.parquet"

            rows, populated = populate_metadata_file(
                Path(file_path),
                rtrans_df,
                temp_output
            )

            output_files.append(temp_output)
            total_rows += rows
            total_populated += populated

            if i % 5 == 0:
                print(f"  Progress: {i}/{len(files)} files ({100*i/len(files):.1f}%)")

        print(f"\n✓ All files processed")
        print(f"  Total rows: {total_rows:,}")
        print(f"  Total populated: {total_populated:,}")

        # Step 4: Concatenate all processed files
        final_rows = concatenate_parquet_files(output_files, output_path)

        # Cleanup temp directory
        print(f"\nCleaning up temporary files...")
        shutil.rmtree(temp_dir)

        # Summary
        total_elapsed = time.time() - start_time
        file_size_mb = output_path.stat().st_size / 1024**2

        print(f"\n{'='*70}")
        print("COMPLETED SUCCESSFULLY")
        print(f"{'='*70}")
        print(f"Files processed: {len(files)}")
        print(f"Final rows: {final_rows:,}")
        print(f"Values populated: {total_populated:,}")
        print(f"Total time: {total_elapsed:.1f}s ({total_elapsed/60:.1f} minutes)")
        print(f"Output file: {output_path}")
        print(f"Output size: {file_size_mb:.1f} MB")

        return 0

    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()

        # Cleanup on error
        if temp_dir.exists():
            print(f"Cleaning up temporary directory...")
            shutil.rmtree(temp_dir)

        return 1


if __name__ == '__main__':
    sys.exit(main())
