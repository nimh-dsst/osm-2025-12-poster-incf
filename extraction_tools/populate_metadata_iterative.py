#!/usr/bin/env python3
"""
Iteratively populate fields in metadata files using rtransparent output parquet files.

Default fields: is_open_code, is_open_data, funder
- For existing fields (is_open_code, is_open_data): only populate if blank
- For new fields (funder): create column and populate all matches

Processes each metadata file against all rtrans parquet files to find matches and populate fields.

Updated 2025-11-24: Now uses rtrans_out_full_parquets (1,647 files, complete 120+ column output)
instead of rtrans_out_chunks (21 files, incomplete 9-column subset).
"""

import argparse
import pandas as pd
from pathlib import Path
from glob import glob
import sys
import time
from datetime import datetime


def populate_from_chunks(metadata_df: pd.DataFrame, chunk_files: list, fields: list) -> tuple:
    """
    Populate specified fields in metadata from rtrans chunks.

    For existing fields: only populate if blank
    For new fields: create column and populate all matches

    Returns: (populated_count, matched_rows)
    """
    # Convert pmid to string
    metadata_df['pmid'] = metadata_df['pmid'].astype(str)

    total_populated = 0
    matched_rows = set()

    # Identify which fields are new (don't exist in metadata)
    new_fields = [f for f in fields if f not in metadata_df.columns]
    existing_fields = [f for f in fields if f in metadata_df.columns]

    # Initialize new fields as None/empty
    for field in new_fields:
        metadata_df[field] = None

    for chunk_file in chunk_files:
        # Load rtrans chunk
        rtrans_chunk = pd.read_parquet(chunk_file)
        rtrans_chunk['pmid'] = rtrans_chunk['pmid'].astype(str)

        # Only join fields that exist in the chunk
        available_fields = [f for f in fields if f in rtrans_chunk.columns]
        if not available_fields:
            continue

        # Perform left join
        merged = pd.merge(
            metadata_df,
            rtrans_chunk[['pmid'] + available_fields],
            on='pmid',
            how='left',
            suffixes=('', '_rtrans')
        )

        # Populate fields
        for field in available_fields:
            rtrans_col = f"{field}_rtrans"

            if rtrans_col in merged.columns:
                # Check if column contains array/list type data
                sample_value = merged[rtrans_col].dropna().iloc[0] if len(merged[rtrans_col].dropna()) > 0 else None
                is_array_type = hasattr(sample_value, '__len__') and not isinstance(sample_value, str)

                if is_array_type:
                    # For array/list columns: check if array has elements
                    has_rtrans_value = merged[rtrans_col].notna() & (merged[rtrans_col].apply(lambda x: len(x) > 0 if hasattr(x, '__len__') else False))
                else:
                    # For scalar columns: check if not null and not empty string
                    has_rtrans_value = merged[rtrans_col].notna() & (merged[rtrans_col] != '')

                if field in new_fields:
                    # For new fields: populate all rows that have rtrans value and don't already have a value
                    is_blank = metadata_df[field].isna()
                    to_populate = is_blank & has_rtrans_value
                else:
                    # For existing fields: only populate if blank
                    is_blank = (metadata_df[field].isna()) | (metadata_df[field] == '')
                    to_populate = is_blank & has_rtrans_value

                populated_count = to_populate.sum()

                if populated_count > 0:
                    # Update original dataframe
                    metadata_df.loc[to_populate, field] = merged.loc[to_populate, rtrans_col]
                    total_populated += populated_count

        # Track matched rows
        matches = metadata_df['pmid'].isin(rtrans_chunk['pmid'])
        matched_rows.update(metadata_df[matches]['pmid'].tolist())

    return total_populated, len(matched_rows)


def process_metadata_file(metadata_path: Path, chunk_files: list,
                          output_dir: Path, fields: list) -> dict:
    """Process a single metadata file."""

    filename = metadata_path.name
    print(f"\n{'='*70}")
    print(f"Processing: {filename}")
    print(f"{'='*70}")

    start = time.time()

    # Load metadata
    print(f"  Loading metadata...", end=" ")
    metadata_df = pd.read_parquet(metadata_path)
    rows = len(metadata_df)
    print(f"{rows:,} rows")

    # Identify new vs existing fields
    new_fields = [f for f in fields if f not in metadata_df.columns]
    existing_fields = [f for f in fields if f in metadata_df.columns]

    # Count blank fields before (only for existing fields)
    blank_before = {}
    for field in existing_fields:
        blank_count = ((metadata_df[field].isna()) | (metadata_df[field] == '')).sum()
        blank_before[field] = blank_count
        print(f"    {field}: {blank_count:,} blank ({100*blank_count/rows:.1f}%)")

    for field in new_fields:
        blank_before[field] = rows  # All blank since it doesn't exist
        print(f"    {field}: NEW (will be created)")

    # Populate from chunks
    print(f"\n  Processing {len(chunk_files)} rtrans chunks...")
    populated_count, matched_rows = populate_from_chunks(metadata_df, chunk_files, fields)

    # Count blank fields after
    print(f"\n  Results:")
    blank_after = {}
    for field in fields:
        # Check if field contains array/list type data
        sample_value = metadata_df[field].dropna().iloc[0] if len(metadata_df[field].dropna()) > 0 else None
        is_array_type = hasattr(sample_value, '__len__') and not isinstance(sample_value, str)

        if is_array_type:
            # For array columns: count as blank if null or empty array
            blank_count = (metadata_df[field].isna() | metadata_df[field].apply(lambda x: len(x) == 0 if hasattr(x, '__len__') else True)).sum()
        else:
            # For scalar columns: count as blank if null or empty string
            blank_count = ((metadata_df[field].isna()) | (metadata_df[field] == '')).sum()

        blank_after[field] = blank_count
        populated = blank_before[field] - blank_count
        print(f"    {field}: {blank_count:,} blank (populated {populated:,})")

    print(f"  Matched rows: {matched_rows:,}/{rows:,} ({100*matched_rows/rows:.1f}%)")
    print(f"  Total values populated: {populated_count:,}")

    # Save to output directory
    output_path = output_dir / filename
    print(f"\n  Saving to {output_path.name}...", end=" ")
    metadata_df.to_parquet(output_path, index=False)

    file_size_mb = output_path.stat().st_size / 1024**2
    print(f"{file_size_mb:.1f} MB")

    elapsed = time.time() - start
    print(f"  ✓ Completed in {elapsed:.1f}s")

    return {
        'filename': filename,
        'rows': rows,
        'matched_rows': matched_rows,
        'populated_count': populated_count,
        'blank_before': blank_before,
        'blank_after': blank_after,
        'elapsed': elapsed
    }


def main():
    parser = argparse.ArgumentParser(
        description='Iteratively populate metadata fields from rtrans chunks.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use full rtrans parquets (recommended - complete 120+ column dataset)
  %(prog)s --metadata-dir ~/pmcoaXMLs/extracted_metadata_parquet --rtrans-dir ~/pmcoaXMLs/rtrans_out_full_parquets --output-dir ~/pmcoaXMLs/populated_metadata

  # Legacy: Use incomplete chunks (9 columns only, 4M records)
  %(prog)s --metadata-dir ~/pmcoaXMLs/extracted_metadata_parquet --rtrans-dir ~/pmcoaXMLs/rtrans_out_chunks --output-dir ~/pmcoaXMLs/populated_metadata
        """
    )

    parser.add_argument(
        '--metadata-dir',
        type=str,
        required=True,
        help='Directory containing metadata parquet files'
    )

    parser.add_argument(
        '--rtrans-dir',
        type=str,
        required=True,
        help='Directory containing rtrans parquet files (use rtrans_out_full_parquets for complete data)'
    )

    parser.add_argument(
        '--output-dir',
        type=str,
        required=True,
        help='Output directory for populated metadata files'
    )

    parser.add_argument(
        '--fields',
        type=str,
        default='is_open_code,is_open_data,funder',
        help='Comma-separated list of fields to populate (default: is_open_code,is_open_data,funder)'
    )

    args = parser.parse_args()

    # Expand paths
    metadata_dir = Path(args.metadata_dir).expanduser()
    rtrans_dir = Path(args.rtrans_dir).expanduser()
    output_dir = Path(args.output_dir).expanduser()
    fields = [f.strip() for f in args.fields.split(',')]

    # Validate directories
    if not metadata_dir.exists():
        print(f"Error: Metadata directory does not exist: {metadata_dir}", file=sys.stderr)
        return 1

    if not rtrans_dir.exists():
        print(f"Error: rtrans directory does not exist: {rtrans_dir}", file=sys.stderr)
        return 1

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    print("="*70)
    print("ITERATIVE METADATA POPULATOR")
    print("="*70)
    print(f"Metadata directory: {metadata_dir}")
    print(f"rtrans directory:   {rtrans_dir}")
    print(f"Output directory:   {output_dir}")
    print(f"Fields to populate: {', '.join(fields)}")
    print()

    start_time = time.time()

    # Find metadata files
    metadata_pattern = str(metadata_dir / "*.parquet")
    metadata_files = sorted(glob(metadata_pattern))

    if not metadata_files:
        print(f"Error: No metadata files found in {metadata_dir}", file=sys.stderr)
        return 1

    print(f"Found {len(metadata_files)} metadata files")

    # Find rtrans parquet files
    # Support both full parquets (*.parquet) and legacy chunks (rtrans_chunk_*.parquet)
    chunk_pattern = str(rtrans_dir / "*.parquet")
    chunk_files = sorted(glob(chunk_pattern))

    if not chunk_files:
        print(f"Error: No rtrans parquet files found in {rtrans_dir}", file=sys.stderr)
        return 1

    print(f"Found {len(chunk_files)} rtrans parquet files")
    print()

    # Process each metadata file
    results = []

    for i, metadata_path in enumerate(metadata_files, 1):
        print(f"\n[{i}/{len(metadata_files)}]")

        result = process_metadata_file(
            Path(metadata_path),
            chunk_files,
            output_dir,
            fields
        )
        results.append(result)

        # Progress summary
        total_rows = sum(r['rows'] for r in results)
        total_populated = sum(r['populated_count'] for r in results)
        print(f"\n  Cumulative: {total_rows:,} rows, {total_populated:,} values populated")

    # Final summary
    total_elapsed = time.time() - start_time

    print(f"\n{'='*70}")
    print("FINAL SUMMARY")
    print(f"{'='*70}")

    total_files = len(results)
    total_rows = sum(r['rows'] for r in results)
    total_populated = sum(r['populated_count'] for r in results)

    print(f"Metadata files processed: {total_files}")
    print(f"Total rows: {total_rows:,}")
    print(f"Total values populated: {total_populated:,}")
    print(f"Total time: {total_elapsed:.1f}s ({total_elapsed/60:.1f} minutes)")
    print(f"Output directory: {output_dir}")

    # Per-field summary
    print(f"\nPer-field summary:")
    for field in fields:
        total_before = sum(r['blank_before'][field] for r in results)
        total_after = sum(r['blank_after'][field] for r in results)
        populated = total_before - total_after
        print(f"  {field}:")
        print(f"    Blank before: {total_before:,}")
        print(f"    Blank after:  {total_after:,}")
        print(f"    Populated:    {populated:,} ({100*populated/total_before:.1f}% of blanks)")

    return 0


if __name__ == '__main__':
    sys.exit(main())
