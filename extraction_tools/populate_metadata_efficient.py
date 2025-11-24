#!/usr/bin/env python3
"""
Efficiently populate metadata fields from rtransparent full parquet outputs.

Strategy: First merge all rtrans files, then populate metadata files.
This is much faster than the iterative approach when dealing with 1,647+ files.

Usage:
    python populate_metadata_efficient.py \\
        --metadata-dir ~/pmcoaXMLs/extracted_metadata_parquet \\
        --rtrans-dir ~/pmcoaXMLs/rtrans_out_full_parquets \\
        --output-dir ~/pmcoaXMLs/populated_metadata_full
"""

import argparse
import pandas as pd
from pathlib import Path
from glob import glob
import sys
import time
from datetime import datetime
import gc


def detect_available_fields(rtrans_dir: Path, metadata_dir: Path) -> tuple:
    """
    Detect which sophisticated fields are available in rtrans files.
    Only returns fields that are either:
    1. Sophisticated analysis fields (not basic metadata like pmcid, doi, etc.)
    2. New fields not in metadata (like 'funder')

    Returns: (fields_to_populate, fields_skipped)
    """
    rtrans_files = sorted(glob(str(rtrans_dir / "*.parquet")))
    if not rtrans_files:
        return [], []

    # Read schema from first rtrans file
    first_file = rtrans_files[0]
    df_rtrans = pd.read_parquet(first_file)
    rtrans_fields = set(df_rtrans.columns) - {'pmid'}

    # Read schema from first metadata file
    metadata_files = sorted(glob(str(metadata_dir / "*.parquet")))
    if not metadata_files:
        return list(rtrans_fields), []

    df_metadata = pd.read_parquet(metadata_files[0])
    metadata_fields = set(df_metadata.columns)

    # Fields that are "copied" metadata - already populated, don't reload
    copied_fields = {
        'pmcid_pmc', 'pmcid_uid', 'doi', 'filename', 'journal', 'publisher',
        'affiliation_institution', 'affiliation_country', 'year_epub', 'year_ppub',
        'type', 'coi_text', 'fund_text', 'register_text',
        'fund_pmc_institute', 'fund_pmc_source', 'fund_pmc_anysource',
        'file_size', 'chars_in_body'
    }

    # Fields to populate:
    # 1. Sophisticated fields (in rtrans, in metadata, not in copied list)
    # 2. New fields (in rtrans, not in metadata)
    sophisticated_in_metadata = rtrans_fields & metadata_fields - copied_fields
    new_fields = rtrans_fields - metadata_fields

    fields_to_populate = list(sophisticated_in_metadata | new_fields)
    fields_skipped = list((rtrans_fields & metadata_fields & copied_fields))

    return fields_to_populate, fields_skipped


def merge_rtrans_files(rtrans_dir: Path, fields: list, batch_size: int = 100) -> pd.DataFrame:
    """
    Merge all rtrans parquet files into a single DataFrame using batched concatenation.
    Only load specified fields plus pmid to reduce memory.

    Uses batched approach to avoid OOM errors:
    - Read and concatenate in batches
    - Deduplicate each batch
    - Merge batches at the end
    """
    print("\n" + "="*70)
    print("PHASE 1: Merging rtrans parquet files (batched)")
    print("="*70)

    rtrans_files = sorted(glob(str(rtrans_dir / "*.parquet")))
    print(f"Found {len(rtrans_files)} rtrans files")
    print(f"Batch size: {batch_size} files")

    # Fields to load (only what we need)
    load_fields = ['pmid'] + fields
    print(f"Loading {len(fields)} fields from rtrans files")

    batches = []
    current_batch = []
    start = time.time()

    for i, file in enumerate(rtrans_files, 1):
        if i % 50 == 0 or i == 1 or i == len(rtrans_files):
            elapsed = time.time() - start
            rate = i / elapsed if elapsed > 0 else 0
            remaining = (len(rtrans_files) - i) / rate if rate > 0 else 0
            print(f"  [{i:4d}/{len(rtrans_files)}] {rate:.1f} files/sec, "
                  f"ETA: {remaining/60:.1f} min, {len(batches)} batches ready", end='\r')

        try:
            df = pd.read_parquet(file, columns=load_fields)
            df['pmid'] = df['pmid'].astype(str)
            current_batch.append(df)
        except Exception as e:
            print(f"\n  Warning: Failed to read {file}: {e}")
            continue

        # Process batch when it reaches batch_size
        if len(current_batch) >= batch_size:
            batch_df = pd.concat(current_batch, ignore_index=True)
            batch_df = batch_df.drop_duplicates(subset=['pmid'], keep='last')
            batches.append(batch_df)
            current_batch = []
            gc.collect()  # Free memory

    # Process remaining files
    if current_batch:
        batch_df = pd.concat(current_batch, ignore_index=True)
        batch_df = batch_df.drop_duplicates(subset=['pmid'], keep='last')
        batches.append(batch_df)
        current_batch = []

    print()  # New line after progress

    # Merge all batches
    print(f"\n  Concatenating {len(batches)} batches...")
    merged = pd.concat(batches, ignore_index=True)
    del batches
    gc.collect()

    # Final deduplication
    print(f"  Final deduplication by pmid...")
    initial_rows = len(merged)
    merged = merged.drop_duplicates(subset=['pmid'], keep='last')
    final_rows = len(merged)

    print(f"  Merged: {final_rows:,} unique records ({initial_rows - final_rows:,} duplicates removed)")

    elapsed = time.time() - start
    print(f"  Time: {elapsed/60:.1f} minutes")

    return merged


def populate_metadata_file(metadata_path: Path, rtrans_merged: pd.DataFrame,
                           output_dir: Path, fields: list) -> dict:
    """
    Populate a single metadata file using the merged rtrans data.
    """
    filename = metadata_path.name
    print(f"\n{'='*70}")
    print(f"Processing: {filename}")
    print(f"{'='*70}")

    start = time.time()

    # Load metadata
    print(f"  Loading metadata...", end=" ")
    metadata_df = pd.read_parquet(metadata_path)
    metadata_df['pmid'] = metadata_df['pmid'].astype(str)
    rows = len(metadata_df)
    print(f"{rows:,} rows")

    # Identify new vs existing fields
    new_fields = [f for f in fields if f not in metadata_df.columns]
    existing_fields = [f for f in fields if f in metadata_df.columns]

    # Initialize new fields
    for field in new_fields:
        metadata_df[field] = None

    # Count blank before
    blank_before = {}
    for field in existing_fields:
        blank_count = ((metadata_df[field].isna()) | (metadata_df[field] == '')).sum()
        blank_before[field] = blank_count
        print(f"    {field}: {blank_count:,} blank ({100*blank_count/rows:.1f}%)")

    for field in new_fields:
        blank_before[field] = rows
        print(f"    {field}: NEW (will be created)")

    # Perform single merge with rtrans data
    print(f"\n  Merging with rtrans data...")
    merged = pd.merge(
        metadata_df,
        rtrans_merged,
        on='pmid',
        how='left',
        suffixes=('', '_rtrans')
    )

    # Populate fields
    populated_count = 0
    for field in fields:
        rtrans_col = f"{field}_rtrans"

        if rtrans_col in merged.columns:
            # Check data type
            sample_value = merged[rtrans_col].dropna().iloc[0] if len(merged[rtrans_col].dropna()) > 0 else None
            is_array_type = hasattr(sample_value, '__len__') and not isinstance(sample_value, str)

            if is_array_type:
                has_rtrans_value = merged[rtrans_col].notna() & (merged[rtrans_col].apply(lambda x: len(x) > 0 if hasattr(x, '__len__') else False))
            else:
                has_rtrans_value = merged[rtrans_col].notna() & (merged[rtrans_col] != '')

            # Determine which rows to populate
            if field in new_fields:
                is_blank = metadata_df[field].isna()
            else:
                is_blank = (metadata_df[field].isna()) | (metadata_df[field] == '')

            to_populate = is_blank & has_rtrans_value
            count = to_populate.sum()

            if count > 0:
                metadata_df.loc[to_populate, field] = merged.loc[to_populate, rtrans_col]
                populated_count += count

    # Count blank after
    print(f"\n  Results:")
    blank_after = {}
    for field in fields:
        sample_value = metadata_df[field].dropna().iloc[0] if len(metadata_df[field].dropna()) > 0 else None
        is_array_type = hasattr(sample_value, '__len__') and not isinstance(sample_value, str)

        if is_array_type:
            blank_count = (metadata_df[field].isna() | metadata_df[field].apply(lambda x: len(x) == 0 if hasattr(x, '__len__') else True)).sum()
        else:
            blank_count = ((metadata_df[field].isna()) | (metadata_df[field] == '')).sum()

        blank_after[field] = blank_count
        populated = blank_before[field] - blank_count
        print(f"    {field}: {populated:,} populated ({100*populated/blank_before[field]:.1f}% of blanks)")

    # Save output
    output_path = output_dir / filename
    print(f"\n  Saving to: {output_path.name}...", end=" ")
    metadata_df.to_parquet(output_path, index=False)
    print("✓")

    elapsed = time.time() - start
    print(f"  Time: {elapsed:.1f} seconds")

    return {
        'filename': filename,
        'rows': rows,
        'populated': populated_count,
        'time_seconds': elapsed
    }


def main():
    parser = argparse.ArgumentParser(
        description='Efficiently populate metadata fields from rtrans full parquets.',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument('--metadata-dir', type=str, required=True,
                       help='Directory containing metadata parquet files')
    parser.add_argument('--rtrans-dir', type=str, required=True,
                       help='Directory containing rtrans full parquet files')
    parser.add_argument('--output-dir', type=str, required=True,
                       help='Output directory for populated metadata files')
    parser.add_argument('--fields', type=str, default='auto',
                       help='Comma-separated list of fields to populate, or "auto" to populate all available sophisticated fields (default: auto)')

    args = parser.parse_args()

    metadata_dir = Path(args.metadata_dir).expanduser()
    rtrans_dir = Path(args.rtrans_dir).expanduser()
    output_dir = Path(args.output_dir).expanduser()

    # Validate
    if not metadata_dir.exists():
        print(f"Error: Metadata directory does not exist: {metadata_dir}", file=sys.stderr)
        return 1

    if not rtrans_dir.exists():
        print(f"Error: rtrans directory does not exist: {rtrans_dir}", file=sys.stderr)
        return 1

    output_dir.mkdir(parents=True, exist_ok=True)

    print("="*70)
    print("EFFICIENT METADATA POPULATOR")
    print("="*70)
    print(f"Metadata directory: {metadata_dir}")
    print(f"rtrans directory:   {rtrans_dir}")
    print(f"Output directory:   {output_dir}")

    # Determine fields to populate
    if args.fields.lower() == 'auto':
        print("\nDetecting available fields from rtrans files...")
        fields, skipped = detect_available_fields(rtrans_dir, metadata_dir)

        print(f"  Found {len(fields)} sophisticated fields to populate")
        print(f"  Skipped {len(skipped)} copied metadata fields (already in metadata)")

        if len(fields) == 0:
            print("\nError: No fields to populate!", file=sys.stderr)
            return 1
    else:
        fields = [f.strip() for f in args.fields.split(',')]

    print(f"\nFields to populate ({len(fields)} total):")
    for i, field in enumerate(fields, 1):
        print(f"  {i:3d}. {field}")
        if i >= 20 and len(fields) > 20:
            print(f"  ... and {len(fields) - 20} more")
            break

    overall_start = time.time()

    # Phase 1: Merge all rtrans files
    rtrans_merged = merge_rtrans_files(rtrans_dir, fields)
    gc.collect()  # Free memory

    # Phase 2: Process metadata files
    print("\n" + "="*70)
    print("PHASE 2: Populating metadata files")
    print("="*70)

    metadata_files = sorted(glob(str(metadata_dir / "*.parquet")))
    print(f"Found {len(metadata_files)} metadata files\n")

    results = []
    for i, metadata_path in enumerate(metadata_files, 1):
        print(f"\n[{i}/{len(metadata_files)}]")
        result = populate_metadata_file(Path(metadata_path), rtrans_merged, output_dir, fields)
        results.append(result)

    # Summary
    total_elapsed = time.time() - overall_start
    total_rows = sum(r['rows'] for r in results)
    total_populated = sum(r['populated'] for r in results)

    print("\n" + "="*70)
    print("COMPLETION SUMMARY")
    print("="*70)
    print(f"Files processed:    {len(results)}")
    print(f"Total rows:         {total_rows:,}")
    print(f"Fields populated:   {total_populated:,}")
    print(f"Total time:         {total_elapsed/60:.1f} minutes")
    print(f"Average per file:   {total_elapsed/len(results):.1f} seconds")
    print(f"\nOutput directory:   {output_dir}")
    print("="*70)

    return 0


if __name__ == '__main__':
    sys.exit(main())
