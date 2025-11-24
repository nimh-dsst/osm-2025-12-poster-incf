#!/usr/bin/env python3
"""
Stream-based metadata population - NO large dataframe creation.

Memory usage: 1 metadata file + 1 rtrans file at a time
Perfect for systems with limited RAM.

Strategy:
1. For each metadata file:
   - Load metadata (small, ~200K rows)
   - Stream through rtrans files one at a time
   - Look up matching PMIDs and populate fields
   - Save result
2. Total memory: ~500MB max instead of 6GB+
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


def populate_metadata_streaming(metadata_path: Path, rtrans_files: list,
                                output_dir: Path, fields: list) -> dict:
    """
    Populate a metadata file by streaming through rtrans files.
    Memory efficient: only holds 1 metadata + 1 rtrans file at a time.
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

    # Track which PMIDs still need values for each field
    needs_value = {}
    for field in fields:
        if field in existing_fields:
            # Only populate if blank
            is_blank = (metadata_df[field].isna()) | (metadata_df[field] == '')
            needs_value[field] = set(metadata_df.loc[is_blank, 'pmid'])
        else:
            # New field - all rows need values
            needs_value[field] = set(metadata_df['pmid'])

    initial_needs = {field: len(pmids) for field, pmids in needs_value.items()}

    print(f"\n  Initial blank counts:")
    for field in fields[:10]:  # Show first 10
        print(f"    {field}: {initial_needs[field]:,}")
    if len(fields) > 10:
        print(f"    ... and {len(fields) - 10} more fields")

    # Stream through rtrans files
    print(f"\n  Streaming through {len(rtrans_files)} rtrans files...")
    load_fields = ['pmid'] + fields
    populated_count = 0

    for i, rtrans_file in enumerate(rtrans_files, 1):
        if i % 100 == 0 or i == 1 or i == len(rtrans_files):
            total_remaining = sum(len(pmids) for pmids in needs_value.values())
            print(f"    [{i:4d}/{len(rtrans_files)}] {total_remaining:,} values still needed", end='\r')

        # Check if we're done
        if all(len(pmids) == 0 for pmids in needs_value.values()):
            print(f"\n    All values found after {i} files!")
            break

        try:
            # Load rtrans file
            rtrans_chunk = pd.read_parquet(rtrans_file, columns=load_fields)
            rtrans_chunk['pmid'] = rtrans_chunk['pmid'].astype(str)

            # Find matching PMIDs
            matching_pmids = set(rtrans_chunk['pmid']) & set(metadata_df['pmid'])
            if not matching_pmids:
                continue

            # Populate each field
            for field in fields:
                if field not in rtrans_chunk.columns:
                    continue

                # Find PMIDs that need this field and are in this chunk
                to_populate_pmids = needs_value[field] & matching_pmids
                if not to_populate_pmids:
                    continue

                # Get values from rtrans
                rtrans_matches = rtrans_chunk[rtrans_chunk['pmid'].isin(to_populate_pmids)]

                # Update metadata
                for _, row in rtrans_matches.iterrows():
                    pmid = row['pmid']
                    value = row[field]

                    # Check if value is meaningful
                    is_meaningful = False
                    if hasattr(value, '__len__') and not isinstance(value, str):
                        # Array type
                        is_meaningful = len(value) > 0
                    else:
                        # Scalar type
                        is_meaningful = pd.notna(value) and value != ''

                    if is_meaningful:
                        metadata_df.loc[metadata_df['pmid'] == pmid, field] = value
                        needs_value[field].discard(pmid)
                        populated_count += 1

        except Exception as e:
            print(f"\n    Warning: Failed to process {rtrans_file}: {e}")
            continue

    print()  # New line after progress

    # Final counts
    print(f"\n  Results:")
    for field in fields[:10]:
        remaining = len(needs_value[field])
        populated = initial_needs[field] - remaining
        if initial_needs[field] > 0:
            pct = 100 * populated / initial_needs[field]
            print(f"    {field}: {populated:,} populated ({pct:.1f}%)")
    if len(fields) > 10:
        total_initial = sum(initial_needs.values())
        total_remaining = sum(len(pmids) for pmids in needs_value.values())
        total_populated = total_initial - total_remaining
        print(f"    ... Total: {total_populated:,}/{total_initial:,} populated ({100*total_populated/total_initial:.1f}%)")

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
        description='Stream-based metadata population (memory efficient)',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument('--metadata-dir', type=str, required=True,
                       help='Directory containing metadata parquet files')
    parser.add_argument('--rtrans-dir', type=str, required=True,
                       help='Directory containing rtrans full parquet files')
    parser.add_argument('--output-dir', type=str, required=True,
                       help='Output directory for populated metadata files')
    parser.add_argument('--fields', type=str, default='auto',
                       help='Comma-separated list of fields to populate, or "auto" (default)')

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
    print("STREAMING METADATA POPULATOR (Memory Efficient)")
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

    # Get rtrans files
    rtrans_files = sorted(glob(str(rtrans_dir / "*.parquet")))
    print(f"\nFound {len(rtrans_files)} rtrans files")

    # Get metadata files
    metadata_files = sorted(glob(str(metadata_dir / "*.parquet")))
    print(f"Found {len(metadata_files)} metadata files")

    # Process each metadata file
    results = []
    for i, metadata_path in enumerate(metadata_files, 1):
        print(f"\n[{i}/{len(metadata_files)}]")
        result = populate_metadata_streaming(
            Path(metadata_path), rtrans_files, output_dir, fields
        )
        results.append(result)
        gc.collect()

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
