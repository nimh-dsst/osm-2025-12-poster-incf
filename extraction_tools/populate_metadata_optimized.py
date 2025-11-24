#!/usr/bin/env python3
"""
Optimized metadata population using PMCID range filtering.

Key insight: Both metadata and rtrans files are sorted by pmcid_pmc.
We can build a PMCID range index and only load relevant rtrans files.

Speed improvement: Check ~50-100 rtrans files instead of all 1,647!
"""

import argparse
import pandas as pd
from pathlib import Path
from glob import glob
import sys
import time
from datetime import datetime
import gc


def extract_pmcid_number(pmcid_str):
    """Extract numeric part from PMCID for comparison."""
    if pd.isna(pmcid_str) or pmcid_str == '':
        return -1
    # PMC12345 -> 12345
    try:
        return int(str(pmcid_str).replace('PMC', '').replace('pmc', ''))
    except:
        return -1


def build_rtrans_index(rtrans_dir: Path):
    """
    Build PMCID range index for all rtrans files.
    Returns: dict of {filename: (min_pmcid_num, max_pmcid_num)}
    """
    print("\nBuilding PMCID range index for rtrans files...")
    start = time.time()

    rtrans_files = sorted(glob(str(rtrans_dir / "*.parquet")))
    index = {}

    for i, file in enumerate(rtrans_files, 1):
        if i % 100 == 0 or i == len(rtrans_files):
            print(f"  Indexing [{i:4d}/{len(rtrans_files)}]", end='\r')

        try:
            # Read only pmcid_pmc column
            df = pd.read_parquet(file, columns=['pmcid_pmc'])
            pmcids = df['pmcid_pmc'].apply(extract_pmcid_number)
            min_pmcid = pmcids.min()
            max_pmcid = pmcids.max()
            index[file] = (min_pmcid, max_pmcid)
        except Exception as e:
            print(f"\n  Warning: Failed to index {file}: {e}")
            continue

    elapsed = time.time() - start
    print(f"\n  Indexed {len(index)} files in {elapsed:.1f} seconds")

    return index


def get_relevant_rtrans_files(metadata_pmcid_range, rtrans_index):
    """
    Get list of rtrans files whose PMCID ranges overlap with metadata range.

    Returns: list of rtrans file paths
    """
    meta_min, meta_max = metadata_pmcid_range
    relevant = []

    for file, (rtrans_min, rtrans_max) in rtrans_index.items():
        # Check if ranges overlap
        if rtrans_max < meta_min or rtrans_min > meta_max:
            # No overlap
            continue
        relevant.append(file)

    return relevant


def detect_available_fields(rtrans_dir: Path, metadata_dir: Path) -> tuple:
    """Detect sophisticated fields to populate."""
    rtrans_files = sorted(glob(str(rtrans_dir / "*.parquet")))
    if not rtrans_files:
        return [], []

    df_rtrans = pd.read_parquet(rtrans_files[0])
    rtrans_fields = set(df_rtrans.columns) - {'pmid'}

    metadata_files = sorted(glob(str(metadata_dir / "*.parquet")))
    if not metadata_files:
        return list(rtrans_fields), []

    df_metadata = pd.read_parquet(metadata_files[0])
    metadata_fields = set(df_metadata.columns)

    copied_fields = {
        'pmcid_pmc', 'pmcid_uid', 'doi', 'filename', 'journal', 'publisher',
        'affiliation_institution', 'affiliation_country', 'year_epub', 'year_ppub',
        'type', 'coi_text', 'fund_text', 'register_text',
        'fund_pmc_institute', 'fund_pmc_source', 'fund_pmc_anysource',
        'file_size', 'chars_in_body'
    }

    sophisticated_in_metadata = rtrans_fields & metadata_fields - copied_fields
    new_fields = rtrans_fields - metadata_fields

    fields_to_populate = list(sophisticated_in_metadata | new_fields)
    fields_skipped = list((rtrans_fields & metadata_fields & copied_fields))

    return fields_to_populate, fields_skipped


def populate_metadata_optimized(metadata_path: Path, rtrans_index: dict,
                                output_dir: Path, fields: list) -> dict:
    """
    Populate metadata using PMCID-based filtering.
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

    # Get PMCID range for this metadata file
    pmcid_nums = metadata_df['pmcid_pmc'].apply(extract_pmcid_number)
    meta_min = pmcid_nums.min()
    meta_max = pmcid_nums.max()
    print(f"  PMCID range: {meta_min:,} ... {meta_max:,}")

    # Find relevant rtrans files
    relevant_files = get_relevant_rtrans_files((meta_min, meta_max), rtrans_index)
    print(f"  Relevant rtrans files: {len(relevant_files)} of {len(rtrans_index)}")

    # Initialize new fields
    new_fields = [f for f in fields if f not in metadata_df.columns]
    for field in new_fields:
        metadata_df[field] = None

    # Track which PMIDs need values
    needs_value = {}
    for field in fields:
        if field in metadata_df.columns and field not in new_fields:
            is_blank = (metadata_df[field].isna()) | (metadata_df[field] == '')
            needs_value[field] = set(metadata_df.loc[is_blank, 'pmid'])
        else:
            needs_value[field] = set(metadata_df['pmid'])

    initial_needs = {field: len(pmids) for field, pmids in needs_value.items()}

    # Stream through relevant rtrans files only
    load_fields = ['pmid'] + fields
    populated_count = 0

    for i, rtrans_file in enumerate(relevant_files, 1):
        if i % 10 == 0 or i == 1 or i == len(relevant_files):
            total_remaining = sum(len(pmids) for pmids in needs_value.values())
            print(f"    [{i:4d}/{len(relevant_files)}] {total_remaining:,} values still needed", end='\r')

        # Check if we're done
        if all(len(pmids) == 0 for pmids in needs_value.values()):
            print(f"\n    All values found after {i} files!")
            break

        try:
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

                to_populate_pmids = needs_value[field] & matching_pmids
                if not to_populate_pmids:
                    continue

                rtrans_matches = rtrans_chunk[rtrans_chunk['pmid'].isin(to_populate_pmids)]

                for _, row in rtrans_matches.iterrows():
                    pmid = row['pmid']
                    value = row[field]

                    is_meaningful = False
                    if hasattr(value, '__len__') and not isinstance(value, str):
                        is_meaningful = len(value) > 0
                    else:
                        is_meaningful = pd.notna(value) and value != ''

                    if is_meaningful:
                        metadata_df.loc[metadata_df['pmid'] == pmid, field] = value
                        needs_value[field].discard(pmid)
                        populated_count += 1

        except Exception as e:
            print(f"\n    Warning: Failed to process {rtrans_file}: {e}")
            continue

    print()

    # Results
    print(f"\n  Populated {populated_count:,} field values")

    # Save output
    output_path = output_dir / filename
    print(f"  Saving to: {output_path.name}...", end=" ")
    metadata_df.to_parquet(output_path, index=False)
    print("✓")

    elapsed = time.time() - start
    print(f"  Time: {elapsed:.1f} seconds")

    return {
        'filename': filename,
        'rows': rows,
        'populated': populated_count,
        'relevant_files': len(relevant_files),
        'time_seconds': elapsed
    }


def main():
    parser = argparse.ArgumentParser(
        description='Optimized metadata population using PMCID range filtering'
    )

    parser.add_argument('--metadata-dir', type=str, required=True)
    parser.add_argument('--rtrans-dir', type=str, required=True)
    parser.add_argument('--output-dir', type=str, required=True)
    parser.add_argument('--fields', type=str, default='auto')

    args = parser.parse_args()

    metadata_dir = Path(args.metadata_dir).expanduser()
    rtrans_dir = Path(args.rtrans_dir).expanduser()
    output_dir = Path(args.output_dir).expanduser()

    if not metadata_dir.exists() or not rtrans_dir.exists():
        print(f"Error: Directory does not exist", file=sys.stderr)
        return 1

    output_dir.mkdir(parents=True, exist_ok=True)

    print("="*70)
    print("OPTIMIZED METADATA POPULATOR (PMCID Range Filtering)")
    print("="*70)
    print(f"Metadata directory: {metadata_dir}")
    print(f"rtrans directory:   {rtrans_dir}")
    print(f"Output directory:   {output_dir}")

    # Determine fields
    if args.fields.lower() == 'auto':
        print("\nDetecting available fields...")
        fields, skipped = detect_available_fields(rtrans_dir, metadata_dir)
        print(f"  Found {len(fields)} sophisticated fields to populate")
        print(f"  Skipped {len(skipped)} copied metadata fields")
    else:
        fields = [f.strip() for f in args.fields.split(',')]

    overall_start = time.time()

    # Build PMCID index
    rtrans_index = build_rtrans_index(rtrans_dir)

    # Process metadata files
    metadata_files = sorted(glob(str(metadata_dir / "*.parquet")))
    print(f"\nProcessing {len(metadata_files)} metadata files...")

    results = []
    for i, metadata_path in enumerate(metadata_files, 1):
        print(f"\n[{i}/{len(metadata_files)}]")
        result = populate_metadata_optimized(
            Path(metadata_path), rtrans_index, output_dir, fields
        )
        results.append(result)
        gc.collect()

    # Summary
    total_elapsed = time.time() - overall_start
    total_rows = sum(r['rows'] for r in results)
    total_populated = sum(r['populated'] for r in results)
    avg_relevant = sum(r['relevant_files'] for r in results) / len(results)

    print("\n" + "="*70)
    print("COMPLETION SUMMARY")
    print("="*70)
    print(f"Files processed:      {len(results)}")
    print(f"Total rows:           {total_rows:,}")
    print(f"Fields populated:     {total_populated:,}")
    print(f"Avg relevant files:   {avg_relevant:.0f} of {len(rtrans_index)}")
    print(f"Total time:           {total_elapsed/60:.1f} minutes")
    print(f"Average per file:     {total_elapsed/len(results):.1f} seconds")
    print(f"\nOutput directory:     {output_dir}")
    print("="*70)

    return 0


if __name__ == '__main__':
    sys.exit(main())
