#!/usr/bin/env python3
"""
Create Compact RTrans Dataset

Processes rtrans files individually to create compact analysis-ready datasets.
Adds missing metadata fields, applies funder matching, and filters to essential columns.

Usage:
    python create_compact_rtrans.py \\
        --input-dir /path/to/rtrans_out_full_parquets \\
        --metadata-dir /path/to/extracted_metadata_parquet \\
        --output-dir /path/to/compact_rtrans \\
        --max-field-length 30

Author: Generated for INCF 2025 Poster Analysis
Date: 2025-11-25 (Updated: 2025-11-26)
"""

import pandas as pd
import numpy as np
import logging
import sys
import argparse
import glob
import pickle
import re
from pathlib import Path
from collections import defaultdict
from datetime import datetime
import gc

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Funding columns to search for funder matching
FUNDING_COLUMNS = ['fund_text', 'fund_pmc_institute', 'fund_pmc_source', 'fund_pmc_anysource']


def normalize_pmcid(pmcid):
    """
    Normalize PMCID for consistent lookup.

    Args:
        pmcid: Raw PMCID value (may have whitespace, inconsistent formatting)

    Returns:
        str: Normalized PMCID (e.g., "PMC1234567")
    """
    if pd.isna(pmcid) or pmcid == '' or str(pmcid) == 'nan':
        return None

    pmcid_str = str(pmcid).strip().upper()

    # Remove any PMC prefix and get just the number
    pmcid_str = re.sub(r'^PMC', '', pmcid_str)

    # Remove any non-numeric characters
    pmcid_num = re.sub(r'[^0-9]', '', pmcid_str)

    if not pmcid_num:
        return None

    # Return with PMC prefix
    return f"PMC{pmcid_num}"


def load_metadata_lookup(metadata_dir, cache_file=None, rebuild=False):
    """
    Create PMCID → (file_size, chars_in_body) lookup dictionary from metadata files.

    Args:
        metadata_dir: Directory containing extracted_metadata parquet files
        cache_file: Path to cache file (pickle format)
        rebuild: Force rebuild even if cache exists

    Returns:
        dict: {pmcid_str: {'file_size': int, 'chars_in_body': int}}
    """
    # Try to load from cache first
    if cache_file and Path(cache_file).exists() and not rebuild:
        logger.info(f"Loading metadata lookup from cache: {cache_file}")
        try:
            with open(cache_file, 'rb') as f:
                lookup = pickle.load(f)
            logger.info(f"✓ Loaded {len(lookup):,} PMCIDs from cache")
            return lookup
        except Exception as e:
            logger.warning(f"Failed to load cache ({e}), rebuilding...")

    logger.info("Building metadata lookup dictionary (first run, ~10 minutes)...")
    metadata_files = sorted(glob.glob(f'{metadata_dir}/*.parquet'))

    if not metadata_files:
        logger.error(f"No parquet files found in {metadata_dir}")
        sys.exit(1)

    logger.debug(f"Found {len(metadata_files)} metadata files")

    lookup = {}
    total_records = 0

    for i, mf in enumerate(metadata_files, 1):
        try:
            df = pd.read_parquet(mf, columns=['pmcid_pmc', 'file_size', 'chars_in_body'])

            # Normalize PMCIDs
            df['pmcid_normalized'] = df['pmcid_pmc'].apply(normalize_pmcid)

            # Filter out invalid PMCIDs
            df = df[df['pmcid_normalized'].notna()]

            # Build lookup dictionary efficiently
            for _, row in df.iterrows():
                pmcid = row['pmcid_normalized']
                lookup[pmcid] = {
                    'file_size': row['file_size'] if pd.notna(row['file_size']) else None,
                    'chars_in_body': row['chars_in_body'] if pd.notna(row['chars_in_body']) else None
                }
                total_records += 1

            del df

            if i % 5 == 0:
                logger.info(f"  Building cache: {i}/{len(metadata_files)} files, {total_records:,} PMCIDs")
                gc.collect()

        except Exception as e:
            logger.error(f"Error processing {Path(mf).name}: {e}")

    logger.info(f"✓ Metadata lookup complete: {len(lookup):,} unique PMCIDs")

    # Save to cache
    if cache_file:
        logger.info(f"  Saving cache to {cache_file}...")
        try:
            with open(cache_file, 'wb') as f:
                pickle.dump(lookup, f, protocol=pickle.HIGHEST_PROTOCOL)
            logger.info(f"✓ Cache saved (will load in ~5 sec on next run)")
        except Exception as e:
            logger.warning(f"Failed to save cache: {e}")

    return lookup


def load_data_dictionary(dict_path):
    """
    Load data dictionary and return dataframe.

    Args:
        dict_path: Path to data_dictionary.csv

    Returns:
        DataFrame or None
    """
    try:
        dd = pd.read_csv(dict_path)
        logger.debug(f"Loaded data dictionary with {len(dd)} field definitions")
        return dd
    except FileNotFoundError:
        logger.warning(f"Data dictionary not found at {dict_path}, will keep all fields")
        return None


def load_funders_db(db_path):
    """Load the funder reference database."""
    try:
        funders_df = pd.read_csv(db_path)
        logger.debug(f"Loaded {len(funders_df)} funder organizations")
        return funders_df
    except FileNotFoundError:
        logger.error(f"Funder database not found at: {db_path}")
        sys.exit(1)


def match_funders_vectorized(df, funders_df, show_progress=False):
    """
    Apply funder matching using vectorized pandas operations (much faster).

    Args:
        df: DataFrame with funding columns
        funders_df: DataFrame with Name and Acronym columns
        show_progress: Show progress for large dataframes

    Returns:
        DataFrame: DataFrame with funder_* columns added
    """
    logger.debug(f"Applying funder matching to {len(df)} records")

    # Create combined funding text column for searching
    combined_texts = pd.Series([''] * len(df), index=df.index)

    for col in FUNDING_COLUMNS:
        if col in df.columns:
            combined_texts = combined_texts + ' ' + df[col].fillna('').astype(str)

    # Create lowercase version for case-insensitive name matching
    combined_texts_lower = combined_texts.str.lower()

    # Process each funder
    funder_cols = {}

    for idx, funder in funders_df.iterrows():
        name = funder['Name']
        acronym = funder['Acronym']

        # Create safe column name from acronym
        col_name = f"funder_{acronym.lower().replace(' ', '_').replace('-', '_')}"

        # Case-insensitive name match
        if pd.notna(name):
            name_matches = combined_texts_lower.str.contains(
                re.escape(name.lower()),
                regex=True,
                na=False
            )
        else:
            name_matches = pd.Series([False] * len(df), index=df.index)

        # Case-sensitive acronym match
        if pd.notna(acronym):
            acronym_matches = combined_texts.str.contains(
                re.escape(acronym),
                regex=True,
                na=False
            )
        else:
            acronym_matches = pd.Series([False] * len(df), index=df.index)

        # Combine matches
        funder_cols[col_name] = (name_matches | acronym_matches).astype(int)

        logger.debug(f"  Funder {idx + 1}/{len(funders_df)}: {acronym}")

    # Create funder dataframe
    funder_df = pd.DataFrame(funder_cols, index=df.index)

    # Add to original dataframe
    for col in funder_df.columns:
        df[col] = funder_df[col]

    return df


def filter_short_fields(df, max_length, data_dict, preserve_fields=None):
    """
    Filter dataframe to keep only fields with max_length <= threshold.

    Args:
        df: Input dataframe
        max_length: Maximum field length threshold
        data_dict: Data dictionary dataframe
        preserve_fields: List of field names to always preserve

    Returns:
        DataFrame: Filtered dataframe with only short fields + preserved fields
    """
    if data_dict is None:
        return df

    # Get fields within length threshold
    short_fields = data_dict[
        (data_dict['max_length'] <= max_length) |
        (data_dict['max_length'].isna())
    ]['column_name'].tolist()

    # Add preserved fields
    if preserve_fields:
        short_fields.extend([f for f in preserve_fields if f not in short_fields])

    # Keep only columns that exist in the dataframe
    cols_to_keep = [col for col in short_fields if col in df.columns]

    # Also keep any funder columns that were added
    funder_cols = [col for col in df.columns if col.startswith('funder_')]
    cols_to_keep.extend([col for col in funder_cols if col not in cols_to_keep])

    # Preserve order: original columns first, then funder columns
    ordered_cols = []
    for col in df.columns:
        if col in cols_to_keep:
            ordered_cols.append(col)

    logger.debug(f"Filtered to {len(ordered_cols)} fields (includes {len(funder_cols)} funder columns)")

    return df[ordered_cols]


def add_metadata_fields(df, metadata_lookup, pmcid_col='pmcid_pmc'):
    """
    Add file_size and chars_in_body from metadata lookup.

    Args:
        df: Input dataframe
        metadata_lookup: PMCID lookup dict
        pmcid_col: Name of PMCID column

    Returns:
        DataFrame: DataFrame with metadata fields added
        dict: Statistics about matches
    """
    if pmcid_col not in df.columns:
        logger.warning(f"No PMCID column '{pmcid_col}' in dataframe, skipping metadata lookup")
        df['file_size'] = None
        df['chars_in_body'] = None
        return df, {'total': len(df), 'matched': 0, 'file_size_found': 0, 'chars_found': 0}

    # Normalize PMCIDs in dataframe
    df['_pmcid_normalized'] = df[pmcid_col].apply(normalize_pmcid)

    # Vectorized lookup
    file_sizes = []
    chars_in_body = []
    matched = 0
    file_size_found = 0
    chars_found = 0

    for pmcid in df['_pmcid_normalized']:
        if pmcid and pmcid in metadata_lookup:
            matched += 1
            fs = metadata_lookup[pmcid]['file_size']
            cb = metadata_lookup[pmcid]['chars_in_body']
            file_sizes.append(fs)
            chars_in_body.append(cb)
            if fs is not None:
                file_size_found += 1
            if cb is not None:
                chars_found += 1
        else:
            file_sizes.append(None)
            chars_in_body.append(None)

    df['file_size'] = file_sizes
    df['chars_in_body'] = chars_in_body

    # Remove temporary column
    df = df.drop(columns=['_pmcid_normalized'])

    stats = {
        'total': len(df),
        'matched': matched,
        'file_size_found': file_size_found,
        'chars_found': chars_found
    }

    return df, stats


def process_rtrans_file(file_path, metadata_lookup, funders_df, max_length, data_dict):
    """
    Process a single rtrans file to create compact version.

    Args:
        file_path: Path to rtrans parquet file
        metadata_lookup: PMCID lookup dict from load_metadata_lookup()
        funders_df: Funder database dataframe
        max_length: Maximum field length to include
        data_dict: Data dictionary dataframe

    Returns:
        tuple: (DataFrame, dict of statistics)
    """
    # Load rtrans file
    df = pd.read_parquet(file_path)
    original_cols = df.shape[1]

    # Determine PMCID column
    pmcid_col = 'pmcid_pmc' if 'pmcid_pmc' in df.columns else 'pmcid'

    # Step 1: Filter to short fields (but don't add metadata/funders yet)
    df = filter_short_fields(df, max_length, data_dict, preserve_fields=[pmcid_col])

    # Step 2: Add metadata fields
    df, metadata_stats = add_metadata_fields(df, metadata_lookup, pmcid_col)

    # Step 3: Apply funder matching (vectorized, much faster)
    show_progress = len(df) > 2000
    df = match_funders_vectorized(df, funders_df, show_progress=show_progress)

    # Count funder columns
    funder_cols = [col for col in df.columns if col.startswith('funder_')]
    funder_matches = df[funder_cols].sum().sum() if funder_cols else 0

    stats = {
        'records': len(df),
        'original_cols': original_cols,
        'final_cols': df.shape[1],
        'funder_cols': len(funder_cols),
        'funder_matches': int(funder_matches),
        'metadata_matched': metadata_stats['matched'],
        'file_size_found': metadata_stats['file_size_found'],
        'chars_found': metadata_stats['chars_found']
    }

    return df, stats


def main():
    parser = argparse.ArgumentParser(
        description='Create compact rtrans datasets with funder matching and metadata enrichment.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full processing with cache
  python create_compact_rtrans.py \\
      --input-dir /path/to/rtrans_out_full_parquets \\
      --metadata-dir /path/to/extracted_metadata_parquet \\
      --output-dir /path/to/compact_rtrans

  # Test with first 10 files
  python create_compact_rtrans.py \\
      --input-dir /path/to/rtrans \\
      --metadata-dir /path/to/metadata \\
      --output-dir /path/to/output \\
      --limit 10

  # Resume processing (skip existing files)
  python create_compact_rtrans.py \\
      --input-dir /path/to/rtrans \\
      --metadata-dir /path/to/metadata \\
      --output-dir /path/to/output \\
      --resume

  # Force overwrite all files
  python create_compact_rtrans.py \\
      --input-dir /path/to/rtrans \\
      --metadata-dir /path/to/metadata \\
      --output-dir /path/to/output \\
      --overwrite
        """
    )

    parser.add_argument('--input-dir', required=True,
                        help='Directory containing rtrans_out_full_parquets files')
    parser.add_argument('--metadata-dir', required=True,
                        help='Directory containing extracted_metadata_parquet files')
    parser.add_argument('--output-dir', required=True,
                        help='Output directory for compact parquet files')
    parser.add_argument('--max-field-length', type=int, default=30,
                        help='Maximum field length to include (default: 30)')
    parser.add_argument('--funder-db',
                        default='funder_analysis/biomedical_research_funders.csv',
                        help='Path to funder database CSV (default: funder_analysis/biomedical_research_funders.csv)')
    parser.add_argument('--data-dict',
                        default='docs/data_dictionary.csv',
                        help='Path to data dictionary CSV (default: docs/data_dictionary.csv)')
    parser.add_argument('--limit', type=int, default=None,
                        help='Process only first N files (for testing)')
    parser.add_argument('--overwrite', action='store_true',
                        help='Overwrite existing output files')
    parser.add_argument('--resume', action='store_true',
                        help='Skip files that already exist in output directory')
    parser.add_argument('--cache-file',
                        default='metadata_lookup_cache.pkl',
                        help='Path to metadata lookup cache file (default: metadata_lookup_cache.pkl)')
    parser.add_argument('--rebuild-cache', action='store_true',
                        help='Force rebuild of metadata lookup cache')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable verbose (DEBUG level) logging')

    args = parser.parse_args()

    # Set logging level based on verbose flag
    if args.verbose:
        logger.setLevel(logging.DEBUG)
        logger.debug("Verbose logging enabled")

    # Validate directories
    if not Path(args.input_dir).exists():
        logger.error(f"Input directory not found: {args.input_dir}")
        sys.exit(1)

    if not Path(args.metadata_dir).exists():
        logger.error(f"Metadata directory not found: {args.metadata_dir}")
        sys.exit(1)

    # Check for conflicting flags
    if args.overwrite and args.resume:
        logger.error("Cannot use --overwrite and --resume together")
        sys.exit(1)

    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        logger.info("="*70)
        logger.info("CREATE COMPACT RTRANS DATASET")
        logger.info("="*70)
        logger.debug(f"Input: {args.input_dir}")
        logger.debug(f"Metadata: {args.metadata_dir}")
        logger.debug(f"Output: {args.output_dir}")
        logger.debug(f"Max field length: {args.max_field_length}")

        mode_info = []
        if args.overwrite:
            mode_info.append("overwrite")
        if args.resume:
            mode_info.append("resume")
        if args.limit:
            mode_info.append(f"limit={args.limit}")
        if mode_info:
            logger.info(f"Mode: {', '.join(mode_info)}")
        logger.info("")

        # Load metadata lookup (with caching)
        metadata_lookup = load_metadata_lookup(
            args.metadata_dir,
            cache_file=args.cache_file,
            rebuild=args.rebuild_cache
        )

        # Load data dictionary
        data_dict = load_data_dictionary(args.data_dict)

        # Load funder database
        funders_df = load_funders_db(args.funder_db)

        # Get rtrans files
        rtrans_files = sorted(glob.glob(f'{args.input_dir}/*.parquet'))

        if not rtrans_files:
            logger.error(f"No parquet files found in {args.input_dir}")
            sys.exit(1)

        if args.limit:
            rtrans_files = rtrans_files[:args.limit]
            logger.info(f"Processing first {args.limit} files (test mode)")

        logger.info(f"Found {len(rtrans_files)} rtrans files to process")
        logger.info("")

        # Process files
        total_records = 0
        total_files_processed = 0
        total_files_skipped = 0
        total_funder_matches = 0
        total_metadata_matched = 0
        first_file_stats = None
        start_time = datetime.now()

        logger.info("Starting file processing...")

        for i, rf in enumerate(rtrans_files, 1):
            filename = Path(rf).name
            output_file = output_dir / filename

            # Check if file exists and handle accordingly
            if output_file.exists():
                if args.resume:
                    total_files_skipped += 1
                    logger.debug(f"Skipping existing file: {filename}")
                    continue
                elif not args.overwrite:
                    logger.warning(f"File exists (use --overwrite or --resume): {filename}")
                    total_files_skipped += 1
                    continue

            logger.debug(f"Processing [{i}/{len(rtrans_files)}]: {filename}")

            try:
                # Process file
                compact_df, stats = process_rtrans_file(
                    rf,
                    metadata_lookup,
                    funders_df,
                    args.max_field_length,
                    data_dict
                )

                # Save first file stats for validation
                if first_file_stats is None:
                    first_file_stats = stats
                    logger.info("✓ First file processed successfully:")
                    logger.info(f"  {stats['original_cols']} columns → {stats['final_cols']} columns "
                               f"({stats['funder_cols']} funders, 2 metadata)")
                    logger.info(f"  Metadata matched: {stats['metadata_matched']}/{stats['records']} "
                               f"({100*stats['metadata_matched']/stats['records']:.1f}%)")
                    logger.debug(f"  Sample: {', '.join(compact_df.columns[:10].tolist())}")
                    if 'file_size' in compact_df.columns and 'chars_in_body' in compact_df.columns:
                        logger.info(f"  ✓ file_size and chars_in_body present")
                    funder_sample = [c for c in compact_df.columns if c.startswith('funder_')][:3]
                    if funder_sample:
                        logger.debug(f"  Funder sample: {', '.join(funder_sample)}")

                # Save compact file
                compact_df.to_parquet(output_file, index=False)

                total_records += stats['records']
                total_files_processed += 1
                total_funder_matches += stats['funder_matches']
                total_metadata_matched += stats['metadata_matched']

                # Show progress at intervals
                if i % 100 == 0 or i == len(rtrans_files):
                    pct_complete = 100 * i / len(rtrans_files)
                    elapsed = (datetime.now() - start_time).total_seconds()
                    rate = i / elapsed if elapsed > 0 else 0
                    eta_sec = (len(rtrans_files) - i) / rate if rate > 0 else 0
                    eta_min = int(eta_sec / 60)

                    if i == len(rtrans_files):
                        logger.info(f"Progress: {i}/{len(rtrans_files)} (100%) - Complete!")
                    else:
                        logger.info(f"Progress: {i}/{len(rtrans_files)} ({pct_complete:.0f}%) | "
                                  f"{rate:.1f} files/sec | "
                                  f"ETA: {eta_min}m")

                del compact_df
                gc.collect()

            except Exception as e:
                logger.error(f"Error processing {filename}: {e}")
                import traceback
                traceback.print_exc()

        # Calculate runtime
        elapsed = datetime.now() - start_time
        elapsed_min = int(elapsed.total_seconds() / 60)
        elapsed_sec = int(elapsed.total_seconds() % 60)

        logger.info("")
        logger.info("="*70)
        logger.info("✓ PROCESSING COMPLETE")
        logger.info("="*70)
        logger.info(f"Runtime: {elapsed_min}m {elapsed_sec}s")
        logger.info(f"Files: {total_files_processed} processed, {total_files_skipped} skipped")
        logger.info(f"Records: {total_records:,} total")
        logger.info(f"Metadata: {total_metadata_matched:,}/{total_records:,} matched "
                   f"({100*total_metadata_matched/total_records:.1f}%)" if total_records > 0 else "")
        logger.info(f"Funders: {total_funder_matches:,} total matches across all records")

        if first_file_stats:
            logger.info("")
            logger.info("Output schema:")
            logger.info(f"  {first_file_stats['final_cols']} columns per file")
            logger.info(f"  └─ {first_file_stats['original_cols']} original short fields")
            logger.info(f"  └─ {first_file_stats['funder_cols']} funder binary columns")
            logger.info(f"  └─ 2 metadata fields (file_size, chars_in_body)")

        logger.info("")
        logger.info(f"Output: {args.output_dir}")
        logger.info("Dataset ready for analysis!")

    except Exception as e:
        logger.error(f"Failed to process data: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
