#!/usr/bin/env python3
"""
Build dashboard data file from PMC Open Access corpus - HPC Optimized Version.

This version is optimized for high-memory, multi-core HPC nodes (e.g., NIH Biowulf):
- Parallel funder matching using multiprocessing
- Vectorized operations where possible
- Progress bars with ETAs using tqdm
- Memory-efficient batch processing
- Detailed timing statistics

Expected speedup: 10-50x faster than original (depending on CPU cores)
Recommended: Run on node with 32+ GB RAM and 16+ CPU cores

Input sources:
- PMC filelist CSVs (for PMCID list with license info)
- rtrans parquets (for journal, country, year, funding text)
- oddpub merged parquet (for is_open_data/code)
- Funder aliases CSV (for canonical funder matching)

Usage:
    # On HPC with 32 cores
    python build_dashboard_data_hpc.py \
        --filelist-dir /data/pmcoaXMLs/raw_download \
        --rtrans-dir /data/pmcoaXMLs/rtrans_out_full_parquets \
        --oddpub-file /data/pmcoaXMLs/oddpub_merged/oddpub_v7.2.3_all.parquet \
        --output /data/matches_dashboard.parquet \
        --licenses comm,noncomm \
        --workers 32

    # Test with small subset
    python build_dashboard_data_hpc.py \
        --filelist-dir ~/pmcoaXMLs/raw_download \
        --rtrans-dir ~/pmcoaXMLs/rtrans_out_full_parquets \
        --oddpub-file ~/pmcoaXMLs/oddpub_merged/oddpub_v7.2.3_all.parquet \
        --output ~/test_dashboard.parquet \
        --licenses comm \
        --limit 10000
"""

import argparse
import os
import re
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from glob import glob
from multiprocessing import cpu_count
from pathlib import Path
from typing import List, Dict, Tuple, Optional

import numpy as np
import pandas as pd

# Try to import tqdm for progress bars
try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    print("Note: Install tqdm for progress bars: pip install tqdm")

# Add parent directory for imports
sys.path.insert(0, str(Path(__file__).parent.parent))


def log_time(msg: str, start_time: float = None) -> float:
    """Log message with timestamp and optional elapsed time."""
    now = time.time()
    timestamp = datetime.now().strftime('%H:%M:%S')
    if start_time is not None:
        elapsed = now - start_time
        print(f"[{timestamp}] {msg} (elapsed: {elapsed:.1f}s)")
    else:
        print(f"[{timestamp}] {msg}")
    return now


def parse_args():
    parser = argparse.ArgumentParser(
        description="Build dashboard data file (HPC optimized)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Full production run with 32 cores
    python build_dashboard_data_hpc.py \\
        --filelist-dir /data/pmcoaXMLs/raw_download \\
        --rtrans-dir /data/pmcoaXMLs/rtrans_out_full_parquets \\
        --oddpub-file /data/pmcoaXMLs/oddpub_merged/oddpub_v7.2.3_all.parquet \\
        --output /data/matches_dashboard.parquet \\
        --workers 32

    # Test run with 1000 PMCIDs
    python build_dashboard_data_hpc.py \\
        --filelist-dir ~/pmcoaXMLs/raw_download \\
        --rtrans-dir ~/pmcoaXMLs/rtrans_out_full_parquets \\
        --oddpub-file ~/pmcoaXMLs/oddpub_merged/oddpub_v7.2.3_all.parquet \\
        --output ~/test_dashboard.parquet \\
        --limit 1000
        """
    )
    parser.add_argument(
        "--filelist-dir",
        required=True,
        help="Directory containing PMC filelist CSVs",
    )
    parser.add_argument(
        "--rtrans-dir",
        required=True,
        help="Directory containing rtrans parquet files",
    )
    parser.add_argument(
        "--oddpub-file",
        required=True,
        help="Path to merged oddpub parquet file",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output parquet file path",
    )
    parser.add_argument(
        "--licenses",
        default="comm,noncomm",
        help="Comma-separated list of license types to include (default: comm,noncomm)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of PMCIDs to process (for testing)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help=f"Number of parallel workers (default: CPU count = {cpu_count()})",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=100000,
        help="Chunk size for parallel processing (default: 100000)",
    )
    return parser.parse_args()


def load_pmcid_list(filelist_dir: str, licenses: List[str]) -> pd.DataFrame:
    """Load all PMCIDs from filelist CSVs with license information."""
    start = log_time(f"Loading PMCIDs from {filelist_dir}...")

    all_records = []

    for license_type in licenses:
        pattern = f"oa_{license_type}_xml.PMC*.baseline.*.filelist.csv"
        csv_files = glob(os.path.join(filelist_dir, pattern))
        log_time(f"  Found {len(csv_files)} {license_type} filelist CSVs")

        for csv_file in csv_files:
            try:
                df = pd.read_csv(csv_file)
                if len(df) == 0:
                    continue

                # Extract PMCID from Accession ID column
                if 'Accession ID' in df.columns:
                    pmcids = df['Accession ID'].astype(str)
                elif 'AccessionID' in df.columns:
                    pmcids = df['AccessionID'].astype(str)
                else:
                    pmcids = df.iloc[:, 0].astype(str)

                # Get PMIDs if available
                pmids = df.get('PMID', pd.Series([None] * len(df)))

                for pmcid, pmid in zip(pmcids, pmids):
                    if pd.notna(pmcid) and pmcid.startswith('PMC'):
                        all_records.append({
                            'pmcid': pmcid.strip(),
                            'pmid': int(pmid) if pd.notna(pmid) and str(pmid).isdigit() else None,
                            'license': license_type,
                        })
            except Exception as e:
                print(f"    Warning: Error reading {csv_file}: {e}")

    df = pd.DataFrame(all_records)
    df = df.drop_duplicates(subset=['pmcid'])

    log_time(f"  Total unique PMCIDs: {len(df):,}", start)
    print(f"  License distribution:")
    for license_type, count in df['license'].value_counts().items():
        print(f"    {license_type}: {count:,}")

    return df


def load_rtrans_data(rtrans_dir: str, pmcids: set) -> pd.DataFrame:
    """Load relevant columns from rtrans parquet files with progress bar."""
    start = log_time(f"Loading rtrans data from {rtrans_dir}...")

    parquet_files = sorted(glob(os.path.join(rtrans_dir, "*.parquet")))
    log_time(f"  Found {len(parquet_files)} parquet files")

    columns = [
        'pmid', 'pmcid_pmc', 'journal', 'affiliation_country',
        'year_epub', 'year_ppub', 'fund_text', 'fund_pmc_source',
        'fund_pmc_institute', 'fund_pmc_anysource'
    ]

    all_dfs = []

    if HAS_TQDM:
        iterator = tqdm(parquet_files, desc="  Loading rtrans files", unit="file")
    else:
        iterator = parquet_files

    for i, pfile in enumerate(iterator):
        if not HAS_TQDM and (i + 1) % 100 == 0:
            print(f"    Processing file {i+1}/{len(parquet_files)}...")

        try:
            df = pd.read_parquet(pfile, columns=columns)

            # Vectorized PMCID normalization
            df['pmcid'] = df['pmcid_pmc'].apply(
                lambda x: f"PMC{x}" if pd.notna(x) and not str(x).startswith('PMC') else str(x) if pd.notna(x) else None
            )

            # Filter to PMCIDs we care about
            df = df[df['pmcid'].isin(pmcids)]

            if len(df) > 0:
                all_dfs.append(df)
        except Exception as e:
            print(f"    Warning: Error reading {pfile}: {e}")

    if not all_dfs:
        return pd.DataFrame()

    result = pd.concat(all_dfs, ignore_index=True)
    result = result.drop_duplicates(subset=['pmcid'])
    log_time(f"  Loaded {len(result):,} records from rtrans", start)
    return result


def load_oddpub_data(oddpub_file: str, pmcids: set) -> pd.DataFrame:
    """Load oddpub data for open data/code detection."""
    start = log_time(f"Loading oddpub data from {oddpub_file}...")

    df = pd.read_parquet(oddpub_file)
    log_time(f"  Total records in oddpub: {len(df):,}")

    # Vectorized PMCID extraction using regex
    if 'article' in df.columns:
        def extract_pmcid(article):
            if pd.isna(article):
                return None
            match = re.match(r'PMC(PMC\d+)\.txt', str(article))
            if match:
                return match.group(1)
            match = re.match(r'(PMC\d+)', str(article))
            if match:
                return match.group(1)
            return None

        df['pmcid'] = df['article'].apply(extract_pmcid)
        log_time(f"  Extracted PMCIDs from article column")

    # Filter to PMCIDs we care about
    df = df[df['pmcid'].isin(pmcids)]

    # Keep only columns we need
    keep_cols = ['pmcid', 'is_open_data', 'is_open_code']
    df = df[[c for c in keep_cols if c in df.columns]]

    log_time(f"  Matched {len(df):,} records", start)
    return df


# Global variable for worker processes (loaded once per process)
_worker_patterns = None


def _init_worker(patterns_dict: Dict[str, str]):
    """Initialize worker process with compiled patterns."""
    global _worker_patterns
    # Recompile patterns in worker process
    _worker_patterns = {
        canonical: re.compile(pattern, re.IGNORECASE)
        for canonical, pattern in patterns_dict.items()
    }


def _match_funders_batch(texts: List[str]) -> List[List[str]]:
    """Match funders for a batch of texts (runs in worker process)."""
    global _worker_patterns
    results = []
    for text in texts:
        if pd.isna(text) or not text:
            results.append([])
        else:
            text_str = str(text)
            matched = [
                canonical for canonical, pattern in _worker_patterns.items()
                if pattern.search(text_str)
            ]
            results.append(matched)
    return results


def build_funder_patterns(aliases_csv: Path) -> Dict[str, str]:
    """Build regex pattern strings from funder aliases CSV."""
    from funder_analysis.normalize_funders import FunderNormalizer
    normalizer = FunderNormalizer(str(aliases_csv))

    # Extract pattern strings (not compiled patterns, for serialization)
    patterns = {}
    for canonical in normalizer.get_all_canonical_names():
        variants = normalizer.get_variants(canonical)
        sorted_variants = sorted(variants, key=len, reverse=True)

        pattern_parts = []
        for variant in sorted_variants:
            escaped = re.escape(variant)
            if len(variant) <= 6 and variant.isupper():
                pattern_parts.append(r'\b' + escaped + r'\b')
            else:
                pattern_parts.append(escaped)

        patterns[canonical] = '|'.join(pattern_parts)

    return patterns


def match_funders_parallel(
    texts: pd.Series,
    patterns: Dict[str, str],
    num_workers: int,
    chunk_size: int = 10000,
) -> List[List[str]]:
    """Match funders in parallel across multiple processes."""
    start = log_time(f"Matching funders with {num_workers} workers...")

    # Convert to list for easier chunking
    text_list = texts.tolist()
    n_total = len(text_list)

    # Create chunks
    chunks = [
        text_list[i:i + chunk_size]
        for i in range(0, n_total, chunk_size)
    ]
    log_time(f"  Split into {len(chunks)} chunks of up to {chunk_size}")

    results = [None] * len(chunks)

    with ProcessPoolExecutor(
        max_workers=num_workers,
        initializer=_init_worker,
        initargs=(patterns,)
    ) as executor:
        # Submit all tasks
        future_to_idx = {
            executor.submit(_match_funders_batch, chunk): i
            for i, chunk in enumerate(chunks)
        }

        # Collect results with progress
        if HAS_TQDM:
            iterator = tqdm(
                as_completed(future_to_idx),
                total=len(chunks),
                desc="  Matching funders",
                unit="chunk"
            )
        else:
            iterator = as_completed(future_to_idx)
            completed = 0

        for future in iterator:
            idx = future_to_idx[future]
            try:
                results[idx] = future.result()
            except Exception as e:
                print(f"    Warning: Chunk {idx} failed: {e}")
                results[idx] = [[] for _ in chunks[idx]]

            if not HAS_TQDM:
                completed += 1
                if completed % 10 == 0 or completed == len(chunks):
                    pct = 100 * completed / len(chunks)
                    print(f"    Progress: {completed}/{len(chunks)} chunks ({pct:.1f}%)")

    # Flatten results
    all_funders = []
    for chunk_result in results:
        all_funders.extend(chunk_result)

    log_time(f"  Matched funders for {len(all_funders):,} records", start)

    # Count stats
    has_funder = sum(1 for f in all_funders if f)
    log_time(f"  Records with funders: {has_funder:,} ({100*has_funder/len(all_funders):.2f}%)")

    return all_funders


def build_dashboard_data(
    pmcid_df: pd.DataFrame,
    rtrans_df: pd.DataFrame,
    oddpub_df: pd.DataFrame,
    patterns: Dict[str, str],
    num_workers: int,
    chunk_size: int,
) -> pd.DataFrame:
    """Merge all data sources and build final output."""
    start = log_time("Building dashboard data...")

    # Start with PMCID list
    result = pmcid_df.copy()
    log_time(f"  Starting with {len(result):,} PMCIDs")

    # Merge rtrans data
    merge_start = log_time("  Merging rtrans data...")
    if len(rtrans_df) > 0:
        rtrans_cols = ['pmcid', 'pmid', 'journal', 'affiliation_country',
                       'year_epub', 'year_ppub', 'fund_text', 'fund_pmc_source',
                       'fund_pmc_institute', 'fund_pmc_anysource']
        rtrans_subset = rtrans_df[[c for c in rtrans_cols if c in rtrans_df.columns]]

        result = result.merge(rtrans_subset, on='pmcid', how='left', suffixes=('', '_rtrans'))

        if 'pmid_rtrans' in result.columns:
            result['pmid'] = result['pmid'].combine_first(result['pmid_rtrans'])
            result = result.drop(columns=['pmid_rtrans'])
    log_time(f"    Merged, now {len(result):,} rows", merge_start)

    # Merge oddpub data
    merge_start = log_time("  Merging oddpub data...")
    if len(oddpub_df) > 0:
        result = result.merge(oddpub_df, on='pmcid', how='left')
    log_time(f"    Merged, now {len(result):,} rows", merge_start)

    # Fill missing open data/code with False
    if 'is_open_data' not in result.columns:
        result['is_open_data'] = False
    else:
        result['is_open_data'] = result['is_open_data'].fillna(False).astype(bool)

    if 'is_open_code' not in result.columns:
        result['is_open_code'] = False
    else:
        result['is_open_code'] = result['is_open_code'].fillna(False).astype(bool)

    # Calculate year (prefer epub)
    if 'year_epub' in result.columns and 'year_ppub' in result.columns:
        result['year'] = result['year_epub'].combine_first(result['year_ppub'])
        result['year'] = pd.to_numeric(result['year'], errors='coerce')
    else:
        result['year'] = None

    # Combine funding text columns for funder matching
    combine_start = log_time("  Combining funding text columns...")
    funding_cols = ['fund_text', 'fund_pmc_source', 'fund_pmc_institute', 'fund_pmc_anysource']
    available_cols = [c for c in funding_cols if c in result.columns]

    if available_cols:
        # Vectorized string concatenation
        result['_combined_funding'] = result[available_cols].fillna('').agg(' '.join, axis=1)
        # Replace empty strings with None for cleaner handling
        result['_combined_funding'] = result['_combined_funding'].replace(r'^\s*$', None, regex=True)
    else:
        result['_combined_funding'] = None
    log_time(f"    Combined {len(available_cols)} funding columns", combine_start)

    # Match funders in parallel
    funder_lists = match_funders_parallel(
        result['_combined_funding'],
        patterns,
        num_workers,
        chunk_size
    )

    # Convert to numpy arrays
    result['funder'] = [np.array(f, dtype=object) for f in funder_lists]

    # Build data_tags
    tags_start = log_time("  Building data_tags...")
    result['data_tags'] = result['license'].apply(
        lambda lic: np.array(['pmc_oa', lic if pd.notna(lic) else 'unknown'], dtype=object)
    )
    log_time("    Done", tags_start)

    # Add created_at timestamp
    result['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + '-04'

    # Select final columns in correct order
    final_columns = [
        'pmid', 'journal', 'affiliation_country',
        'is_open_data', 'is_open_code', 'year',
        'funder', 'data_tags', 'created_at'
    ]

    for col in final_columns:
        if col not in result.columns:
            result[col] = None

    result = result[final_columns]

    # Convert pmid to int64 (with NaN handling)
    result['pmid'] = pd.to_numeric(result['pmid'], errors='coerce').astype('Int64')

    log_time(f"  Final dataset: {len(result):,} records", start)
    print(f"  Open data: {result['is_open_data'].sum():,} ({100*result['is_open_data'].mean():.2f}%)")
    print(f"  Open code: {result['is_open_code'].sum():,} ({100*result['is_open_code'].mean():.2f}%)")

    has_funder = result['funder'].apply(lambda x: len(x) > 0 if isinstance(x, np.ndarray) else False)
    print(f"  With funders: {has_funder.sum():,} ({100*has_funder.mean():.2f}%)")

    return result


def main():
    total_start = time.time()
    args = parse_args()

    print("=" * 70)
    print("Dashboard Data Builder - HPC Optimized")
    print("=" * 70)

    # Configuration
    num_workers = args.workers or cpu_count()
    licenses = [l.strip() for l in args.licenses.split(',')]

    print(f"\nConfiguration:")
    print(f"  Workers: {num_workers}")
    print(f"  Chunk size: {args.chunk_size:,}")
    print(f"  Licenses: {licenses}")
    print(f"  Output: {args.output}")
    if args.limit:
        print(f"  Limit: {args.limit:,} PMCIDs (testing mode)")
    print()

    # Load PMCID list
    pmcid_df = load_pmcid_list(args.filelist_dir, licenses)

    if args.limit:
        log_time(f"Limiting to {args.limit} PMCIDs for testing")
        pmcid_df = pmcid_df.head(args.limit)

    pmcids = set(pmcid_df['pmcid'].dropna())
    log_time(f"Total PMCIDs to process: {len(pmcids):,}")

    # Load rtrans data
    rtrans_df = load_rtrans_data(args.rtrans_dir, pmcids)

    # Load oddpub data
    oddpub_df = load_oddpub_data(args.oddpub_file, pmcids)

    # Build funder patterns
    aliases_csv = Path(__file__).parent.parent / 'funder_analysis' / 'funder_aliases.csv'
    if aliases_csv.exists():
        patterns = build_funder_patterns(aliases_csv)
        log_time(f"Loaded {len(patterns)} canonical funder patterns")
    else:
        log_time(f"Warning: Funder aliases file not found at {aliases_csv}")
        patterns = {}

    # Build final dataset
    result = build_dashboard_data(
        pmcid_df, rtrans_df, oddpub_df, patterns,
        num_workers, args.chunk_size
    )

    # Save output
    save_start = log_time(f"Saving to {args.output}...")
    result.to_parquet(args.output, index=False)
    file_size = os.path.getsize(args.output) / (1024 * 1024)
    log_time(f"  Saved {file_size:.1f} MB", save_start)

    # Verify output
    verify = pd.read_parquet(args.output)
    log_time(f"Verification: {len(verify):,} records, {len(verify.columns)} columns")
    print(f"  Columns: {verify.columns.tolist()}")

    # Summary
    total_elapsed = time.time() - total_start
    print()
    print("=" * 70)
    print(f"COMPLETE in {total_elapsed/60:.1f} minutes ({total_elapsed:.0f} seconds)")
    print(f"  Records: {len(result):,}")
    print(f"  Output: {args.output}")
    print(f"  Size: {file_size:.1f} MB")
    print("=" * 70)


if __name__ == "__main__":
    main()
