#!/usr/bin/env python3
"""
Build dashboard data file from PMC Open Access corpus - DuckDB Version.

This version uses DuckDB for extremely fast data loading and joining:
- Reads all parquet files with a single glob pattern (no per-file overhead)
- Uses SQL joins instead of pandas .isin() (orders of magnitude faster)
- Memory-efficient columnar processing
- Expected runtime: 5-15 minutes for 6.5M PMCIDs

Input sources:
- PMC filelist CSVs (for PMCID list with license info)
- rtrans parquets (for journal, country, year, funding text)
- oddpub merged parquet (for is_open_data/code)
- Funder aliases CSV (for canonical funder matching)

Usage:
    # Full production run (uses funder_aliases_v3.csv by default)
    python build_dashboard_data_duckdb.py \
        --filelist-dir /data/pmcoaXMLs/raw_download \
        --rtrans-dir /data/pmcoaXMLs/rtrans_out_full_parquets \
        --oddpub-file /data/pmcoaXMLs/oddpub_merged/oddpub_v7.2.3_all.parquet \
        --output /data/matches_dashboard.parquet \
        --licenses comm,noncomm

    # Specify a different funder aliases file
    python build_dashboard_data_duckdb.py \
        --filelist-dir /data/pmcoaXMLs/raw_download \
        --rtrans-dir /data/pmcoaXMLs/rtrans_out_full_parquets \
        --oddpub-file /data/pmcoaXMLs/oddpub_merged/oddpub_v7.2.3_all.parquet \
        --funder-aliases funder_analysis/funder_aliases_v2.csv \
        --output /data/matches_dashboard.parquet

    # Test with small subset
    python build_dashboard_data_duckdb.py \
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

import duckdb
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
        description="Build dashboard data file (DuckDB version - fastest)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Full production run
    python build_dashboard_data_duckdb.py \\
        --filelist-dir /data/pmcoaXMLs/raw_download \\
        --rtrans-dir /data/pmcoaXMLs/rtrans_out_full_parquets \\
        --oddpub-file /data/pmcoaXMLs/oddpub_merged/oddpub_v7.2.3_all.parquet \\
        --output /data/matches_dashboard.parquet

    # Test run with 1000 PMCIDs
    python build_dashboard_data_duckdb.py \\
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
        help=f"Number of parallel workers for funder matching (default: CPU count = {cpu_count()})",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=100000,
        help="Chunk size for funder matching (default: 100000)",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=None,
        help="Number of DuckDB threads (default: auto)",
    )
    parser.add_argument(
        "--funder-aliases",
        default=None,
        help="Path to funder aliases CSV file (default: funder_analysis/funder_aliases_v3.csv)",
    )
    parser.add_argument(
        "--aggregate-children",
        action="store_true",
        help="Aggregate child funders into parent totals (e.g., NIH institutes -> NIH)",
    )
    return parser.parse_args()


def load_data_with_duckdb(
    filelist_dir: str,
    rtrans_dir: str,
    oddpub_file: str,
    licenses: List[str],
    limit: Optional[int] = None,
    threads: Optional[int] = None,
) -> pd.DataFrame:
    """Load and join all data sources using DuckDB for maximum speed."""

    start = log_time("Initializing DuckDB...")

    # Create DuckDB connection with optimal settings
    conn = duckdb.connect(':memory:')
    if threads:
        conn.execute(f"SET threads TO {threads}")

    # Step 1: Load PMCIDs from filelist CSVs
    log_time("Loading PMCIDs from filelist CSVs...")

    # Build UNION ALL query for all filelist CSVs
    csv_queries = []
    for license_type in licenses:
        pattern = f"oa_{license_type}_xml.PMC*.baseline.*.filelist.csv"
        csv_files = sorted(glob(os.path.join(filelist_dir, pattern)))
        log_time(f"  Found {len(csv_files)} {license_type} filelist CSVs")

        for csv_file in csv_files:
            # Note: DuckDB read_csv_auto normalizes "Accession ID" to "AccessionID"
            csv_queries.append(f"""
                SELECT
                    TRIM(AccessionID) as pmcid,
                    TRY_CAST(PMID AS BIGINT) as pmid_filelist,
                    '{license_type}' as license
                FROM read_csv_auto('{csv_file}', header=true)
                WHERE AccessionID IS NOT NULL
                  AND AccessionID LIKE 'PMC%'
            """)

    if not csv_queries:
        raise ValueError("No filelist CSVs found!")

    # Create pmcids table from all CSVs
    union_query = " UNION ALL ".join(csv_queries)

    limit_clause = f"LIMIT {limit}" if limit else ""

    conn.execute(f"""
        CREATE TABLE pmcids AS
        SELECT DISTINCT ON (pmcid) pmcid, pmid_filelist, license
        FROM ({union_query})
        ORDER BY pmcid
        {limit_clause}
    """)

    pmcid_count = conn.execute("SELECT COUNT(*) FROM pmcids").fetchone()[0]
    log_time(f"  Loaded {pmcid_count:,} unique PMCIDs")

    # Show license distribution
    print("  License distribution:")
    for row in conn.execute("SELECT license, COUNT(*) as cnt FROM pmcids GROUP BY license ORDER BY cnt DESC").fetchall():
        print(f"    {row[0]}: {row[1]:,}")

    # Step 2: Load rtrans data with glob pattern (very fast!)
    rtrans_start = log_time("Loading rtrans data...")

    rtrans_pattern = os.path.join(rtrans_dir, "*.parquet")

    conn.execute(f"""
        CREATE TABLE rtrans AS
        SELECT
            pmid,
            CASE
                WHEN pmcid_pmc LIKE 'PMC%' THEN pmcid_pmc
                WHEN pmcid_pmc IS NOT NULL THEN 'PMC' || pmcid_pmc
                ELSE NULL
            END as pmcid,
            journal,
            affiliation_country,
            year_epub,
            year_ppub,
            fund_text,
            fund_pmc_source,
            fund_pmc_institute,
            fund_pmc_anysource
        FROM read_parquet('{rtrans_pattern}')
        WHERE pmcid_pmc IS NOT NULL
    """)

    rtrans_count = conn.execute("SELECT COUNT(*) FROM rtrans").fetchone()[0]
    log_time(f"  Loaded {rtrans_count:,} rtrans records", rtrans_start)

    # Step 3: Load oddpub data
    # NOTE: The oddpub parquet has a clean 'pmcid' column (format: PMC544856)
    # We use it directly instead of extracting from 'article' column
    oddpub_start = log_time("Loading oddpub data...")

    conn.execute(f"""
        CREATE TABLE oddpub AS
        SELECT
            pmcid,
            is_open_data,
            is_open_code
        FROM read_parquet('{oddpub_file}')
        WHERE pmcid IS NOT NULL
    """)

    oddpub_count = conn.execute("SELECT COUNT(*) FROM oddpub").fetchone()[0]
    log_time(f"  Loaded {oddpub_count:,} oddpub records", oddpub_start)

    # Step 4: Join all tables
    join_start = log_time("Joining tables...")

    conn.execute("""
        CREATE TABLE merged AS
        SELECT
            COALESCE(TRY_CAST(r.pmid AS BIGINT), TRY_CAST(p.pmid_filelist AS BIGINT)) as pmid,
            p.pmcid,
            p.license,
            r.journal,
            r.affiliation_country,
            COALESCE(TRY_CAST(r.year_epub AS INTEGER), TRY_CAST(r.year_ppub AS INTEGER)) as year,
            COALESCE(o.is_open_data, false) as is_open_data,
            COALESCE(o.is_open_code, false) as is_open_code,
            COALESCE(r.fund_text, '') || ' ' ||
            COALESCE(r.fund_pmc_source, '') || ' ' ||
            COALESCE(r.fund_pmc_institute, '') || ' ' ||
            COALESCE(r.fund_pmc_anysource, '') as combined_funding
        FROM pmcids p
        LEFT JOIN rtrans r ON p.pmcid = r.pmcid
        LEFT JOIN oddpub o ON p.pmcid = o.pmcid
    """)

    merged_count = conn.execute("SELECT COUNT(*) FROM merged").fetchone()[0]
    log_time(f"  Merged: {merged_count:,} records", join_start)

    # Show match statistics
    rtrans_matched = conn.execute("SELECT COUNT(*) FROM merged WHERE journal IS NOT NULL").fetchone()[0]
    oddpub_matched = conn.execute("SELECT COUNT(*) FROM merged WHERE is_open_data IS NOT NULL").fetchone()[0]
    print(f"  rtrans matched: {rtrans_matched:,} ({100*rtrans_matched/merged_count:.1f}%)")
    print(f"  oddpub matched: {oddpub_matched:,} ({100*oddpub_matched/merged_count:.1f}%)")

    # Convert to pandas for funder matching
    export_start = log_time("Exporting to pandas...")
    result_df = conn.execute("SELECT * FROM merged").df()
    log_time(f"  Exported {len(result_df):,} records", export_start)

    conn.close()
    log_time("DuckDB data loading complete", start)

    return result_df


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
        if pd.isna(text) or not text or text.strip() == '':
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


def build_child_to_parent_map(aliases_csv: Path) -> Dict[str, str]:
    """Build a mapping of child funders to their parent funders."""
    from funder_analysis.normalize_funders import FunderNormalizer
    normalizer = FunderNormalizer(str(aliases_csv))

    child_to_parent = {}
    for canonical in normalizer.get_all_canonical_names():
        parent = normalizer.get_parent(canonical)
        if parent:
            child_to_parent[canonical] = parent

    return child_to_parent


def aggregate_funders_in_lists(
    funder_lists: List[List[str]],
    child_to_parent: Dict[str, str],
) -> List[List[str]]:
    """
    Aggregate child funders into parent funders for each article's funder list.

    For each article, if a child funder is found:
    - Add the parent funder if not already present
    - Remove the child funder from the list

    Args:
        funder_lists: List of funder lists (one per article)
        child_to_parent: Dict mapping child funder names to parent names

    Returns:
        Modified funder lists with aggregation applied
    """
    aggregated_lists = []
    for funders in funder_lists:
        if not funders:
            aggregated_lists.append([])
            continue

        # Start with a set of funders for efficient lookup
        funder_set = set(funders)

        # Add parent funders for any children found
        for funder in list(funder_set):
            parent = child_to_parent.get(funder)
            if parent:
                funder_set.add(parent)
                funder_set.discard(funder)  # Remove child

        aggregated_lists.append(list(funder_set))

    return aggregated_lists


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


def build_final_output(
    df: pd.DataFrame,
    funder_lists: List[List[str]],
) -> pd.DataFrame:
    """Build final output DataFrame with correct schema."""
    start = log_time("Building final output...")

    # Add funder arrays
    df['funder'] = [np.array(f, dtype=object) for f in funder_lists]

    # Build data_tags
    df['data_tags'] = df['license'].apply(
        lambda lic: np.array(['pmc_oa', lic if pd.notna(lic) else 'unknown'], dtype=object)
    )

    # Add created_at timestamp
    df['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + '-04'

    # Select final columns in correct order
    final_columns = [
        'pmid', 'journal', 'affiliation_country',
        'is_open_data', 'is_open_code', 'year',
        'funder', 'data_tags', 'created_at'
    ]

    for col in final_columns:
        if col not in df.columns:
            df[col] = None

    result = df[final_columns].copy()

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
    print("Dashboard Data Builder - DuckDB Version (Fastest)")
    print("=" * 70)

    # Configuration
    num_workers = args.workers or cpu_count()
    licenses = [l.strip() for l in args.licenses.split(',')]

    # Determine funder aliases file
    if args.funder_aliases:
        funder_aliases_display = args.funder_aliases
    else:
        funder_aliases_display = "funder_analysis/funder_aliases_v3.csv (default)"

    print(f"\nConfiguration:")
    print(f"  Workers (funder matching): {num_workers}")
    print(f"  Chunk size: {args.chunk_size:,}")
    print(f"  Licenses: {licenses}")
    print(f"  Funder aliases: {funder_aliases_display}")
    print(f"  Aggregate children: {args.aggregate_children}")
    print(f"  Output: {args.output}")
    if args.limit:
        print(f"  Limit: {args.limit:,} PMCIDs (testing mode)")
    if args.threads:
        print(f"  DuckDB threads: {args.threads}")
    print()

    # Load and join data using DuckDB
    df = load_data_with_duckdb(
        args.filelist_dir,
        args.rtrans_dir,
        args.oddpub_file,
        licenses,
        args.limit,
        args.threads,
    )

    # Build funder patterns
    if args.funder_aliases:
        aliases_csv = Path(args.funder_aliases)
    else:
        # Default to v3 funder aliases
        aliases_csv = Path(__file__).parent.parent / 'funder_analysis' / 'funder_aliases_v3.csv'

    if aliases_csv.exists():
        patterns = build_funder_patterns(aliases_csv)
        log_time(f"Loaded {len(patterns)} canonical funder patterns from {aliases_csv}")
    else:
        log_time(f"Warning: Funder aliases file not found at {aliases_csv}")
        patterns = {}

    # Match funders in parallel
    funder_lists = match_funders_parallel(
        df['combined_funding'],
        patterns,
        num_workers,
        args.chunk_size
    )

    # Aggregate children into parents if requested
    if args.aggregate_children and aliases_csv.exists():
        agg_start = log_time("Aggregating child funders into parents...")
        child_to_parent = build_child_to_parent_map(aliases_csv)
        if child_to_parent:
            log_time(f"  Found {len(child_to_parent)} child->parent relationships")
            for child, parent in sorted(child_to_parent.items()):
                log_time(f"    {child} -> {parent}")
            funder_lists = aggregate_funders_in_lists(funder_lists, child_to_parent)
            log_time("  Aggregation complete", agg_start)
        else:
            log_time("  No parent-child relationships found, skipping aggregation")

    # Build final output
    result = build_final_output(df, funder_lists)

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
