#!/usr/bin/env python3
"""
Funder Data Sharing Summary

Calculates data sharing rates for all canonical funders, filtered by article type.
Supports v3 funder aliases with parent-child aggregation.

For each funder, computes:
1. Total pubs acknowledging the funder (2010-2024) with allowed article types
2. Total pubs with data sharing
3. Percentage (ratio of the two)

Article type filter: Only includes research-article, brief-report, data-paper,
systematic-review, other, or blank. Excludes review-article, editorial, letter,
correction, retraction, news, etc.

Output: CSV with all funders having at least 1,000 pubs with data sharing.

Usage:
    python analysis/funder_data_sharing_summary.py \
        --oddpub-file ~/claude/pmcoaXMLs/oddpub_merged/oddpub_v7.2.3_all.parquet \
        --rtrans-dir ~/claude/pmcoaXMLs/rtrans_out_full_parquets \
        --registry hpc_scripts/pmcid_registry.duckdb \
        --funder-aliases funder_analysis/funder_aliases_v3.csv \
        --output results/funder_data_sharing_summary_v3.csv \
        --aggregate-children

Author: INCF 2025 Poster Analysis
Date: 2025-12-07
Updated: 2025-12-08 - Added v3 funder aliases with parent-child aggregation
"""

import argparse
import gc
import glob
import logging
import sys
from collections import defaultdict
from pathlib import Path

import duckdb
import pandas as pd

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from funder_analysis.normalize_funders import FunderNormalizer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Allowed article types for research analysis
ALLOWED_ARTICLE_TYPES = (
    'research-article',
    'brief-report',
    'data-paper',
    'systematic-review',
    'other',
)


def aggregate_children_to_parents(counts: dict, normalizer: FunderNormalizer) -> dict:
    """
    Aggregate child funder counts into parent funder totals.

    For funders with parent relationships (e.g., NIH institutes -> NIH, MRC -> UKRI),
    adds child counts to parent and removes child entries from output.

    Args:
        counts: Dict mapping funder name to count (int)
        normalizer: FunderNormalizer with parent relationship data

    Returns:
        Dict with aggregated counts (children removed, parents updated)
    """
    # Build child->parent mapping for funders in our data
    child_to_parent = {}
    for funder in counts.keys():
        parent = normalizer.get_parent(funder)
        if parent and parent in counts:
            child_to_parent[funder] = parent

    if not child_to_parent:
        logger.info("No parent-child relationships found in data, skipping aggregation")
        return counts

    logger.info(f"Aggregating {len(child_to_parent)} child funders into parents:")
    for child, parent in sorted(child_to_parent.items()):
        logger.info(f"  {child} -> {parent}")

    # Copy counts to avoid mutating input
    aggregated = dict(counts)

    # Add child counts to parents
    for child_funder, parent_funder in child_to_parent.items():
        aggregated[parent_funder] += aggregated[child_funder]

    # Remove child funders from output
    for child_funder in child_to_parent.keys():
        del aggregated[child_funder]

    logger.info(f"Reduced from {len(counts)} to {len(aggregated)} funders after aggregation")

    return aggregated


def normalize_pmcid(pmcid: str) -> str:
    """Normalize PMCID to PMC######### format."""
    if pd.isna(pmcid):
        return None
    pmcid = str(pmcid).strip().upper()
    if pmcid.startswith('PMCPMC'):
        pmcid = pmcid[3:]
    if pmcid.endswith('.TXT'):
        pmcid = pmcid[:-4]
    if not pmcid.startswith('PMC'):
        pmcid = 'PMC' + pmcid
    return pmcid


def count_funders_duckdb(rtrans_dir: Path,
                          oddpub_file: Path,
                          registry_path: Path,
                          normalizer: FunderNormalizer,
                          year_range: tuple = (2010, 2024),
                          limit: int = None) -> tuple:
    """
    Count funders using DuckDB for memory-efficient processing.

    Returns tuple of (corpus_counts, open_data_counts)
    """
    min_year, max_year = year_range
    all_funders = normalizer.get_all_canonical_names()

    logger.info(f"Searching for {len(all_funders)} canonical funders")
    logger.info(f"Year range: {min_year}-{max_year}")

    # Initialize counts
    corpus_counts = {funder: 0 for funder in all_funders}
    open_data_counts = {funder: 0 for funder in all_funders}

    # Create in-memory DuckDB for efficient joins
    con = duckdb.connect(':memory:')

    # Load article types from registry (using SQL for efficiency)
    logger.info(f"Loading article types from registry...")
    allowed_types_sql = ','.join(f"'{t}'" for t in ALLOWED_ARTICLE_TYPES)

    # Attach the registry DuckDB to load article types
    con.execute(f"ATTACH '{registry_path}' AS reg (READ_ONLY)")
    con.execute(f"""
        CREATE TABLE article_types AS
        SELECT pmcid, article_type
        FROM reg.pmcids
        WHERE article_type IN ({allowed_types_sql})
           OR article_type IS NULL
    """)
    con.execute("DETACH reg")

    count_result = con.execute("SELECT COUNT(*) FROM article_types").fetchone()
    logger.info(f"Loaded {count_result[0]:,} PMCIDs with allowed article types")

    # Load open data PMCIDs
    logger.info(f"Loading open data PMCIDs from {oddpub_file}")
    con.execute(f"""
        CREATE TABLE open_data_pmcids AS
        SELECT DISTINCT
            CASE
                WHEN pmcid IS NOT NULL THEN UPPER(TRIM(pmcid))
                WHEN article IS NOT NULL THEN
                    CASE
                        WHEN UPPER(article) LIKE 'PMCPMC%' THEN SUBSTR(UPPER(article), 4)
                        WHEN UPPER(article) LIKE 'PMC%' THEN UPPER(article)
                        ELSE 'PMC' || UPPER(article)
                    END
                ELSE NULL
            END as pmcid
        FROM read_parquet('{oddpub_file}')
        WHERE is_open_data = true
    """)
    open_data_count = con.execute("SELECT COUNT(*) FROM open_data_pmcids WHERE pmcid IS NOT NULL").fetchone()
    logger.info(f"Loaded {open_data_count[0]:,} open data PMCIDs")

    # Process rtrans files in batches
    parquet_files = sorted(glob.glob(f'{rtrans_dir}/*.parquet'))
    if limit:
        parquet_files = parquet_files[:limit]
        logger.info(f"Limited to first {limit} files for testing")

    logger.info(f"Processing {len(parquet_files)} rtrans parquet files")

    funding_cols = ['fund_text', 'fund_pmc_institute', 'fund_pmc_source', 'fund_pmc_anysource']

    total_corpus = 0
    total_open_data = 0

    for i, pf in enumerate(parquet_files):
        try:
            # Load rtrans file
            df = pd.read_parquet(pf)

            # Normalize PMCID
            if 'pmcid_pmc' in df.columns:
                df['pmcid_norm'] = df['pmcid_pmc'].apply(normalize_pmcid)
            elif 'pmcid' in df.columns:
                df['pmcid_norm'] = df['pmcid'].apply(normalize_pmcid)
            else:
                continue

            # Get year - prefer epub, fallback to ppub
            df['year'] = pd.Series([None] * len(df), dtype='float64')
            for yc in ['year_epub', 'year_ppub']:
                if yc in df.columns:
                    year_vals = pd.to_numeric(df[yc], errors='coerce')
                    df['year'] = df['year'].combine_first(year_vals)

            df = df[df['year'].notna()]
            df['year'] = df['year'].astype(int)
            df = df[(df['year'] >= min_year) & (df['year'] <= max_year)]

            if len(df) == 0:
                continue

            # Register DataFrame in DuckDB
            con.register('rtrans_batch', df)

            # Join with article_types (filter to allowed types)
            filtered_df = con.execute("""
                SELECT r.*
                FROM rtrans_batch r
                INNER JOIN article_types a ON r.pmcid_norm = a.pmcid
            """).fetchdf()

            if len(filtered_df) == 0:
                con.unregister('rtrans_batch')
                continue

            total_corpus += len(filtered_df)

            # Mark open data articles
            con.register('filtered_batch', filtered_df)
            with_open_data = con.execute("""
                SELECT f.*, (o.pmcid IS NOT NULL) as has_open_data
                FROM filtered_batch f
                LEFT JOIN open_data_pmcids o ON f.pmcid_norm = o.pmcid
            """).fetchdf()
            con.unregister('filtered_batch')
            con.unregister('rtrans_batch')

            total_open_data += with_open_data['has_open_data'].sum()

            # Get available funding columns
            available_cols = [c for c in funding_cols if c in with_open_data.columns]
            if not available_cols:
                continue

            # Combine all funding text
            with_open_data['combined_fund'] = ''
            for col in available_cols:
                with_open_data['combined_fund'] = with_open_data['combined_fund'] + ' ' + with_open_data[col].fillna('').astype(str)

            # For each canonical funder, count matches
            for funder in all_funders:
                pattern = normalizer.search_patterns.get(funder)
                if pattern:
                    matches = with_open_data['combined_fund'].str.contains(
                        pattern.pattern, case=False, na=False, regex=True
                    )
                    corpus_counts[funder] += matches.sum()
                    open_data_counts[funder] += (matches & with_open_data['has_open_data']).sum()

            del df, filtered_df, with_open_data
            gc.collect()

            if (i + 1) % 100 == 0:
                logger.info(f"  Processed {i+1}/{len(parquet_files)} files, corpus: {total_corpus:,}, open_data: {total_open_data:,}")

        except Exception as e:
            logger.warning(f"Error processing {Path(pf).name}: {e}")

    con.close()

    logger.info(f"Finished processing {len(parquet_files)} files")
    logger.info(f"Total corpus (filtered): {total_corpus:,}")
    logger.info(f"Total open data: {total_open_data:,}")

    return corpus_counts, open_data_counts


def main():
    parser = argparse.ArgumentParser(
        description='Calculate funder data sharing summary with article type filter',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--oddpub-file', type=Path, required=True,
                        help='Merged oddpub parquet file')
    parser.add_argument('--rtrans-dir', type=Path, required=True,
                        help='Directory containing rtrans parquet files')
    parser.add_argument('--registry', type=Path, required=True,
                        help='Path to pmcid_registry.duckdb')
    parser.add_argument('--output', type=Path, required=True,
                        help='Output CSV file')
    parser.add_argument('--year-range', type=int, nargs=2, metavar=('MIN', 'MAX'),
                        default=[2010, 2024],
                        help='Year range (default: 2010 2024)')
    parser.add_argument('--min-data-sharing', type=int, default=1000,
                        help='Minimum data sharing count to include funder (default: 1000)')
    parser.add_argument('--limit', type=int, default=None,
                        help='Limit number of rtrans files (for testing)')
    parser.add_argument('--funder-aliases', type=Path, default=None,
                        help='Path to funder_aliases CSV (default: funder_analysis/funder_aliases.csv)')
    parser.add_argument('--aggregate-children', action='store_true',
                        help='Aggregate child funder counts into parent totals (requires v3 aliases)')

    args = parser.parse_args()

    logger.info("=" * 70)
    logger.info("FUNDER DATA SHARING SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Year range: {args.year_range[0]}-{args.year_range[1]}")
    logger.info(f"Min data sharing count: {args.min_data_sharing:,}")
    logger.info("")
    logger.info("Allowed article types: research-article, brief-report, data-paper,")
    logger.info("                       systematic-review, other, blank")
    logger.info("=" * 70)

    # Initialize normalizer
    normalizer = FunderNormalizer(args.funder_aliases)
    logger.info(f"Loaded {len(normalizer.get_all_canonical_names())} canonical funders from {normalizer.aliases_csv}")

    # Count funders using DuckDB
    corpus_counts, open_data_counts = count_funders_duckdb(
        args.rtrans_dir,
        args.oddpub_file,
        args.registry,
        normalizer,
        tuple(args.year_range),
        args.limit
    )

    # Aggregate children into parents if requested
    if args.aggregate_children:
        logger.info("Aggregating child funders into parent totals...")
        corpus_counts = aggregate_children_to_parents(corpus_counts, normalizer)
        open_data_counts = aggregate_children_to_parents(open_data_counts, normalizer)

    # Build results dataframe
    results = []
    funders_to_process = corpus_counts.keys()
    for funder in funders_to_process:
        total = corpus_counts[funder]
        data_sharing = open_data_counts[funder]
        pct = (data_sharing / total * 100) if total > 0 else 0.0

        results.append({
            'funder': funder,
            'total_pubs': total,
            'data_sharing_pubs': data_sharing,
            'data_sharing_pct': round(pct, 2)
        })

    df = pd.DataFrame(results)

    # Filter to funders with >= min_data_sharing
    df_filtered = df[df['data_sharing_pubs'] >= args.min_data_sharing].copy()
    df_filtered = df_filtered.sort_values('data_sharing_pubs', ascending=False)

    # Save output
    args.output.parent.mkdir(parents=True, exist_ok=True)
    df_filtered.to_csv(args.output, index=False)
    logger.info(f"Saved {len(df_filtered)} funders to {args.output}")

    # Also save full results (all funders)
    full_output = args.output.with_name(args.output.stem + '_all.csv')
    df_sorted = df.sort_values('data_sharing_pubs', ascending=False)
    df_sorted.to_csv(full_output, index=False)
    logger.info(f"Saved all {len(df_sorted)} funders to {full_output}")

    # Print summary
    print("\n" + "=" * 70)
    print(f"SUMMARY: Funders with >= {args.min_data_sharing:,} Data Sharing Pubs")
    print("=" * 70)
    print(f"{'Funder':<50} {'Total':>10} {'DataShare':>10} {'%':>8}")
    print("-" * 78)
    for _, row in df_filtered.head(20).iterrows():
        print(f"{row['funder']:<50} {row['total_pubs']:>10,} {row['data_sharing_pubs']:>10,} {row['data_sharing_pct']:>7.1f}%")

    if len(df_filtered) > 20:
        print(f"... and {len(df_filtered) - 20} more funders")

    print("-" * 78)
    print(f"Total funders meeting threshold: {len(df_filtered)}")


if __name__ == '__main__':
    main()
