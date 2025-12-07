#!/usr/bin/env python3
"""
OpenSS Funder Trends Analysis (v3)

Creates line graphs showing open data sharing trends by top canonical funders over time.
Uses the funder_aliases_v3.csv (57 funders) for matching with all variants.

Key updates in v3:
- Supports parent-child funder aggregation (e.g., NIH institutes -> NIH)
- Uses parent_funder column from funder_aliases_v3.csv
- Add --aggregate-children to roll up child counts into parent totals

Key updates in v2:
- Uses all 57 canonical funders from funder_aliases_v2.csv
- Filters to research article types only (research-article, brief-report, data-paper,
  systematic-review, other, blank) - excludes review-article, editorial, letter, etc.
- CSV output includes all 57 funders; graphs show top 10 only
- Requires pmcid_registry.duckdb for article type filtering

This script produces two types of graphs:
1. Absolute counts: Open data articles by funder per year
2. Percentages: Open data % by funder per year (requires corpus totals scan)

Both graphs use consistent colors for top 10 funders.

Usage:
    # Generate both graphs with v3 funder list and aggregation
    python analysis/openss_funder_trends.py \
        --oddpub-file ~/claude/pmcoaXMLs/oddpub_merged/oddpub_v7.2.3_all.parquet \
        --rtrans-dir ~/claude/pmcoaXMLs/rtrans_out_full_parquets \
        --registry hpc_scripts/pmcid_registry.duckdb \
        --funder-aliases funder_analysis/funder_aliases_v3.csv \
        --aggregate-children \
        --output-dir results/openss_funder_trends_v5 \
        --graph both

Author: INCF 2025 Poster Analysis
Date: 2025-12-07
Updated: 2025-12-07 - Added v3 parent-child aggregation support
"""

import argparse
import gc
import glob
import logging
import sys
from collections import defaultdict
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
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

# Allowed article types for research analysis (same as funder_data_sharing_summary.py)
ALLOWED_ARTICLE_TYPES = (
    'research-article',
    'brief-report',
    'data-paper',
    'systematic-review',
    'other',
)

# Top 10 funders for graph display (selected dynamically based on data)
# These are for fallback display names and colors
TOP_10_DISPLAY_NAMES = {
    'National Institutes of Health': 'NIH (USA)',
    'National Natural Science Foundation of China': 'NSFC (China)',
    'National Science Foundation': 'NSF (USA)',
    'European Commission': 'EC (Europe)',
    'Howard Hughes Medical Institute': 'HHMI (USA)',
    'Biotechnology and Biological Sciences Research Council': 'BBSRC (UK)',
    'European Research Council': 'ERC (Europe)',
    'Wellcome Trust': 'Wellcome (UK)',
    'Deutsche Forschungsgemeinschaft': 'DFG (Germany)',
    'Agence Nationale de la Recherche': 'ANR (France)',
    'Medical Research Council': 'MRC (UK)',
    'Japan Society for the Promotion of Science': 'JSPS (Japan)',
    'National Research Foundation of Korea': 'NRF (Korea)',
    'Swiss National Science Foundation': 'SNSF (Switzerland)',
    'Austrian Science Fund': 'FWF (Austria)',
}

# Fixed color mapping for consistency (10 colors for top 10 funders)
FUNDER_COLORS = [
    '#1f77b4',  # Blue
    '#d62728',  # Red
    '#2ca02c',  # Green
    '#ff7f0e',  # Orange
    '#9467bd',  # Purple
    '#8c564b',  # Brown
    '#e377c2',  # Pink
    '#7f7f7f',  # Gray
    '#bcbd22',  # Olive
    '#17becf',  # Cyan
]


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


def load_article_types(registry_path: Path) -> dict:
    """Load article types from pmcid_registry.duckdb."""
    logger.info(f"Loading article types from {registry_path}")
    con = duckdb.connect(str(registry_path), read_only=True)

    # Check which table exists (local uses pmcid_registry, HPC uses pmcids)
    tables = con.execute("SHOW TABLES").fetchall()
    table_names = [t[0] for t in tables]

    if 'pmcid_registry' in table_names:
        table_name = 'pmcid_registry'
    elif 'pmcids' in table_names:
        table_name = 'pmcids'
    else:
        raise ValueError(f"No recognized table found in registry. Available: {table_names}")

    logger.info(f"Using table: {table_name}")

    result = con.execute(f"""
        SELECT pmcid, article_type
        FROM {table_name}
        WHERE article_type IS NOT NULL
    """).fetchall()

    article_types = {row[0]: row[1] for row in result}
    con.close()

    logger.info(f"Loaded {len(article_types):,} article types")
    return article_types


def is_allowed_article_type(article_type: str) -> bool:
    """Check if article type is allowed for research analysis."""
    if pd.isna(article_type) or article_type == '':
        return True  # Blank is allowed
    return article_type.lower() in [t.lower() for t in ALLOWED_ARTICLE_TYPES]


def load_open_data_pmcids(oddpub_file: Path, article_types: dict) -> set:
    """Load PMCIDs of research articles with open data detected by oddpub."""
    logger.info(f"Loading open data PMCIDs from {oddpub_file}")
    df = pd.read_parquet(oddpub_file)
    logger.info(f"Loaded {len(df):,} records")

    open_data_df = df[df['is_open_data'] == True]
    logger.info(f"Found {len(open_data_df):,} with is_open_data=true ({100*len(open_data_df)/len(df):.2f}%)")

    pmcids = set()
    filtered_out = 0

    for col in ['article', 'pmcid', 'filename']:
        if col in open_data_df.columns:
            for val in open_data_df[col].dropna().unique():
                pmcid = normalize_pmcid(val)
                if pmcid:
                    # Filter by article type
                    art_type = article_types.get(pmcid)
                    if is_allowed_article_type(art_type):
                        pmcids.add(pmcid)
                    else:
                        filtered_out += 1

    logger.info(f"Extracted {len(pmcids):,} unique research article PMCIDs with open data")
    logger.info(f"Filtered out {filtered_out:,} non-research articles (review, editorial, letter, etc.)")
    return pmcids


def count_funders_by_year(rtrans_dir: Path,
                          normalizer: FunderNormalizer,
                          open_data_pmcids: set,
                          article_types: dict,
                          limit: int = None) -> dict:
    """
    Count all canonical funders in open data research articles by year.

    Returns dict with counts[funder][year] = count
    """
    parquet_files = sorted(glob.glob(f'{rtrans_dir}/*.parquet'))

    if limit:
        parquet_files = parquet_files[:limit]
        logger.info(f"Limited to first {limit} files for testing")

    all_funders = normalizer.get_all_canonical_names()
    logger.info(f"Processing {len(parquet_files)} rtrans parquet files")
    logger.info(f"Searching for {len(all_funders)} canonical funders in {len(open_data_pmcids):,} open data research articles")

    # Initialize counts: funder -> year -> count
    counts = {funder: defaultdict(int) for funder in all_funders}
    total_matched = 0

    funding_cols = ['fund_text', 'fund_pmc_institute', 'fund_pmc_source', 'fund_pmc_anysource']
    year_cols = ['year_epub', 'year_ppub']

    for i, pf in enumerate(parquet_files):
        try:
            df = pd.read_parquet(pf)

            # Normalize PMCID for matching
            if 'pmcid_pmc' in df.columns:
                df['pmcid_norm'] = df['pmcid_pmc'].apply(normalize_pmcid)
            elif 'pmcid' in df.columns:
                df['pmcid_norm'] = df['pmcid'].apply(normalize_pmcid)
            else:
                continue

            # Filter to open data research articles only
            df = df[df['pmcid_norm'].isin(open_data_pmcids)]

            if len(df) == 0:
                continue

            total_matched += len(df)

            # Get year - prefer epub, fallback to ppub
            df['year'] = None
            for yc in year_cols:
                if yc in df.columns:
                    df['year'] = df['year'].fillna(pd.to_numeric(df[yc], errors='coerce'))

            # Skip records without year
            df = df[df['year'].notna()]
            df['year'] = df['year'].astype(int)

            # Filter to reasonable year range (2000-2025)
            df = df[(df['year'] >= 2000) & (df['year'] <= 2025)]

            if len(df) == 0:
                continue

            # Get available funding columns
            available_cols = [c for c in funding_cols if c in df.columns]
            if not available_cols:
                continue

            # Combine all funding text
            df['combined_fund'] = ''
            for col in available_cols:
                df['combined_fund'] = df['combined_fund'] + ' ' + df[col].fillna('').astype(str)

            # For each canonical funder, count matches by year
            for funder in all_funders:
                pattern = normalizer.search_patterns.get(funder)
                if pattern:
                    matches = df['combined_fund'].str.contains(
                        pattern.pattern, case=False, na=False, regex=True
                    )
                    # Group by year and count
                    matched_df = df[matches]
                    for year, count in matched_df.groupby('year').size().items():
                        counts[funder][year] += count

            del df
            gc.collect()

            if (i + 1) % 100 == 0:
                logger.info(f"  Processed {i+1}/{len(parquet_files)} files, {total_matched:,} open data research articles matched")

        except Exception as e:
            logger.warning(f"Error processing {Path(pf).name}: {e}")

    logger.info(f"Finished processing {len(parquet_files)} files")
    logger.info(f"Total open data research articles matched: {total_matched:,}")

    return counts


def load_corpus_totals_by_year(rtrans_dir: Path,
                                normalizer: FunderNormalizer,
                                article_types: dict,
                                limit: int = None) -> dict:
    """
    Compute corpus totals by year for each funder (research articles only).

    Returns dict with totals[funder][year] = count
    """
    parquet_files = sorted(glob.glob(f'{rtrans_dir}/*.parquet'))

    if limit:
        parquet_files = parquet_files[:limit]

    all_funders = normalizer.get_all_canonical_names()
    logger.info(f"Computing corpus totals by year from {len(parquet_files)} files (research articles only)")

    totals = {funder: defaultdict(int) for funder in all_funders}
    funding_cols = ['fund_text', 'fund_pmc_institute', 'fund_pmc_source', 'fund_pmc_anysource']
    year_cols = ['year_epub', 'year_ppub']
    total_research = 0
    total_filtered = 0

    for i, pf in enumerate(parquet_files):
        try:
            df = pd.read_parquet(pf)

            # Normalize PMCID
            if 'pmcid_pmc' in df.columns:
                df['pmcid_norm'] = df['pmcid_pmc'].apply(normalize_pmcid)
            elif 'pmcid' in df.columns:
                df['pmcid_norm'] = df['pmcid'].apply(normalize_pmcid)
            else:
                continue

            # Filter to research articles only
            original_len = len(df)
            df = df[df['pmcid_norm'].apply(lambda x: is_allowed_article_type(article_types.get(x)))]
            total_filtered += original_len - len(df)
            total_research += len(df)

            if len(df) == 0:
                continue

            # Get year
            df['year'] = None
            for yc in year_cols:
                if yc in df.columns:
                    df['year'] = df['year'].fillna(pd.to_numeric(df[yc], errors='coerce'))

            df = df[df['year'].notna()]
            df['year'] = df['year'].astype(int)
            df = df[(df['year'] >= 2000) & (df['year'] <= 2025)]

            if len(df) == 0:
                continue

            available_cols = [c for c in funding_cols if c in df.columns]
            if not available_cols:
                continue

            df['combined_fund'] = ''
            for col in available_cols:
                df['combined_fund'] = df['combined_fund'] + ' ' + df[col].fillna('').astype(str)

            for funder in all_funders:
                pattern = normalizer.search_patterns.get(funder)
                if pattern:
                    matches = df['combined_fund'].str.contains(
                        pattern.pattern, case=False, na=False, regex=True
                    )
                    matched_df = df[matches]
                    for year, count in matched_df.groupby('year').size().items():
                        totals[funder][year] += count

            del df
            gc.collect()

            if (i + 1) % 100 == 0:
                logger.info(f"  Processed {i+1}/{len(parquet_files)} files for corpus totals ({total_research:,} research articles)")

        except Exception as e:
            logger.warning(f"Error processing {Path(pf).name}: {e}")

    logger.info(f"Total research articles: {total_research:,} (filtered out {total_filtered:,} non-research)")
    return totals


def get_display_name(funder: str, normalizer: FunderNormalizer) -> str:
    """Get display name with country for a funder."""
    if funder in TOP_10_DISPLAY_NAMES:
        return TOP_10_DISPLAY_NAMES[funder]

    # Try to get country from aliases file
    country = normalizer.get_country(funder) if hasattr(normalizer, 'get_country') else None
    if country and country != 'Unknown':
        # Create abbreviated name if too long
        if len(funder) > 30:
            # Try to use acronym
            acronym = normalizer.get_acronym(funder) if hasattr(normalizer, 'get_acronym') else None
            if acronym:
                return f"{acronym} ({country})"
        return f"{funder} ({country})"
    return funder


def aggregate_children_to_parents(counts: dict, normalizer: FunderNormalizer) -> dict:
    """
    Aggregate child funder counts into parent funder totals.

    Args:
        counts: Dictionary of {funder: {year: count}}
        normalizer: FunderNormalizer with parent-child relationships

    Returns:
        Dictionary with child counts rolled into parent totals,
        child funders removed from output
    """
    # Build child-to-parent mapping
    child_to_parent = {}
    for funder in counts.keys():
        parent = normalizer.get_parent(funder)
        if parent and parent in counts:
            child_to_parent[funder] = parent

    if not child_to_parent:
        logger.info("No parent-child relationships found in data, skipping aggregation")
        return counts

    logger.info(f"Found {len(child_to_parent)} child->parent mappings for aggregation:")
    for child, parent in sorted(child_to_parent.items()):
        logger.info(f"  {child} -> {parent}")

    # Copy counts to avoid mutating input
    aggregated = {funder: dict(year_counts) for funder, year_counts in counts.items()}

    # Add child counts to parents
    for child_funder, parent_funder in child_to_parent.items():
        for year, count in aggregated[child_funder].items():
            if year not in aggregated[parent_funder]:
                aggregated[parent_funder][year] = 0
            aggregated[parent_funder][year] += count

    # Remove child funders from output
    for child_funder in child_to_parent.keys():
        del aggregated[child_funder]

    logger.info(f"After aggregation: {len(aggregated)} funders (was {len(counts)})")
    return aggregated


def create_counts_plot(counts: dict, output_dir: Path, year_range: tuple, normalizer: FunderNormalizer):
    """Create line graph of absolute counts by year (top 10 in graph, all in CSV)."""
    # Convert to DataFrame with all funders
    data = {}
    for funder, year_counts in counts.items():
        display_name = get_display_name(funder, normalizer)
        data[display_name] = year_counts

    df = pd.DataFrame(data).T
    df = df.fillna(0).astype(int)

    # Filter year range
    if year_range:
        min_year, max_year = year_range
        df = df[[c for c in df.columns if min_year <= c <= max_year]]

    # Sort columns (years)
    df = df[sorted(df.columns)]

    # Sort rows by total (descending)
    df['total'] = df.sum(axis=1)
    df = df.sort_values('total', ascending=False)

    # Save CSV with ALL funders
    csv_df = df.copy()
    csv_df.to_csv(output_dir / 'openss_funder_counts_by_year.csv')
    logger.info(f"Saved counts CSV with {len(csv_df)} funders")

    # Keep only top 10 for plotting
    df = df.head(10)
    df = df.drop('total', axis=1)

    # Create plot
    fig, ax = plt.subplots(figsize=(12, 8))

    for i, funder in enumerate(df.index):
        years = list(df.columns)
        values = list(df.loc[funder].values)
        color = FUNDER_COLORS[i % len(FUNDER_COLORS)]
        ax.plot(years, values, label=funder, linewidth=2.5, marker='o', markersize=4, color=color)

    ax.set_xlabel('Year', fontsize=14)
    ax.set_ylabel('Number of Open Data Research Articles', fontsize=14)
    ax.set_title('Open Data Research Articles by Top 10 Funders (2010-2024)', fontsize=16)
    ax.legend(title='Funder', bbox_to_anchor=(1.02, 1), loc='upper left', frameon=True, fontsize=10)
    ax.grid(True, alpha=0.3)
    ax.set_ylim(bottom=0)

    # Format x-axis
    years = sorted(df.columns)
    ax.set_xticks(years)
    ax.set_xticklabels(years, rotation=45, ha='right')

    plt.tight_layout()
    png_path = output_dir / 'openss_funder_counts_by_year.png'
    plt.savefig(png_path, dpi=300, bbox_inches='tight')
    logger.info(f"Saved counts plot to {png_path}")
    plt.close()


def create_percentages_plot(counts: dict, totals: dict, output_dir: Path, year_range: tuple, normalizer: FunderNormalizer):
    """Create line graph of percentages by year (top 10 in graph, all in CSV)."""
    # Calculate percentages for all funders
    percentages = {}
    for funder in counts.keys():
        display_name = get_display_name(funder, normalizer)
        percentages[display_name] = {}
        for year in counts[funder].keys():
            total = totals[funder].get(year, 0)
            if total > 0:
                percentages[display_name][year] = (counts[funder][year] / total) * 100
            else:
                percentages[display_name][year] = 0

    df = pd.DataFrame(percentages).T
    df = df.fillna(0)

    # Filter year range
    if year_range:
        min_year, max_year = year_range
        df = df[[c for c in df.columns if min_year <= c <= max_year]]

    # Sort columns (years)
    df = df[sorted(df.columns)]

    # Sort rows by final year value (descending)
    if len(df.columns) > 0:
        final_year = df.columns[-1]
        df = df.sort_values(final_year, ascending=False)

    # Save CSV with ALL funders
    df.to_csv(output_dir / 'openss_funder_percentages_by_year.csv')
    logger.info(f"Saved percentages CSV with {len(df)} funders")

    # Keep only top 10 for plotting
    df = df.head(10)

    # Create plot
    fig, ax = plt.subplots(figsize=(12, 8))

    for i, funder in enumerate(df.index):
        years = list(df.columns)
        values = list(df.loc[funder].values)
        color = FUNDER_COLORS[i % len(FUNDER_COLORS)]
        ax.plot(years, values, label=funder, linewidth=2.5, marker='o', markersize=4, color=color)

    ax.set_xlabel('Year', fontsize=14)
    ax.set_ylabel('Open Data Rate (%)', fontsize=14)
    ax.set_title('Open Data Rate by Top 10 Funders - Research Articles Only (2010-2024)', fontsize=16)
    ax.legend(title='Funder', bbox_to_anchor=(1.02, 1), loc='upper left', frameon=True, fontsize=10)
    ax.grid(True, alpha=0.3)
    ax.set_ylim(bottom=0)

    # Format x-axis
    years = sorted(df.columns)
    ax.set_xticks(years)
    ax.set_xticklabels(years, rotation=45, ha='right')

    plt.tight_layout()
    png_path = output_dir / 'openss_funder_percentages_by_year.png'
    plt.savefig(png_path, dpi=300, bbox_inches='tight')
    logger.info(f"Saved percentages plot to {png_path}")
    plt.close()


def main():
    parser = argparse.ArgumentParser(
        description='Create line graphs of open data trends by funder (v2: all 57 funders, research articles only)',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--oddpub-file', type=Path, required=True,
                        help='Merged oddpub parquet file')
    parser.add_argument('--rtrans-dir', type=Path, required=True,
                        help='Directory containing rtrans parquet files')
    parser.add_argument('--registry', type=Path, required=True,
                        help='Path to pmcid_registry.duckdb for article type filtering')
    parser.add_argument('--funder-aliases', type=Path, default=None,
                        help='Path to funder_aliases CSV (default: funder_analysis/funder_aliases.csv)')
    parser.add_argument('--output-dir', type=Path, required=True,
                        help='Output directory for graphs and CSVs')
    parser.add_argument('--graph', choices=['counts', 'percentages', 'both'], default='both',
                        help='Which graph(s) to generate (default: both)')
    parser.add_argument('--year-range', type=int, nargs=2, metavar=('MIN', 'MAX'),
                        default=[2010, 2024],
                        help='Year range for plots (default: 2010 2024)')
    parser.add_argument('--limit', type=int, default=None,
                        help='Limit number of rtrans files (for testing)')
    parser.add_argument('--aggregate-children', action='store_true',
                        help='Aggregate child funder counts into parent totals (e.g., NIH institutes -> NIH)')

    args = parser.parse_args()

    logger.info("=" * 70)
    logger.info("OPENSS FUNDER TRENDS ANALYSIS (v3)")
    logger.info("=" * 70)
    logger.info("Using all canonical funders from aliases file")
    logger.info("Filtering to research articles only")
    logger.info(f"Year range: {args.year_range[0]}-{args.year_range[1]}")
    if args.aggregate_children:
        logger.info("Parent-child aggregation: ENABLED")
    logger.info("=" * 70)

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Load article types from registry
    article_types = load_article_types(args.registry)

    # Load open data PMCIDs (filtered to research articles)
    open_data_pmcids = load_open_data_pmcids(args.oddpub_file, article_types)

    # Initialize normalizer with specified aliases file
    normalizer = FunderNormalizer(args.funder_aliases)
    all_funders = normalizer.get_all_canonical_names()
    logger.info(f"Loaded {len(all_funders)} canonical funders from {normalizer.aliases_csv}")

    # Count all funders by year in open data research articles
    counts = count_funders_by_year(
        args.rtrans_dir, normalizer, open_data_pmcids, article_types, args.limit
    )

    # Aggregate children to parents if requested
    if args.aggregate_children:
        logger.info("Aggregating child funder counts to parent funders...")
        counts = aggregate_children_to_parents(counts, normalizer)

    # Generate counts graph
    if args.graph in ['counts', 'both']:
        logger.info("Generating counts graph...")
        create_counts_plot(counts, args.output_dir, tuple(args.year_range), normalizer)

    # Generate percentages graph
    if args.graph in ['percentages', 'both']:
        logger.info("Computing corpus totals for percentages (research articles only)...")
        totals = load_corpus_totals_by_year(
            args.rtrans_dir, normalizer, article_types, args.limit
        )
        # Aggregate totals too if aggregating children
        if args.aggregate_children:
            logger.info("Aggregating corpus totals to parent funders...")
            totals = aggregate_children_to_parents(totals, normalizer)
        create_percentages_plot(counts, totals, args.output_dir, tuple(args.year_range), normalizer)

    # Print summary
    print("\n" + "=" * 70)
    print("SUMMARY: Top 20 Funders by Open Data Research Article Counts")
    print("=" * 70)

    # Calculate totals and sort
    funder_totals = [(f, sum(counts[f].values())) for f in counts.keys()]
    funder_totals.sort(key=lambda x: x[1], reverse=True)

    for funder, total in funder_totals[:20]:
        display = get_display_name(funder, normalizer)
        print(f"  {display:<45} {total:>10,}")

    logger.info(f"Output saved to {args.output_dir}")


if __name__ == '__main__':
    main()
