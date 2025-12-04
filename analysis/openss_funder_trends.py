#!/usr/bin/env python3
"""
OpenSS Funder Trends Analysis

Creates line graphs showing open data sharing trends by top canonical funders over time.
Uses the funder alias mapping to match funders by canonical name with all variants.

This script produces two types of graphs:
1. Absolute counts: Open data articles by funder per year (can run immediately)
2. Percentages: Open data % by funder per year (requires corpus totals)

Both graphs use consistent colors for each funder.

Usage:
    # Generate counts graph (immediate)
    python analysis/openss_funder_trends.py \
        --oddpub-file ~/claude/pmcoaXMLs/oddpub_merged/oddpub_v7.2.3_all.parquet \
        --rtrans-dir ~/claude/pmcoaXMLs/rtrans_out_full_parquets \
        --output-dir results/openss_funder_trends \
        --graph counts

    # Generate percentages graph (requires corpus totals)
    python analysis/openss_funder_trends.py \
        --oddpub-file ~/claude/pmcoaXMLs/oddpub_merged/oddpub_v7.2.3_all.parquet \
        --rtrans-dir ~/claude/pmcoaXMLs/rtrans_out_full_parquets \
        --corpus-totals results/canonical_funder_corpus_totals.parquet \
        --output-dir results/openss_funder_trends \
        --graph percentages

Author: INCF 2025 Poster Analysis
Date: 2025-12-02
"""

import argparse
import gc
import glob
import logging
import sys
from collections import defaultdict
from pathlib import Path

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

# Top 10 funders selected from the union of:
# - Top 10 by absolute counts (2024): NIH, NSFC, NSF, EC, DFG, Wellcome, MRC, ERC, JSPS, ANR
# - Top 10 by percentage (2024): HHMI, BBSRC, ANR, ERC, FWF, Wellcome, DFG, SNSF, NSF, NIH
# Selected to provide geographic diversity and show interesting trends
TOP_FUNDERS = [
    'National Institutes of Health',           # Top by counts, large US funder
    'National Natural Science Foundation of China',  # Top by counts, China
    'National Science Foundation',             # Top in both
    'European Commission',                     # Top by counts, Europe
    'Howard Hughes Medical Institute',         # Top by percentage
    'Biotechnology and Biological Sciences Research Council',  # Top by percentage
    'European Research Council',               # Top in both
    'Wellcome Trust',                          # Top in both
    'Deutsche Forschungsgemeinschaft',         # Top in both
    'Agence Nationale de la Recherche',        # Top in both
]

# Display names with country/region
FUNDER_DISPLAY_NAMES = {
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
}

# Fixed color mapping for consistency across both graphs
# Using a colorblind-friendly palette
FUNDER_COLORS = {
    'NIH (USA)': '#1f77b4',       # Blue
    'NSFC (China)': '#d62728',    # Red
    'NSF (USA)': '#2ca02c',       # Green
    'EC (Europe)': '#ff7f0e',     # Orange
    'HHMI (USA)': '#9467bd',      # Purple
    'BBSRC (UK)': '#8c564b',      # Brown
    'ERC (Europe)': '#e377c2',    # Pink
    'Wellcome (UK)': '#7f7f7f',   # Gray
    'DFG (Germany)': '#bcbd22',   # Olive
    'ANR (France)': '#17becf',    # Cyan
}


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


def load_open_data_pmcids(oddpub_file: Path) -> set:
    """Load PMCIDs of articles with open data detected by oddpub."""
    logger.info(f"Loading open data PMCIDs from {oddpub_file}")
    df = pd.read_parquet(oddpub_file)
    logger.info(f"Loaded {len(df):,} records")

    open_data_df = df[df['is_open_data'] == True]
    logger.info(f"Found {len(open_data_df):,} with is_open_data=true ({100*len(open_data_df)/len(df):.2f}%)")

    pmcids = set()
    for col in ['article', 'pmcid', 'filename']:
        if col in open_data_df.columns:
            for val in open_data_df[col].dropna().unique():
                pmcid = normalize_pmcid(val)
                if pmcid:
                    pmcids.add(pmcid)

    logger.info(f"Extracted {len(pmcids):,} unique PMCIDs with open data")
    return pmcids


def count_funders_by_year(rtrans_dir: Path,
                          normalizer: FunderNormalizer,
                          open_data_pmcids: set,
                          funders: list,
                          limit: int = None) -> dict:
    """
    Count canonical funders in open data articles by year.

    Returns dict with counts[funder][year] = count
    """
    parquet_files = sorted(glob.glob(f'{rtrans_dir}/*.parquet'))

    if limit:
        parquet_files = parquet_files[:limit]
        logger.info(f"Limited to first {limit} files for testing")

    logger.info(f"Processing {len(parquet_files)} rtrans parquet files")
    logger.info(f"Searching for {len(funders)} canonical funders in {len(open_data_pmcids):,} open data articles")

    # Initialize counts: funder -> year -> count
    counts = {funder: defaultdict(int) for funder in funders}
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

            # Filter to open data articles only
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
            for funder in funders:
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
                logger.info(f"  Processed {i+1}/{len(parquet_files)} files, {total_matched:,} open data articles matched")

        except Exception as e:
            logger.warning(f"Error processing {Path(pf).name}: {e}")

    logger.info(f"Finished processing {len(parquet_files)} files")
    logger.info(f"Total open data articles matched: {total_matched:,}")

    return counts


def load_corpus_totals_by_year(corpus_totals_file: Path, rtrans_dir: Path,
                                normalizer: FunderNormalizer, funders: list,
                                limit: int = None) -> dict:
    """
    Load or compute corpus totals by year for each funder.

    If corpus_totals_file exists, we need to recompute by year since the
    existing file only has overall totals.

    Returns dict with totals[funder][year] = count
    """
    # For now, we need to compute this by scanning rtrans files
    # Similar to count_funders_by_year but for ALL articles (not just open data)

    parquet_files = sorted(glob.glob(f'{rtrans_dir}/*.parquet'))

    if limit:
        parquet_files = parquet_files[:limit]

    logger.info(f"Computing corpus totals by year from {len(parquet_files)} files")

    totals = {funder: defaultdict(int) for funder in funders}
    funding_cols = ['fund_text', 'fund_pmc_institute', 'fund_pmc_source', 'fund_pmc_anysource']
    year_cols = ['year_epub', 'year_ppub']

    for i, pf in enumerate(parquet_files):
        try:
            df = pd.read_parquet(pf)

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

            for funder in funders:
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
                logger.info(f"  Processed {i+1}/{len(parquet_files)} files for corpus totals")

        except Exception as e:
            logger.warning(f"Error processing {Path(pf).name}: {e}")

    return totals


def create_counts_plot(counts: dict, output_dir: Path, year_range: tuple = None):
    """Create line graph of absolute counts by year."""
    # Convert to DataFrame
    data = {}
    for funder, year_counts in counts.items():
        display_name = FUNDER_DISPLAY_NAMES.get(funder, funder)
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
    df = df.drop('total', axis=1)

    # Save CSV
    csv_path = output_dir / 'openss_funder_counts_by_year.csv'
    df.to_csv(csv_path)
    logger.info(f"Saved counts CSV to {csv_path}")

    # Create plot
    fig, ax = plt.subplots(figsize=(12, 8))

    for funder in df.index:
        years = list(df.columns)
        values = list(df.loc[funder].values)
        color = FUNDER_COLORS.get(funder, '#333333')
        ax.plot(years, values, label=funder, linewidth=2.5, marker='o', markersize=4, color=color)

    ax.set_xlabel('Year', fontsize=14)
    ax.set_ylabel('Number of Open Data Articles', fontsize=14)
    ax.set_title('Open Data Articles by Top 10 Major Funders (2010-2024)', fontsize=16)
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


def create_percentages_plot(counts: dict, totals: dict, output_dir: Path, year_range: tuple = None):
    """Create line graph of percentages by year."""
    # Calculate percentages
    percentages = {}
    for funder in counts.keys():
        display_name = FUNDER_DISPLAY_NAMES.get(funder, funder)
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

    # Save CSV
    csv_path = output_dir / 'openss_funder_percentages_by_year.csv'
    df.to_csv(csv_path)
    logger.info(f"Saved percentages CSV to {csv_path}")

    # Create plot
    fig, ax = plt.subplots(figsize=(12, 8))

    for funder in df.index:
        years = list(df.columns)
        values = list(df.loc[funder].values)
        color = FUNDER_COLORS.get(funder, '#333333')
        ax.plot(years, values, label=funder, linewidth=2.5, marker='o', markersize=4, color=color)

    ax.set_xlabel('Year', fontsize=14)
    ax.set_ylabel('Open Data Rate (%)', fontsize=14)
    ax.set_title('Open Data Rate by Top 10 Major Funders (2010-2024)', fontsize=16)
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
        description='Create line graphs of open data trends by funder',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--oddpub-file', type=Path, required=True,
                        help='Merged oddpub parquet file')
    parser.add_argument('--rtrans-dir', type=Path, required=True,
                        help='Directory containing rtrans parquet files')
    parser.add_argument('--corpus-totals', type=Path, default=None,
                        help='Corpus totals parquet file (for percentages graph)')
    parser.add_argument('--output-dir', type=Path, required=True,
                        help='Output directory for graphs and CSVs')
    parser.add_argument('--graph', choices=['counts', 'percentages', 'both'], default='counts',
                        help='Which graph(s) to generate')
    parser.add_argument('--year-range', type=int, nargs=2, metavar=('MIN', 'MAX'),
                        default=[2010, 2024],
                        help='Year range for plots (default: 2010 2024)')
    parser.add_argument('--limit', type=int, default=None,
                        help='Limit number of rtrans files (for testing)')

    args = parser.parse_args()

    logger.info("=" * 70)
    logger.info("OPENSS FUNDER TRENDS ANALYSIS")
    logger.info("=" * 70)

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Load open data PMCIDs
    open_data_pmcids = load_open_data_pmcids(args.oddpub_file)

    # Initialize normalizer
    normalizer = FunderNormalizer()
    logger.info(f"Loaded {len(normalizer.get_all_canonical_names())} canonical funders with alias mapping")

    # Count funders by year in open data articles
    counts = count_funders_by_year(
        args.rtrans_dir, normalizer, open_data_pmcids, TOP_FUNDERS, args.limit
    )

    # Generate counts graph
    if args.graph in ['counts', 'both']:
        logger.info("Generating counts graph...")
        create_counts_plot(counts, args.output_dir, tuple(args.year_range))

    # Generate percentages graph (requires corpus totals)
    if args.graph in ['percentages', 'both']:
        if args.corpus_totals and args.corpus_totals.exists():
            logger.info("Loading corpus totals for percentages...")
            # Need to compute totals by year (existing file only has overall totals)
            totals = load_corpus_totals_by_year(
                args.corpus_totals, args.rtrans_dir, normalizer, TOP_FUNDERS, args.limit
            )
            create_percentages_plot(counts, totals, args.output_dir, tuple(args.year_range))
        else:
            logger.warning("Corpus totals file not available. Skipping percentages graph.")
            logger.warning("Run with --graph counts first, then rerun with --corpus-totals when available.")

    # Print summary
    print("\n" + "=" * 70)
    print("SUMMARY: Top 10 Funders Open Data Counts by Year")
    print("=" * 70)
    for funder in TOP_FUNDERS:
        total = sum(counts[funder].values())
        display = FUNDER_DISPLAY_NAMES.get(funder, funder)
        print(f"  {display:<30} {total:>10,}")


if __name__ == '__main__':
    main()
