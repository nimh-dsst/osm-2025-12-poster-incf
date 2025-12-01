#!/usr/bin/env python3
"""
Compare oddpub v5 (via rtransparent) vs v7.2.3 Detection Rates

This script compares open data detection between the two oddpub versions
and creates visualizations showing:
1. Overall detection rate differences
2. Detection rate differences by funder
3. Year-over-year trends for both versions

Usage:
    python compare_oddpub_versions.py \
        --input-dir ~/claude/pmcoaXMLs/compact_rtrans \
        --output-prefix results/oddpub_comparison \
        --year-range 2015 2024

Prerequisites:
    The compact_rtrans dataset must have been updated to include oddpub v7.2.3 columns:
    - oddpub_is_open_data
    - oddpub_is_open_code
    - oddpub_open_data_category

Author: Generated for INCF 2025 Poster Analysis
Date: 2025-12-01
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import sys
import logging
import argparse
import glob
from pathlib import Path
import gc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Major funders to analyze
MAJOR_FUNDERS = [
    'funder_nih', 'funder_ec', 'funder_nsfc', 'funder_dfg', 'funder_amed',
    'funder_wt', 'funder_cihr', 'funder_mrc', 'funder_hhmi', 'funder_bmgf'
]

FUNDER_DISPLAY_NAMES = {
    'funder_nih': 'NIH (USA)',
    'funder_ec': 'EC (Europe)',
    'funder_nsfc': 'NSFC (China)',
    'funder_dfg': 'DFG (Germany)',
    'funder_amed': 'AMED (Japan)',
    'funder_wt': 'Wellcome Trust (UK)',
    'funder_cihr': 'CIHR (Canada)',
    'funder_mrc': 'MRC (UK)',
    'funder_hhmi': 'HHMI (USA)',
    'funder_bmgf': 'BMGF (USA)'
}


def load_data_in_batches(input_dir, columns_needed, batch_size=100, limit=None):
    """Load parquet files in batches, extracting only needed columns."""
    parquet_files = sorted(glob.glob(f'{input_dir}/*.parquet'))

    if not parquet_files:
        logger.error(f"No parquet files found in {input_dir}")
        sys.exit(1)

    if limit:
        parquet_files = parquet_files[:limit]
        logger.info(f"Limited to first {limit} files for testing")

    logger.info(f"Loading {len(parquet_files)} parquet files...")

    all_dfs = []
    for i, pf in enumerate(parquet_files, 1):
        try:
            df = pd.read_parquet(pf, columns=columns_needed)
            all_dfs.append(df)

            if i % 100 == 0:
                logger.info(f"  Loaded {i}/{len(parquet_files)} files...")
        except Exception as e:
            logger.warning(f"Error loading {Path(pf).name}: {e}")

    result = pd.concat(all_dfs, ignore_index=True)
    logger.info(f"Loaded {len(result):,} total records")

    return result


def compare_detection_rates(df, output_prefix):
    """Calculate and compare overall detection rates."""

    # Check required columns exist
    v5_col = 'is_open_data'
    v7_col = 'oddpub_is_open_data'

    if v7_col not in df.columns:
        logger.error(f"Column '{v7_col}' not found. Run oddpub integration first.")
        logger.info("Available columns: " + ", ".join([c for c in df.columns if 'open' in c.lower()]))
        return None

    # Calculate overall rates
    total = len(df)
    v5_true = df[v5_col].sum()
    v7_true = df[v7_col].sum()

    v5_rate = v5_true / total * 100
    v7_rate = v7_true / total * 100

    # Agreement matrix
    both_true = ((df[v5_col] == True) & (df[v7_col] == True)).sum()
    v5_only = ((df[v5_col] == True) & (df[v7_col] == False)).sum()
    v7_only = ((df[v5_col] == False) & (df[v7_col] == True)).sum()
    both_false = ((df[v5_col] == False) & (df[v7_col] == False)).sum()

    results = {
        'total_records': total,
        'v5_true': int(v5_true),
        'v7_true': int(v7_true),
        'v5_rate_pct': v5_rate,
        'v7_rate_pct': v7_rate,
        'both_true': int(both_true),
        'v5_only': int(v5_only),
        'v7_only': int(v7_only),
        'both_false': int(both_false),
        'agreement_pct': (both_true + both_false) / total * 100
    }

    # Print summary
    logger.info("\n" + "=" * 60)
    logger.info("OVERALL DETECTION COMPARISON")
    logger.info("=" * 60)
    logger.info(f"Total records: {total:,}")
    logger.info(f"\noddpub v5 (rtransparent): {v5_true:,} ({v5_rate:.2f}%)")
    logger.info(f"oddpub v7.2.3:            {v7_true:,} ({v7_rate:.2f}%)")
    logger.info(f"\nAgreement Matrix:")
    logger.info(f"  Both TRUE:  {both_true:,} ({both_true/total*100:.2f}%)")
    logger.info(f"  v5 only:    {v5_only:,} ({v5_only/total*100:.2f}%)")
    logger.info(f"  v7 only:    {v7_only:,} ({v7_only/total*100:.2f}%)")
    logger.info(f"  Both FALSE: {both_false:,} ({both_false/total*100:.2f}%)")
    logger.info(f"\nOverall agreement: {results['agreement_pct']:.2f}%")

    return results


def compare_by_funder(df, output_prefix):
    """Compare detection rates by funder."""

    v5_col = 'is_open_data'
    v7_col = 'oddpub_is_open_data'

    results = []

    for funder in MAJOR_FUNDERS:
        if funder not in df.columns:
            continue

        funder_df = df[df[funder] == 1]
        total = len(funder_df)

        if total == 0:
            continue

        v5_true = funder_df[v5_col].sum()
        v7_true = funder_df[v7_col].sum()

        results.append({
            'funder': FUNDER_DISPLAY_NAMES.get(funder, funder),
            'total_funded': total,
            'v5_true': int(v5_true),
            'v5_rate': v5_true / total * 100,
            'v7_true': int(v7_true),
            'v7_rate': v7_true / total * 100,
            'diff_pct': (v7_true - v5_true) / total * 100
        })

    results_df = pd.DataFrame(results)
    results_df = results_df.sort_values('v7_rate', ascending=False)

    # Save to CSV
    results_df.to_csv(f'{output_prefix}_by_funder.csv', index=False)
    logger.info(f"Saved funder comparison to {output_prefix}_by_funder.csv")

    # Print table
    logger.info("\n" + "=" * 80)
    logger.info("DETECTION RATES BY FUNDER")
    logger.info("=" * 80)
    logger.info(f"{'Funder':<25} {'Total':<12} {'v5 Rate':<12} {'v7 Rate':<12} {'Diff':<10}")
    logger.info("-" * 80)
    for _, row in results_df.iterrows():
        logger.info(f"{row['funder']:<25} {row['total_funded']:<12,} {row['v5_rate']:<12.2f}% {row['v7_rate']:<12.2f}% {row['diff_pct']:+.2f}%")

    # Create comparison bar chart
    fig, ax = plt.subplots(figsize=(12, 8))

    x = np.arange(len(results_df))
    width = 0.35

    bars1 = ax.barh(x - width/2, results_df['v5_rate'], width, label='oddpub v5 (rtrans)', color='steelblue')
    bars2 = ax.barh(x + width/2, results_df['v7_rate'], width, label='oddpub v7.2.3', color='coral')

    ax.set_xlabel('Data Sharing Rate (%)', fontsize=12)
    ax.set_ylabel('Funder', fontsize=12)
    ax.set_title('Open Data Detection: oddpub v5 vs v7.2.3 by Funder', fontsize=14)
    ax.set_yticks(x)
    ax.set_yticklabels(results_df['funder'])
    ax.legend(loc='lower right')
    ax.grid(axis='x', alpha=0.3)

    plt.tight_layout()
    plt.savefig(f'{output_prefix}_by_funder.png', dpi=300, bbox_inches='tight')
    logger.info(f"Saved funder comparison chart to {output_prefix}_by_funder.png")
    plt.close()

    return results_df


def compare_by_year(df, output_prefix, year_range=None):
    """Compare detection rates by year."""

    v5_col = 'is_open_data'
    v7_col = 'oddpub_is_open_data'

    # Process year
    df['year_numeric'] = pd.to_numeric(df['year_epub'], errors='coerce')
    df = df.dropna(subset=['year_numeric'])
    df['year_numeric'] = df['year_numeric'].astype(int)

    if year_range:
        df = df[(df['year_numeric'] >= year_range[0]) & (df['year_numeric'] <= year_range[1])]

    # Aggregate by year
    yearly_stats = df.groupby('year_numeric').agg({
        v5_col: ['sum', 'count'],
        v7_col: 'sum'
    }).reset_index()

    yearly_stats.columns = ['year', 'v5_true', 'total', 'v7_true']
    yearly_stats['v5_rate'] = yearly_stats['v5_true'] / yearly_stats['total'] * 100
    yearly_stats['v7_rate'] = yearly_stats['v7_true'] / yearly_stats['total'] * 100

    # Save to CSV
    yearly_stats.to_csv(f'{output_prefix}_by_year.csv', index=False)
    logger.info(f"Saved yearly comparison to {output_prefix}_by_year.csv")

    # Create line chart
    fig, ax = plt.subplots(figsize=(12, 6))

    ax.plot(yearly_stats['year'], yearly_stats['v5_rate'],
            marker='o', linewidth=2, label='oddpub v5 (rtrans)', color='steelblue')
    ax.plot(yearly_stats['year'], yearly_stats['v7_rate'],
            marker='s', linewidth=2, label='oddpub v7.2.3', color='coral')

    ax.set_xlabel('Year', fontsize=12)
    ax.set_ylabel('Data Sharing Rate (%)', fontsize=12)
    ax.set_title('Open Data Detection Over Time: oddpub v5 vs v7.2.3', fontsize=14)
    ax.legend(loc='upper left')
    ax.grid(alpha=0.3)
    ax.set_ylim(bottom=0)

    plt.tight_layout()
    plt.savefig(f'{output_prefix}_by_year.png', dpi=300, bbox_inches='tight')
    logger.info(f"Saved yearly comparison chart to {output_prefix}_by_year.png")
    plt.close()

    return yearly_stats


def analyze_categories(df, output_prefix):
    """Analyze open_data_category distribution from v7.2.3."""

    cat_col = 'oddpub_open_data_category'

    if cat_col not in df.columns:
        logger.warning(f"Column '{cat_col}' not found, skipping category analysis")
        return None

    # Filter to records with open data
    open_data_df = df[df['oddpub_is_open_data'] == True].copy()

    if len(open_data_df) == 0:
        logger.warning("No records with oddpub_is_open_data=True")
        return None

    # Count categories (can have multiple comma-separated)
    all_categories = []
    for cat_str in open_data_df[cat_col].dropna():
        if pd.notna(cat_str) and cat_str:
            cats = [c.strip() for c in str(cat_str).split(',')]
            all_categories.extend(cats)

    cat_counts = pd.Series(all_categories).value_counts()

    # Save to CSV
    cat_counts.to_csv(f'{output_prefix}_categories.csv')
    logger.info(f"Saved category distribution to {output_prefix}_categories.csv")

    # Print summary
    logger.info("\n" + "=" * 60)
    logger.info("OPEN DATA CATEGORY DISTRIBUTION (oddpub v7.2.3)")
    logger.info("=" * 60)
    for cat, count in cat_counts.head(10).items():
        pct = count / len(open_data_df) * 100
        logger.info(f"  {cat}: {count:,} ({pct:.1f}%)")

    # Create bar chart
    fig, ax = plt.subplots(figsize=(10, 6))

    cat_counts.head(10).plot(kind='barh', ax=ax, color='steelblue')
    ax.set_xlabel('Count', fontsize=12)
    ax.set_ylabel('Category', fontsize=12)
    ax.set_title('Open Data Categories (oddpub v7.2.3)', fontsize=14)
    ax.invert_yaxis()

    plt.tight_layout()
    plt.savefig(f'{output_prefix}_categories.png', dpi=300, bbox_inches='tight')
    logger.info(f"Saved category chart to {output_prefix}_categories.png")
    plt.close()

    return cat_counts


def main():
    parser = argparse.ArgumentParser(
        description='Compare oddpub v5 (rtransparent) vs v7.2.3 detection rates'
    )
    parser.add_argument('--input-dir', required=True,
                       help='Directory containing compact_rtrans parquet files')
    parser.add_argument('--output-prefix', required=True,
                       help='Output prefix for results (e.g., results/oddpub_comparison)')
    parser.add_argument('--year-range', type=int, nargs=2, metavar=('MIN', 'MAX'),
                       help='Year range for analysis (e.g., 2015 2024)')
    parser.add_argument('--limit', type=int, default=None,
                       help='Limit number of files (for testing)')

    args = parser.parse_args()

    # Create output directory
    output_dir = Path(args.output_prefix).parent
    if output_dir and not output_dir.exists():
        output_dir.mkdir(parents=True, exist_ok=True)

    # Define columns to load
    columns_needed = [
        'pmcid_pmc', 'year_epub',
        'is_open_data', 'is_open_code',  # v5 via rtransparent
    ]

    # Add oddpub v7.2.3 columns if they exist
    v7_columns = [
        'oddpub_is_open_data', 'oddpub_is_open_code',
        'oddpub_open_data_category', 'oddpub_is_reuse'
    ]
    columns_needed.extend(v7_columns)

    # Add funder columns
    columns_needed.extend(MAJOR_FUNDERS)

    # Load data
    df = load_data_in_batches(args.input_dir, columns_needed, limit=args.limit)

    # Run comparisons
    overall_results = compare_detection_rates(df, args.output_prefix)

    if overall_results:
        funder_results = compare_by_funder(df, args.output_prefix)
        yearly_results = compare_by_year(df, args.output_prefix, args.year_range)
        category_results = analyze_categories(df, args.output_prefix)

        logger.info("\n" + "=" * 60)
        logger.info("ANALYSIS COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Output files saved with prefix: {args.output_prefix}")
    else:
        logger.error("Could not complete analysis - missing oddpub v7.2.3 columns")
        logger.info("Run the oddpub integration first to add oddpub_* columns")


if __name__ == '__main__':
    main()
