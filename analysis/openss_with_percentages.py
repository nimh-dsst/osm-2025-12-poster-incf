#!/usr/bin/env python3
"""
OpenSS Analysis with Percentage Calculations

This script combines:
1. OpenSS results (open data counts by journal/country/publisher/institution)
2. Corpus summary (total counts by journal/country/publisher/institution)

To produce percentage-based rankings showing what proportion of each category
shares open data.

Output includes both absolute counts and percentages.
"""

import argparse
import logging
from pathlib import Path

import pandas as pd
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_corpus_summary(corpus_file: Path) -> dict:
    """Load corpus summary and create lookup dicts by category."""
    logger.info(f"Loading corpus summary from {corpus_file}")

    df = pd.read_parquet(corpus_file)
    logger.info(f"Loaded {len(df):,} rows")

    # Create lookup dicts
    lookups = {}
    for cat_type in df['category_type'].unique():
        cat_df = df[df['category_type'] == cat_type]
        lookups[cat_type] = dict(zip(cat_df['category_value'], cat_df['total_count']))

    # Get total
    total = lookups.get('total', {}).get('all', 0)
    logger.info(f"Total corpus size: {total:,}")

    return lookups, total


def load_openss_results(openss_dir: Path) -> dict:
    """Load OpenSS analysis results."""
    logger.info(f"Loading OpenSS results from {openss_dir}")

    results = {}

    # Load each CSV
    for name in ['top_journals', 'top_countries', 'top_publishers', 'top_institutions']:
        csv_path = openss_dir / f'{name}.csv'
        if csv_path.exists():
            results[name] = pd.read_csv(csv_path)
            logger.info(f"  {name}: {len(results[name])} entries")
        else:
            logger.warning(f"  {name}: not found")

    return results


def calculate_percentages(openss_df: pd.DataFrame,
                         corpus_lookup: dict,
                         value_col: str,
                         count_col: str = 'count',
                         min_total: int = 100) -> pd.DataFrame:
    """
    Calculate open data percentages for each category value.

    Args:
        openss_df: DataFrame with openss counts
        corpus_lookup: Dict mapping category value to total count
        value_col: Column name containing category values
        count_col: Column name containing openss counts
        min_total: Minimum corpus count to include (avoids noisy percentages)

    Returns:
        DataFrame with added columns: corpus_total, open_data_pct
    """
    result = openss_df.copy()

    # Look up corpus totals
    result['corpus_total'] = result[value_col].map(corpus_lookup).fillna(0).astype(int)

    # Calculate percentage
    result['open_data_pct'] = np.where(
        result['corpus_total'] >= min_total,
        (result[count_col] / result['corpus_total'] * 100).round(2),
        np.nan
    )

    # Check for data quality issues (>100% indicates normalization mismatch)
    mask_over_100 = result['open_data_pct'] > 100
    if mask_over_100.any():
        logger.warning(f"Found {mask_over_100.sum()} entries with >100% (data mismatch)")
        # Cap at 100% and mark as suspect
        result.loc[mask_over_100, 'open_data_pct'] = np.nan  # Mark as invalid

    # Calculate 95% confidence interval (Wilson score)
    # Using normal approximation for simplicity
    result['ci_low'] = np.nan
    result['ci_high'] = np.nan

    mask = result['corpus_total'] >= min_total
    n = result.loc[mask, 'corpus_total']
    p = result.loc[mask, count_col] / n
    z = 1.96  # 95% CI

    # Wilson score interval
    denominator = 1 + z**2/n
    center = (p + z**2/(2*n)) / denominator
    delta = (z * np.sqrt(p*(1-p)/n + z**2/(4*n**2))) / denominator

    result.loc[mask, 'ci_low'] = ((center - delta) * 100).round(2)
    result.loc[mask, 'ci_high'] = ((center + delta) * 100).round(2)

    # Clip to valid range
    result['ci_low'] = result['ci_low'].clip(lower=0)
    result['ci_high'] = result['ci_high'].clip(upper=100)

    return result


def main():
    parser = argparse.ArgumentParser(
        description='Calculate open data percentages by combining OpenSS with corpus totals'
    )
    parser.add_argument('--openss-dir', '-o', required=True,
                       help='Directory containing OpenSS analysis results')
    parser.add_argument('--corpus-summary', '-c', required=True,
                       help='Corpus summary parquet file')
    parser.add_argument('--output-dir', '-d', required=True,
                       help='Output directory for percentage results')
    parser.add_argument('--min-total', type=int, default=100,
                       help='Minimum corpus count to calculate percentage (default: 100)')

    args = parser.parse_args()

    openss_dir = Path(args.openss_dir)
    corpus_file = Path(args.corpus_summary)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load data
    corpus_lookups, corpus_total = load_corpus_summary(corpus_file)
    openss_results = load_openss_results(openss_dir)

    # Process each category
    # Note: institutions skipped due to normalization differences between
    # corpus summary (raw affiliation_institution column) and OpenSS (regex extraction)
    category_mappings = {
        'top_journals': ('journal', 'journal'),
        'top_countries': ('country', 'country'),
        'top_publishers': ('publisher', 'publisher'),
        # 'top_institutions': ('institution', 'institution'),  # Skipped - normalization mismatch
    }

    all_results = {}

    for openss_key, (value_col, corpus_cat) in category_mappings.items():
        if openss_key not in openss_results:
            continue

        logger.info(f"Processing {openss_key}...")

        df = openss_results[openss_key]
        corpus_lookup = corpus_lookups.get(corpus_cat, {})

        if not corpus_lookup:
            logger.warning(f"No corpus data for {corpus_cat}")
            continue

        result = calculate_percentages(
            df, corpus_lookup, value_col,
            min_total=args.min_total
        )

        # Sort by percentage (descending), keeping NaN at end
        result = result.sort_values('open_data_pct', ascending=False, na_position='last')

        all_results[openss_key] = result

        # Save to CSV
        output_path = output_dir / f'{openss_key}_with_pct.csv'
        result.to_csv(output_path, index=False)
        logger.info(f"  Saved to {output_path}")

    # Generate summary report
    report_path = output_dir / 'percentage_summary.txt'
    with open(report_path, 'w') as f:
        f.write("=" * 80 + "\n")
        f.write("OPEN DATA PERCENTAGES BY CATEGORY\n")
        f.write("=" * 80 + "\n\n")

        f.write(f"Corpus total: {corpus_total:,} articles\n")
        f.write(f"Minimum count for percentage: {args.min_total}\n\n")

        for cat_name, df in all_results.items():
            title = cat_name.replace('top_', '').replace('_', ' ').upper()
            f.write("-" * 80 + "\n")
            f.write(f"TOP 20 {title} BY OPEN DATA PERCENTAGE\n")
            f.write("-" * 80 + "\n")

            # Get valid percentages only
            valid = df[df['open_data_pct'].notna()].head(20)

            value_col = [c for c in df.columns if c not in ['count', 'corpus_total', 'open_data_pct', 'ci_low', 'ci_high']][0]

            for _, row in valid.iterrows():
                name = row[value_col][:50] if len(str(row[value_col])) > 50 else row[value_col]
                pct = row['open_data_pct']
                ci = f"({row['ci_low']:.1f}-{row['ci_high']:.1f}%)" if pd.notna(row['ci_low']) else ""
                openss_count = row['count']
                corpus_count = row['corpus_total']

                f.write(f"  {pct:>6.2f}% {ci:>15}  {openss_count:>6,}/{corpus_count:>8,}  {name}\n")

            f.write("\n")

            # Also show top by absolute count
            f.write(f"TOP 10 {title} BY ABSOLUTE COUNT:\n")
            top_abs = df.sort_values('count', ascending=False).head(10)
            for _, row in top_abs.iterrows():
                name = row[value_col][:50] if len(str(row[value_col])) > 50 else row[value_col]
                pct = row['open_data_pct']
                pct_str = f"{pct:.2f}%" if pd.notna(pct) else "N/A"
                f.write(f"  {row['count']:>6,}  ({pct_str:>7})  {name}\n")

            f.write("\n")

        f.write("=" * 80 + "\n")

    logger.info(f"Summary report saved to {report_path}")

    # Print key findings
    print("\n" + "=" * 80)
    print("KEY FINDINGS - HIGHEST OPEN DATA PERCENTAGES")
    print("=" * 80)

    for cat_name, df in all_results.items():
        valid = df[df['open_data_pct'].notna()]
        if len(valid) == 0:
            continue

        title = cat_name.replace('top_', '').replace('_', ' ').title()
        print(f"\n{title}:")

        value_col = [c for c in df.columns if c not in ['count', 'corpus_total', 'open_data_pct', 'ci_low', 'ci_high']][0]

        for _, row in valid.head(5).iterrows():
            name = row[value_col][:40]
            print(f"  {row['open_data_pct']:>6.2f}%  ({row['count']:,}/{row['corpus_total']:,})  {name}")


if __name__ == '__main__':
    main()
