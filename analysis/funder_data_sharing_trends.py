#!/usr/bin/env python3
"""
Funder Data Sharing Trends Analysis

Adapted from funder-line-graph_v15.py to work with parquet files.
Creates line graphs showing data/code sharing rates by major funders over time.

Usage:
    python funder_data_sharing_trends.py \
        --input-dir ~/claude/pmcoaXMLs/compact_rtrans_test \
        --output-prefix results/funder_data_sharing \
        --year-column year_epub \
        --metric is_open_code \
        --year-range 2015 2020 \
        --log INFO

Author: Adapted for INCF 2025 Poster Analysis
Date: 2025-11-26
"""

import pandas as pd
import matplotlib.pyplot as plt
import sys
import logging
from tabulate import tabulate
import argparse
import itertools
import glob
from pathlib import Path
import gc

def setup_logging(log_level):
    """Setup logging configuration"""
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {log_level}')
    logging.basicConfig(level=numeric_level, format='%(levelname)s: %(message)s')

def process_files_in_batches(input_dir, funding_sources, year_column, metric_column, batch_size=100, limit=None):
    """
    Process parquet files in batches to aggregate results without loading all into memory.

    Args:
        input_dir: Directory containing parquet files
        funding_sources: List of funder column names
        year_column: Name of year column
        metric_column: Name of metric column (e.g., 'is_open_data')
        batch_size: Number of files to process at once
        limit: Optional limit on number of files to load (for testing)

    Returns:
        Tuple of (results dict, totals dict, total_records, total_with_metric)
    """
    logging.info(f"Processing parquet files from {input_dir} in batches of {batch_size}")

    parquet_files = sorted(glob.glob(f'{input_dir}/*.parquet'))

    if not parquet_files:
        logging.error(f"No parquet files found in {input_dir}")
        sys.exit(1)

    if limit:
        parquet_files = parquet_files[:limit]
        logging.info(f"Limited to first {limit} files for testing")

    logging.info(f"Found {len(parquet_files)} parquet files")

    # Initialize aggregated results
    results = {source: {} for source in funding_sources}  # Counts with metric
    totals = {source: {} for source in funding_sources}   # Total funded articles
    total_records = 0
    total_with_metric = 0

    # Process files in batches
    for batch_start in range(0, len(parquet_files), batch_size):
        batch_end = min(batch_start + batch_size, len(parquet_files))
        batch_files = parquet_files[batch_start:batch_end]

        # Load batch
        dfs = []
        for pf in batch_files:
            try:
                df = pd.read_parquet(pf)
                dfs.append(df)
            except Exception as e:
                logging.error(f"Error loading {Path(pf).name}: {e}")

        # Concatenate batch
        batch_df = pd.concat(dfs, ignore_index=True)
        del dfs
        gc.collect()

        # Process year column
        batch_df['year_numeric'] = pd.to_numeric(batch_df[year_column], errors='coerce')
        batch_df = batch_df.dropna(subset=['year_numeric'])
        batch_df['year_numeric'] = batch_df['year_numeric'].astype(int)

        # Aggregate by year and funder
        for year in batch_df['year_numeric'].unique():
            year_data = batch_df[batch_df['year_numeric'] == year]
            year_int = int(year)

            for source in funding_sources:
                # Count articles funded by this source
                total_count = year_data[source].sum()

                # Count articles funded by this source WITH the metric
                metric_count = year_data[year_data[source] == 1][metric_column].sum()

                # Aggregate into results
                if year_int not in results[source]:
                    results[source][year_int] = 0
                    totals[source][year_int] = 0

                results[source][year_int] += int(metric_count)
                totals[source][year_int] += int(total_count)

        # Track overall stats
        total_records += len(batch_df)
        total_with_metric += batch_df[metric_column].sum()

        # Clean up batch
        del batch_df
        gc.collect()

        logging.info(f"  Processed {batch_end}/{len(parquet_files)} files ({total_records:,} rows total)")

    logging.info(f"Successfully processed {len(parquet_files)} files ({total_records:,} total rows)")

    return results, totals, total_records, total_with_metric

def get_funder_display_name(col_name):
    """
    Get display name for funder (with full name and country/region).

    Args:
        col_name: Column name like 'funder_nih' or 'funder_nsfc'

    Returns:
        Display name like 'NIH (USA)' or 'EC (Europe)'
    """
    # Mapping of column names to display names
    display_names = {
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

    return display_names.get(col_name, col_name.replace('funder_', '').upper())

def detect_funder_columns(df, exclude_nih_institutes=True):
    """
    Detect funder columns with funder_* prefix.

    Args:
        df: DataFrame
        exclude_nih_institutes: If True, exclude individual NIH institutes (keep only overall NIH)

    Returns:
        List of funder column names
    """
    # Major funders (top-level organizations)
    major_funders = [
        'funder_nih', 'funder_ec', 'funder_nsfc', 'funder_dfg', 'funder_amed',
        'funder_wt', 'funder_cihr', 'funder_mrc', 'funder_hhmi', 'funder_bmgf'
    ]

    if exclude_nih_institutes:
        # Use only the major funders list
        funder_cols = [col for col in df.columns if col in major_funders]
    else:
        # Use all funder columns
        funder_cols = [col for col in df.columns if col.startswith('funder_')]

    logging.info(f"Detected {len(funder_cols)} funder columns")
    logging.debug(f"Funder columns: {', '.join(funder_cols)}")
    return funder_cols

def print_results_table(results, percentages, source_acronyms):
    """Print results in tabular format"""
    table_data = []
    headers = ["Year"] + [f"{acr} (Count)" for acr in source_acronyms.values()] + [f"{acr} (%)" for acr in source_acronyms.values()]
    years = sorted(set(year for source_data in results.values() for year in source_data.keys()))

    for year in years:
        row = [year]
        for source in results.keys():
            row.append(results[source].get(year, 0))
        for source in percentages.keys():
            row.append(f"{percentages[source].get(year, 0):.2f}%")
        table_data.append(row)

    print(tabulate(table_data, headers=headers, tablefmt="grid"))

def create_plots_and_csv(results, percentages, totals, source_display_names, output_filename_base, metric_name, year_range=None):
    """
    Create multiple plots and CSV files for debugging.

    Args:
        results: Dictionary of counts by funder and year (articles with metric)
        percentages: Dictionary of percentages by funder and year
        totals: Dictionary of total article counts by funder and year
        source_display_names: Mapping of column names to display names
        output_filename_base: Base filename for outputs
        metric_name: Name of metric being plotted (e.g., 'Data Sharing')
        year_range: Tuple of (min_year, max_year) for filtering years
    """
    # Filter by year range if specified
    if year_range:
        min_year, max_year = year_range
        count_data = {source_display_names[source]: {year: count for year, count in data.items() if min_year <= year <= max_year}
                      for source, data in results.items()}
        percentage_data = {source_display_names[source]: {year: perc for year, perc in data.items() if min_year <= year <= max_year}
                           for source, data in percentages.items()}
        total_data = {source_display_names[source]: {year: total for year, total in data.items() if min_year <= year <= max_year}
                      for source, data in totals.items()}
    else:
        count_data = {source_display_names[source]: data for source, data in results.items()}
        percentage_data = {source_display_names[source]: data for source, data in percentages.items()}
        total_data = {source_display_names[source]: data for source, data in totals.items()}

    # Create DataFrames
    count_df = pd.DataFrame(count_data).T
    percentage_df = pd.DataFrame(percentage_data).T
    total_df = pd.DataFrame(total_data).T

    # Sort by final year value (descending) for legend ordering
    if len(percentage_df.columns) > 0:
        final_year = sorted(percentage_df.columns)[-1]
        sort_order = percentage_df[final_year].sort_values(ascending=False).index
        count_df = count_df.reindex(sort_order)
        percentage_df = percentage_df.reindex(sort_order)
        total_df = total_df.reindex(sort_order)

    # Save CSVs
    count_df.to_csv(f'{output_filename_base}_counts.csv')
    logging.info(f"Count data saved to '{output_filename_base}_counts.csv'")

    total_df.to_csv(f'{output_filename_base}_totals.csv')
    logging.info(f"Total articles data saved to '{output_filename_base}_totals.csv'")

    percentage_df.to_csv(f'{output_filename_base}_percentages.csv')
    logging.info(f"Percentage data saved to '{output_filename_base}_percentages.csv'")

    # Define distinct colors for each funder
    colors = plt.cm.tab10(range(len(percentage_df.index)))

    # Plot 1: Raw counts
    fig, ax = plt.subplots(figsize=(12, 6))
    for idx, source in enumerate(count_df.index):
        years = sorted(count_df.columns)
        counts = [count_df.loc[source, year] for year in years]
        ax.plot(years, counts, label=source, linewidth=2, color=colors[idx])
    ax.set_xlabel('Year', fontsize=12)
    ax.set_ylabel(f'Number of Articles with {metric_name}', fontsize=12)
    ax.legend(title='Funder', bbox_to_anchor=(1.02, 1), loc='upper left', frameon=True)
    ax.grid(True, alpha=0.3)
    ax.set_ylim(bottom=0)
    plt.tight_layout()
    plt.savefig(f'{output_filename_base}_counts.png', dpi=300, bbox_inches='tight')
    logging.info(f"Count plot saved to '{output_filename_base}_counts.png'")
    plt.close()

    # Plot 2: Total articles per funder
    fig, ax = plt.subplots(figsize=(12, 6))
    for idx, source in enumerate(total_df.index):
        years = sorted(total_df.columns)
        totals_vals = [total_df.loc[source, year] for year in years]
        ax.plot(years, totals_vals, label=source, linewidth=2, color=colors[idx])
    ax.set_xlabel('Year', fontsize=12)
    ax.set_ylabel('Total Number of Articles', fontsize=12)
    ax.legend(title='Funder', bbox_to_anchor=(1.02, 1), loc='upper left', frameon=True)
    ax.grid(True, alpha=0.3)
    ax.set_ylim(bottom=0)
    plt.tight_layout()
    plt.savefig(f'{output_filename_base}_totals.png', dpi=300, bbox_inches='tight')
    logging.info(f"Total articles plot saved to '{output_filename_base}_totals.png'")
    plt.close()

    # Plot 3: Percentages
    fig, ax = plt.subplots(figsize=(12, 6))
    for idx, source in enumerate(percentage_df.index):
        years = sorted(percentage_df.columns)
        percentages_vals = [percentage_df.loc[source, year] for year in years]
        ax.plot(years, percentages_vals, label=source, linewidth=2, color=colors[idx])
    ax.set_xlabel('Year', fontsize=12)
    ax.set_ylabel(f'{metric_name} (%)', fontsize=12)
    ax.legend(title='Funder', bbox_to_anchor=(1.02, 1), loc='upper left', frameon=True)
    ax.grid(True, alpha=0.3)
    ax.set_ylim(bottom=0)
    plt.tight_layout()
    plt.savefig(f'{output_filename_base}_percentages.png', dpi=300, bbox_inches='tight')
    logging.info(f"Percentage plot saved to '{output_filename_base}_percentages.png'")
    plt.close()

def main(args):
    """Main analysis function"""
    setup_logging(args.log)

    # Load first file to get column names for validation
    parquet_files = sorted(glob.glob(f'{args.input_dir}/*.parquet'))
    if not parquet_files:
        logging.error(f"No parquet files found in {args.input_dir}")
        sys.exit(1)

    first_df = pd.read_parquet(parquet_files[0])
    logging.debug(f"Loaded first file to validate columns: {first_df.shape}")

    # Detect funder columns (exclude NIH institutes by default)
    funding_sources = detect_funder_columns(first_df, exclude_nih_institutes=True)
    if not funding_sources:
        logging.error("No funder columns found (expected columns starting with 'funder_')")
        sys.exit(1)

    source_display_names = {source: get_funder_display_name(source) for source in funding_sources}
    logging.info(f"Analyzing {len(funding_sources)} funders: {', '.join(source_display_names.values())}")

    # Check year column exists
    if args.year_column not in first_df.columns:
        logging.error(f"Year column '{args.year_column}' not found in dataframe")
        logging.error(f"Available columns: {', '.join(first_df.columns[:10])}...")
        sys.exit(1)

    # Check metric column exists
    if args.metric not in first_df.columns:
        logging.error(f"Metric column '{args.metric}' not found in dataframe")
        logging.error(f"Available metric columns: {[c for c in first_df.columns if 'open' in c.lower() or 'code' in c.lower() or 'data' in c.lower()]}")
        sys.exit(1)

    del first_df
    gc.collect()

    # Process files in batches and aggregate results
    results, totals, total_records, total_with_metric = process_files_in_batches(
        args.input_dir,
        funding_sources,
        args.year_column,
        args.metric,
        batch_size=100,
        limit=args.limit
    )

    # Calculate percentages
    percentages = {source: {} for source in funding_sources}
    for source in funding_sources:
        for year in results[source].keys():
            metric_count = results[source][year]
            total_count = totals[source][year]
            percentages[source][year] = (metric_count / total_count * 100) if total_count > 0 else 0

    # Print results table
    print("\nResults Table:")
    print_results_table(results, percentages, source_display_names)

    # Determine metric name for plots
    metric_name_map = {
        'is_open_code': 'Code Sharing',
        'is_open_data': 'Data Sharing',
        'is_relevant_code': 'Relevant Code Sharing',
        'is_relevant_data': 'Relevant Data Sharing'
    }
    metric_name = metric_name_map.get(args.metric, args.metric)

    # Create plots and CSV files
    if all(len(data) == 0 for data in results.values()):
        logging.warning("No data to plot. All counts are zero.")
    else:
        year_range = (args.year_range[0], args.year_range[1]) if args.year_range else None
        create_plots_and_csv(results, percentages, totals, source_display_names, args.output_prefix, metric_name, year_range)
        logging.info("Graphs and CSV files have been saved.")

    # Summary statistics
    logging.info(f"\n{'='*60}")
    logging.info(f"SUMMARY STATISTICS")
    logging.info(f"{'='*60}")
    logging.info(f"Total rows processed: {total_records:,}")
    all_years = sorted(set(year for source_data in results.values() for year in source_data.keys()))
    logging.info(f"Unique years: {all_years}")
    logging.info(f"\nFunding sources summary:")

    for source in funding_sources:
        # Sum across all years
        total_funded = sum(totals[source].values())
        total_with_metric = sum(results[source].values())
        percentage = (total_with_metric / total_funded * 100) if total_funded > 0 else 0
        logging.info(f"  {source_display_names[source]}: {total_with_metric:,} with {args.metric} out of {total_funded:,} funded ({percentage:.2f}%)")

    metric_percentage = (total_with_metric / total_records * 100) if total_records > 0 else 0
    logging.info(f"\nTotal '{args.metric}' TRUE values: {int(total_with_metric):,} out of {total_records:,} ({metric_percentage:.2f}%)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Analyze funding sources and data/code sharing trends from parquet files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze open code sharing
  python funder_data_sharing_trends.py \\
      --input-dir ~/claude/pmcoaXMLs/compact_rtrans_test \\
      --output-prefix results/funder_code_sharing \\
      --metric is_open_code

  # Analyze open data sharing with specific year range
  python funder_data_sharing_trends.py \\
      --input-dir ~/claude/pmcoaXMLs/compact_rtrans_test \\
      --output-prefix results/funder_data_sharing \\
      --metric is_open_data \\
      --year-range 2015 2020

  # Test with limited files
  python funder_data_sharing_trends.py \\
      --input-dir ~/claude/pmcoaXMLs/compact_rtrans_test \\
      --output-prefix results/test \\
      --metric is_open_code \\
      --limit 10
        """
    )

    parser.add_argument("--input-dir", required=True,
                        help="Directory containing compact parquet files")
    parser.add_argument("--output-prefix", required=True,
                        help="Output prefix for plots and CSV files (e.g., 'results/funder_data_sharing')")
    parser.add_argument("--year-column", default="year_epub",
                        help="Column name for year (default: year_epub)")
    parser.add_argument("--metric", default="is_open_data",
                        help="Metric column to analyze (default: is_open_data). Options: is_open_code, is_open_data, is_relevant_code, is_relevant_data")
    parser.add_argument("--year-range", type=int, nargs=2, metavar=('MIN', 'MAX'),
                        help="Year range for percentage plot (e.g., 2015 2020)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit number of parquet files to load (for testing)")
    parser.add_argument("--log", default="INFO",
                        help="Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)")

    args = parser.parse_args()

    # Create output directory if needed
    output_dir = Path(args.output_prefix).parent
    if output_dir and not output_dir.exists():
        output_dir.mkdir(parents=True, exist_ok=True)
        logging.info(f"Created output directory: {output_dir}")

    main(args)
