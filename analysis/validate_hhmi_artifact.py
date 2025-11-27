#!/usr/bin/env python3
"""
Validate HHMI Data Sharing Results - Check for Systematic Biases

Compares HHMI-funded articles to other funders on:
1. Article length (chars_in_body, file_size)
2. Publication journals (top journals for each funder)
3. Publication year distribution
4. Article type distribution

Usage:
    python validate_hhmi_artifact.py \
        --input-dir ~/claude/pmcoaXMLs/compact_rtrans \
        --output-prefix results/hhmi_validation

Author: INCF 2025 Poster Analysis
Date: 2025-11-26
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import argparse
import logging
import glob
from pathlib import Path
import gc

def setup_logging(log_level):
    """Setup logging configuration"""
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {log_level}')
    logging.basicConfig(level=numeric_level, format='%(levelname)s: %(message)s')

def process_files_in_batches(input_dir, batch_size=100, limit=None):
    """
    Process parquet files in batches to collect statistics.

    Returns DataFrame with columns: funder, chars_in_body, file_size, journal, year_epub, type
    """
    logging.info(f"Processing parquet files from {input_dir} in batches of {batch_size}")

    parquet_files = sorted(glob.glob(f'{input_dir}/*.parquet'))

    if not parquet_files:
        logging.error(f"No parquet files found in {input_dir}")
        return None

    if limit:
        parquet_files = parquet_files[:limit]
        logging.info(f"Limited to first {limit} files for testing")

    logging.info(f"Found {len(parquet_files)} parquet files")

    # Major funders to analyze
    funder_cols = ['funder_nih', 'funder_ec', 'funder_nsfc', 'funder_dfg',
                   'funder_amed', 'funder_wt', 'funder_cihr', 'funder_mrc',
                   'funder_hhmi', 'funder_bmgf']

    funder_names = {
        'funder_nih': 'NIH',
        'funder_ec': 'EC',
        'funder_nsfc': 'NSFC',
        'funder_dfg': 'DFG',
        'funder_amed': 'AMED',
        'funder_wt': 'Wellcome Trust',
        'funder_cihr': 'CIHR',
        'funder_mrc': 'MRC',
        'funder_hhmi': 'HHMI',
        'funder_bmgf': 'BMGF'
    }

    # Collect data for each funder
    all_data = []

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

        # Extract data for each funder
        for funder_col in funder_cols:
            if funder_col not in batch_df.columns:
                continue

            # Get articles funded by this funder
            funder_articles = batch_df[batch_df[funder_col] == 1].copy()

            if len(funder_articles) == 0:
                continue

            # Add funder name
            funder_articles['funder'] = funder_names[funder_col]

            # Select relevant columns
            cols_to_keep = ['funder', 'chars_in_body', 'file_size', 'journal',
                           'year_epub', 'type', 'is_open_data']

            # Keep only columns that exist
            cols_to_keep = [c for c in cols_to_keep if c in funder_articles.columns]

            funder_data = funder_articles[cols_to_keep].copy()
            all_data.append(funder_data)

        # Clean up batch
        del batch_df
        gc.collect()

        logging.info(f"  Processed {batch_end}/{len(parquet_files)} files")

    # Concatenate all data
    result_df = pd.concat(all_data, ignore_index=True)
    logging.info(f"Collected {len(result_df):,} funder-article records")

    return result_df

def analyze_article_length(df, output_prefix):
    """Analyze article length distribution by funder"""
    logging.info("Analyzing article length distribution...")

    # Remove NaN values
    df_clean = df[df['chars_in_body'].notna()].copy()

    # Summary statistics by funder
    stats = df_clean.groupby('funder')['chars_in_body'].agg([
        ('count', 'count'),
        ('mean', 'mean'),
        ('median', 'median'),
        ('std', 'std'),
        ('min', 'min'),
        ('max', 'max')
    ]).round(0)

    stats.to_csv(f'{output_prefix}_article_length_stats.csv')
    logging.info(f"Saved article length stats to {output_prefix}_article_length_stats.csv")

    # Box plot
    fig, ax = plt.subplots(figsize=(12, 6))

    funders = sorted(df_clean['funder'].unique())
    data_by_funder = [df_clean[df_clean['funder'] == f]['chars_in_body'].values for f in funders]

    bp = ax.boxplot(data_by_funder, labels=funders, patch_artist=True)

    # Highlight HHMI box
    for patch, funder in zip(bp['boxes'], funders):
        if funder == 'HHMI':
            patch.set_facecolor('red')
            patch.set_alpha(0.3)
        else:
            patch.set_facecolor('lightblue')
            patch.set_alpha(0.5)

    ax.set_ylabel('Characters in Body', fontsize=12)
    ax.set_xlabel('Funder', fontsize=12)
    ax.set_title('Article Length Distribution by Funder', fontsize=14)
    ax.grid(True, alpha=0.3, axis='y')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(f'{output_prefix}_article_length_boxplot.png', dpi=300, bbox_inches='tight')
    logging.info(f"Saved box plot to {output_prefix}_article_length_boxplot.png")
    plt.close()

    return stats

def analyze_journals(df, output_prefix, top_n=20):
    """Analyze top journals by funder"""
    logging.info("Analyzing journal distribution...")

    # Remove NaN journals
    df_clean = df[df['journal'].notna()].copy()

    # Top journals for HHMI
    hhmi_journals = df_clean[df_clean['funder'] == 'HHMI']['journal'].value_counts().head(top_n)

    # Top journals for other funders combined
    other_journals = df_clean[df_clean['funder'] != 'HHMI']['journal'].value_counts().head(top_n)

    # Compare
    journal_comparison = pd.DataFrame({
        'HHMI_count': hhmi_journals,
        'Others_count': other_journals
    }).fillna(0)

    # Calculate percentages
    hhmi_total = len(df_clean[df_clean['funder'] == 'HHMI'])
    others_total = len(df_clean[df_clean['funder'] != 'HHMI'])

    journal_comparison['HHMI_%'] = (journal_comparison['HHMI_count'] / hhmi_total * 100).round(2)
    journal_comparison['Others_%'] = (journal_comparison['Others_count'] / others_total * 100).round(2)
    journal_comparison['Difference'] = (journal_comparison['HHMI_%'] - journal_comparison['Others_%']).round(2)

    # Sort by HHMI percentage
    journal_comparison = journal_comparison.sort_values('HHMI_%', ascending=False)

    journal_comparison.to_csv(f'{output_prefix}_journal_comparison.csv')
    logging.info(f"Saved journal comparison to {output_prefix}_journal_comparison.csv")

    return journal_comparison

def analyze_year_distribution(df, output_prefix):
    """Analyze publication year distribution by funder"""
    logging.info("Analyzing year distribution...")

    # Remove NaN years
    df_clean = df[df['year_epub'].notna()].copy()
    df_clean['year'] = pd.to_numeric(df_clean['year_epub'], errors='coerce')
    df_clean = df_clean[df_clean['year'].notna()]
    df_clean['year'] = df_clean['year'].astype(int)

    # Filter to reasonable years (2000-2025)
    df_clean = df_clean[(df_clean['year'] >= 2000) & (df_clean['year'] <= 2025)]

    # Summary stats
    year_stats = df_clean.groupby('funder')['year'].agg([
        ('count', 'count'),
        ('mean', 'mean'),
        ('median', 'median'),
        ('min', 'min'),
        ('max', 'max')
    ]).round(1)

    year_stats.to_csv(f'{output_prefix}_year_distribution_stats.csv')
    logging.info(f"Saved year stats to {output_prefix}_year_distribution_stats.csv")

    # Plot year distribution
    fig, ax = plt.subplots(figsize=(12, 6))

    for funder in sorted(df_clean['funder'].unique()):
        funder_data = df_clean[df_clean['funder'] == funder]
        year_counts = funder_data.groupby('year').size()

        # Highlight HHMI
        if funder == 'HHMI':
            ax.plot(year_counts.index, year_counts.values, marker='o', label=funder,
                   linewidth=3, markersize=8, color='red')
        else:
            ax.plot(year_counts.index, year_counts.values, alpha=0.5, label=funder)

    ax.set_xlabel('Year', fontsize=12)
    ax.set_ylabel('Number of Articles', fontsize=12)
    ax.set_title('Publication Year Distribution by Funder', fontsize=14)
    ax.legend(bbox_to_anchor=(1.02, 1), loc='upper left', frameon=True)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(f'{output_prefix}_year_distribution.png', dpi=300, bbox_inches='tight')
    logging.info(f"Saved year distribution plot to {output_prefix}_year_distribution.png")
    plt.close()

    return year_stats

def analyze_data_sharing_by_year(df, output_prefix):
    """Analyze data sharing rates by funder and year"""
    logging.info("Analyzing data sharing by year...")

    # Clean data
    df_clean = df[(df['year_epub'].notna()) & (df['is_open_data'].notna())].copy()
    df_clean['year'] = pd.to_numeric(df_clean['year_epub'], errors='coerce')
    df_clean = df_clean[df_clean['year'].notna()]
    df_clean['year'] = df_clean['year'].astype(int)
    df_clean = df_clean[(df_clean['year'] >= 2010) & (df_clean['year'] <= 2025)]

    # Calculate rates by funder and year
    results = []
    for funder in sorted(df_clean['funder'].unique()):
        funder_data = df_clean[df_clean['funder'] == funder]

        for year in sorted(funder_data['year'].unique()):
            year_data = funder_data[funder_data['year'] == year]
            total = len(year_data)
            with_data = year_data['is_open_data'].sum()
            rate = (with_data / total * 100) if total > 0 else 0

            results.append({
                'funder': funder,
                'year': year,
                'total': total,
                'with_open_data': int(with_data),
                'rate_%': rate
            })

    results_df = pd.DataFrame(results)
    results_df.to_csv(f'{output_prefix}_data_sharing_by_year.csv', index=False)
    logging.info(f"Saved data sharing by year to {output_prefix}_data_sharing_by_year.csv")

    # Plot
    fig, ax = plt.subplots(figsize=(12, 6))

    for funder in sorted(results_df['funder'].unique()):
        funder_data = results_df[results_df['funder'] == funder]

        # Highlight HHMI
        if funder == 'HHMI':
            ax.plot(funder_data['year'], funder_data['rate_%'], marker='o', label=funder,
                   linewidth=3, markersize=8, color='red')
        else:
            ax.plot(funder_data['year'], funder_data['rate_%'], alpha=0.5, label=funder)

    ax.set_xlabel('Year', fontsize=12)
    ax.set_ylabel('Data Sharing Rate (%)', fontsize=12)
    ax.set_title('Data Sharing Rate Over Time by Funder (2010-2025)', fontsize=14)
    ax.legend(bbox_to_anchor=(1.02, 1), loc='upper left', frameon=True)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(f'{output_prefix}_data_sharing_trends.png', dpi=300, bbox_inches='tight')
    logging.info(f"Saved data sharing trends to {output_prefix}_data_sharing_trends.png")
    plt.close()

    return results_df

def main():
    parser = argparse.ArgumentParser(
        description="Validate HHMI data sharing results by checking for systematic biases.",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument("--input-dir", required=True,
                       help="Directory containing compact parquet files")
    parser.add_argument("--output-prefix", required=True,
                       help="Output prefix for analysis files")
    parser.add_argument("--limit", type=int, default=None,
                       help="Limit number of files to process (for testing)")
    parser.add_argument("--log", default="INFO",
                       help="Logging level (DEBUG, INFO, WARNING, ERROR)")

    args = parser.parse_args()
    setup_logging(args.log)

    # Create output directory if needed
    output_dir = Path(args.output_prefix).parent
    if output_dir and not output_dir.exists():
        output_dir.mkdir(parents=True, exist_ok=True)

    # Load data
    df = process_files_in_batches(args.input_dir, batch_size=100, limit=args.limit)

    if df is None or len(df) == 0:
        logging.error("No data loaded. Exiting.")
        return

    logging.info(f"Loaded {len(df):,} funder-article records")
    logging.info(f"Funders: {sorted(df['funder'].unique())}")

    # Run analyses
    logging.info("\n" + "="*60)
    logging.info("VALIDATION ANALYSES")
    logging.info("="*60)

    # 1. Article length
    length_stats = analyze_article_length(df, args.output_prefix)
    print("\n### Article Length Statistics ###")
    print(length_stats)

    # 2. Journal distribution - SKIPPED (journal column not in compact dataset)
    # Note: Journal analysis would require full metadata files
    logging.info("Skipping journal analysis (journal column not available in compact dataset)")

    # 3. Year distribution
    year_stats = analyze_year_distribution(df, args.output_prefix)
    print("\n### Year Distribution Statistics ###")
    print(year_stats)

    # 4. Data sharing by year
    sharing_by_year = analyze_data_sharing_by_year(df, args.output_prefix)
    print("\n### Data Sharing by Year (Recent) ###")
    hhmi_recent = sharing_by_year[(sharing_by_year['funder'] == 'HHMI') &
                                   (sharing_by_year['year'] >= 2020)]
    print(hhmi_recent)

    logging.info("\n" + "="*60)
    logging.info("VALIDATION COMPLETE")
    logging.info("="*60)
    logging.info("Check output files for detailed results.")

if __name__ == "__main__":
    main()
