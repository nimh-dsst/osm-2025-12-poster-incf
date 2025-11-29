#!/usr/bin/env python3
"""
Analyze open data categories from oddpub results.
Creates summary statistics and visualizations.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path
import glob
import argparse

# Set plot style
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

# Define color scheme for categories
CATEGORY_COLORS = {
    'field-specific repository': '#2E7D32',
    'general-purpose repository': '#43A047',
    're-use': '#66BB6A',
    'github': '#1976D2',
    'supplement': '#FFA726',
    'upon request': '#FF7043',
    'unknown/misspecified source': '#E53935',
    'no data': '#BDBDBD',
    'multiple': '#9C27B0'
}


def simplify_category(cat_str):
    """Simplify complex category strings to main category."""
    if pd.isna(cat_str) or cat_str == '':
        return 'no data'

    # Count components
    components = [c.strip() for c in cat_str.split(',')]
    if len(components) > 1:
        # For multiple components, prioritize by quality
        if 'field-specific repository' in cat_str:
            return 'field-specific repository'
        elif 'general-purpose repository' in cat_str:
            return 'general-purpose repository'
        elif 'github' in cat_str:
            return 'github'
        else:
            return 'multiple'

    # Single component
    return components[0]


def analyze_oddpub_results(oddpub_dir: Path, max_files: int = None):
    """Load and analyze oddpub results."""

    print(f"Loading oddpub results from {oddpub_dir}")
    files = sorted(glob.glob(str(oddpub_dir / "*.parquet")))

    if max_files:
        files = files[:max_files]

    # Load all data
    dfs = []
    for f in files:
        df = pd.read_parquet(f)
        dfs.append(df)

    combined_df = pd.concat(dfs, ignore_index=True)
    print(f"Loaded {len(combined_df)} records from {len(files)} files")

    # Basic statistics
    print("\nBasic Statistics:")
    print(f"  Total articles: {len(combined_df)}")
    print(f"  Articles with open data: {combined_df['is_open_data'].sum()} ({combined_df['is_open_data'].sum()/len(combined_df)*100:.1f}%)")
    print(f"  Articles with open code: {combined_df['is_open_code'].sum()} ({combined_df['is_open_code'].sum()/len(combined_df)*100:.1f}%)")

    # Simplify categories
    combined_df['category_simple'] = combined_df['open_data_category'].apply(simplify_category)

    # Category distribution
    print("\nOpen Data Category Distribution:")
    cat_counts = combined_df['category_simple'].value_counts()
    for cat, count in cat_counts.items():
        pct = count / len(combined_df) * 100
        print(f"  {cat}: {count} ({pct:.1f}%)")

    # Among those with open data
    open_data_df = combined_df[combined_df['is_open_data'] == True]
    print(f"\nAmong articles with open data (n={len(open_data_df)}):")
    cat_counts_open = open_data_df['category_simple'].value_counts()
    for cat, count in cat_counts_open.items():
        if cat != 'no data':
            pct = count / len(open_data_df) * 100
            print(f"  {cat}: {count} ({pct:.1f}%)")

    return combined_df


def create_category_pie_chart(df: pd.DataFrame, output_file: Path):
    """Create pie chart of open data categories."""

    # Get counts for articles with open data
    open_data_df = df[df['is_open_data'] == True]
    cat_counts = open_data_df['category_simple'].value_counts()

    # Filter out 'no data' category
    cat_counts = cat_counts[cat_counts.index != 'no data']

    # Create pie chart
    fig, ax = plt.subplots(figsize=(10, 8))

    # Get colors
    colors = [CATEGORY_COLORS.get(cat, '#757575') for cat in cat_counts.index]

    # Create pie
    wedges, texts, autotexts = ax.pie(cat_counts.values,
                                      labels=cat_counts.index,
                                      colors=colors,
                                      autopct='%1.1f%%',
                                      startangle=90)

    # Beautify
    for text in texts:
        text.set_fontsize(11)
    for autotext in autotexts:
        autotext.set_color('white')
        autotext.set_fontweight('bold')
        autotext.set_fontsize(10)

    ax.set_title(f'Distribution of Open Data Categories\n(n={len(open_data_df)} articles with open data)',
                fontsize=14, fontweight='bold', pad=20)

    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"\nSaved pie chart to: {output_file}")


def create_stacked_bar_by_year(df: pd.DataFrame, output_file: Path):
    """Create stacked bar chart by publication year (if year data available)."""

    # This would require year data from metadata - placeholder for now
    print("\nNote: Year-based analysis requires merging with metadata containing publication years")


def save_summary_stats(df: pd.DataFrame, output_file: Path):
    """Save summary statistics to CSV."""

    summary = []

    # Overall stats
    summary.append({
        'metric': 'Total articles',
        'value': len(df),
        'percentage': 100.0
    })
    summary.append({
        'metric': 'Articles with open data',
        'value': df['is_open_data'].sum(),
        'percentage': df['is_open_data'].sum() / len(df) * 100
    })
    summary.append({
        'metric': 'Articles with open code',
        'value': df['is_open_code'].sum(),
        'percentage': df['is_open_code'].sum() / len(df) * 100
    })

    # Category breakdown
    cat_counts = df['category_simple'].value_counts()
    for cat, count in cat_counts.items():
        summary.append({
            'metric': f'Category: {cat}',
            'value': count,
            'percentage': count / len(df) * 100
        })

    summary_df = pd.DataFrame(summary)
    summary_df.to_csv(output_file, index=False)
    print(f"Saved summary statistics to: {output_file}")


def main():
    parser = argparse.ArgumentParser(description='Analyze oddpub open data categories')
    parser.add_argument('--input-dir', type=Path,
                      default=Path('/home/ec2-user/claude/osm-oddpub-out'),
                      help='Directory containing oddpub results')
    parser.add_argument('--output-dir', type=Path,
                      default=Path('results'),
                      help='Output directory for results')
    parser.add_argument('--max-files', type=int, default=None,
                      help='Maximum number of files to process')

    args = parser.parse_args()

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Analyze data
    df = analyze_oddpub_results(args.input_dir, args.max_files)

    # Create visualizations
    create_category_pie_chart(df, args.output_dir / 'oddpub_categories_pie.png')

    # Save summary
    save_summary_stats(df, args.output_dir / 'oddpub_summary_stats.csv')

    print("\nAnalysis complete!")


if __name__ == '__main__':
    main()