#!/usr/bin/env python3
"""
Create 100% stacked bar chart showing open data categories by funder.
This combines oddpub results with funder information from compact_rtrans.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path
import glob
import argparse
from typing import Dict, List
import warnings
warnings.filterwarnings('ignore')

# Define color scheme for data sharing categories
CATEGORY_COLORS = {
    'field-specific repository': '#2E7D32',  # Dark green - best practice
    'general-purpose repository': '#43A047',  # Medium green - good
    're-use': '#FDD835',  # Yellow - data reuse
    'github': '#1976D2',  # Blue - code repository
    'supplement': '#FB8C00',  # Orange - okay but not ideal
    'upon request': '#F57C00',  # Dark orange - poor practice
    'unknown/misspecified source': '#D32F2F',  # Red - problematic
    'no statement': '#9E9E9E',  # Gray - no data sharing
    'other': '#795548'  # Brown - combinations/other
}

# Major funders to analyze
MAJOR_FUNDERS = [
    'NIH', 'NSFC', 'EC', 'Wellcome Trust', 'MRC',
    'NHMRC', 'NSF', 'HHMI', 'DFG', 'CRUK',
    'CIHR', 'NCI', 'NHLBI', 'NIAID', 'NIGMS'
]


def categorize_open_data(category_str):
    """Simplify complex category combinations into main categories."""
    if pd.isna(category_str) or category_str == '':
        return 'no statement'

    # Convert to lowercase for matching
    cat_lower = category_str.lower()

    # Priority order for categorization
    if 'field-specific repository' in cat_lower:
        return 'field-specific repository'
    elif 'general-purpose repository' in cat_lower:
        return 'general-purpose repository'
    elif 'github' in cat_lower:
        return 'github'
    elif 're-use' in cat_lower:
        return 're-use'
    elif 'supplement' in cat_lower:
        return 'supplement'
    elif 'upon request' in cat_lower:
        return 'upon request'
    elif 'unknown' in cat_lower or 'misspecified' in cat_lower:
        return 'unknown/misspecified source'
    else:
        return 'other'


def merge_oddpub_with_funders(oddpub_dir: Path, compact_rtrans_dir: Path, sample_size: int = None) -> pd.DataFrame:
    """Merge oddpub results with funder information from compact_rtrans."""

    print("Loading oddpub results...")
    oddpub_files = sorted(glob.glob(str(oddpub_dir / "*.parquet")))

    if sample_size:
        oddpub_files = oddpub_files[:sample_size]

    # Load oddpub data
    oddpub_dfs = []
    for f in oddpub_files:
        df = pd.read_parquet(f)
        # Keep only needed columns
        cols_to_keep = ['pmcid', 'is_open_data', 'open_data_category']
        df = df[[c for c in cols_to_keep if c in df.columns]]
        oddpub_dfs.append(df)

    oddpub_df = pd.concat(oddpub_dfs, ignore_index=True)
    print(f"Loaded {len(oddpub_df)} oddpub records")

    # Ensure PMCIDs are properly formatted
    oddpub_df['pmcid'] = oddpub_df['pmcid'].str.strip().str.upper()
    oddpub_df = oddpub_df[oddpub_df['pmcid'].str.startswith('PMC')]

    print("\nLoading compact_rtrans data for funder information...")
    rtrans_files = sorted(glob.glob(str(compact_rtrans_dir / "compact_rtrans_*.parquet")))[:10]  # Sample

    # Get funder columns
    sample_df = pd.read_parquet(rtrans_files[0])
    funder_cols = [col for col in sample_df.columns if col.startswith('funder_')]

    # Load rtrans data with funder info
    rtrans_dfs = []
    for f in rtrans_files[:5]:  # Sample for testing
        df = pd.read_parquet(f)
        # Keep PMCID and funder columns
        cols_to_keep = ['pmcid_pmc'] + funder_cols
        df = df[[c for c in cols_to_keep if c in df.columns]]
        df.rename(columns={'pmcid_pmc': 'pmcid'}, inplace=True)
        rtrans_dfs.append(df)

    rtrans_df = pd.concat(rtrans_dfs, ignore_index=True)
    print(f"Loaded {len(rtrans_df)} rtrans records with funder data")

    # Merge
    print("\nMerging oddpub and funder data...")
    merged_df = oddpub_df.merge(rtrans_df, on='pmcid', how='inner')
    print(f"Merged dataset: {len(merged_df)} records")

    return merged_df, funder_cols


def calculate_category_percentages(df: pd.DataFrame, funder_cols: List[str]) -> pd.DataFrame:
    """Calculate open data category percentages by funder."""

    # Simplify categories
    df['category_simple'] = df['open_data_category'].apply(categorize_open_data)

    results = []

    for funder_col in funder_cols:
        funder_name = funder_col.replace('funder_', '').upper()

        # Skip if not in major funders
        if funder_name not in [f.upper() for f in MAJOR_FUNDERS]:
            continue

        # Get records funded by this funder
        funder_df = df[df[funder_col] == 1].copy()

        if len(funder_df) < 100:  # Skip funders with too few articles
            continue

        # Count categories
        category_counts = funder_df['category_simple'].value_counts()
        total = len(funder_df)

        # Calculate percentages
        for category, count in category_counts.items():
            results.append({
                'funder': funder_name,
                'category': category,
                'count': count,
                'total': total,
                'percentage': (count / total) * 100
            })

    return pd.DataFrame(results)


def create_stacked_bar_chart(results_df: pd.DataFrame, output_file: Path):
    """Create 100% stacked bar chart."""

    # Pivot data for stacking
    pivot_df = results_df.pivot(index='funder', columns='category', values='percentage')
    pivot_df = pivot_df.fillna(0)

    # Sort funders by total open data percentage (non-gray categories)
    non_gray_cols = [col for col in pivot_df.columns if col != 'no statement']
    open_data_pct = pivot_df[non_gray_cols].sum(axis=1)
    pivot_df = pivot_df.loc[open_data_pct.sort_values(ascending=False).index]

    # Create figure
    fig, ax = plt.subplots(figsize=(12, 8))

    # Create stacked bar chart
    bottom = np.zeros(len(pivot_df))

    # Order categories by quality (best to worst)
    category_order = [
        'field-specific repository',
        'general-purpose repository',
        'github',
        're-use',
        'supplement',
        'upon request',
        'unknown/misspecified source',
        'other',
        'no statement'
    ]

    # Plot each category
    for category in category_order:
        if category in pivot_df.columns:
            values = pivot_df[category].values
            color = CATEGORY_COLORS.get(category, '#795548')
            ax.bar(range(len(pivot_df)), values, bottom=bottom,
                  label=category.title(), color=color, edgecolor='white', linewidth=0.5)
            bottom += values

    # Formatting
    ax.set_xlabel('Funder', fontsize=12, fontweight='bold')
    ax.set_ylabel('Percentage of Funded Articles', fontsize=12, fontweight='bold')
    ax.set_title('Open Data Sharing Categories by Funder\n100% Stacked Bar Chart',
                fontsize=14, fontweight='bold', pad=20)

    # X-axis
    ax.set_xticks(range(len(pivot_df)))
    ax.set_xticklabels(pivot_df.index, rotation=45, ha='right')

    # Y-axis
    ax.set_ylim(0, 100)
    ax.set_yticks(range(0, 101, 10))
    ax.set_yticklabels([f'{y}%' for y in range(0, 101, 10)])

    # Grid
    ax.grid(axis='y', alpha=0.3, linestyle='--')
    ax.set_axisbelow(True)

    # Legend
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles[::-1], labels[::-1], loc='center left', bbox_to_anchor=(1, 0.5),
             title='Data Sharing Category', frameon=True, fancybox=True, shadow=True)

    # Add sample sizes
    for i, (funder, row) in enumerate(pivot_df.iterrows()):
        total = results_df[results_df['funder'] == funder]['total'].iloc[0]
        ax.text(i, -5, f'n={total:,}', ha='center', va='top', fontsize=8, rotation=0)

    # Add horizontal line at 50%
    ax.axhline(y=50, color='black', linestyle='--', linewidth=1, alpha=0.5)

    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    plt.close()

    print(f"\nSaved chart to: {output_file}")

    # Also save summary statistics
    summary_file = output_file.with_suffix('.csv')
    results_df.to_csv(summary_file, index=False)
    print(f"Saved summary data to: {summary_file}")


def main():
    parser = argparse.ArgumentParser(description='Create stacked bar chart of open data categories by funder')
    parser.add_argument('--oddpub-dir', type=Path,
                      default=Path('/home/ec2-user/claude/osm-oddpub-out'),
                      help='Directory containing oddpub results')
    parser.add_argument('--compact-rtrans-dir', type=Path,
                      default=Path('/home/ec2-user/claude/pmcoaXMLs/compact_rtrans'),
                      help='Directory containing compact_rtrans files')
    parser.add_argument('--output', type=Path,
                      default=Path('results/funder_open_data_categories_stacked.png'),
                      help='Output file path')
    parser.add_argument('--sample-size', type=int, default=None,
                      help='Number of oddpub files to process (for testing)')

    args = parser.parse_args()

    # Create output directory
    args.output.parent.mkdir(parents=True, exist_ok=True)

    # Process data
    merged_df, funder_cols = merge_oddpub_with_funders(
        args.oddpub_dir,
        args.compact_rtrans_dir,
        args.sample_size
    )

    # Calculate percentages
    print("\nCalculating category percentages by funder...")
    results_df = calculate_category_percentages(merged_df, funder_cols)

    # Print summary
    print("\nSummary by funder:")
    for funder in results_df['funder'].unique():
        funder_data = results_df[results_df['funder'] == funder]
        total = funder_data['total'].iloc[0]
        open_data = funder_data[funder_data['category'] != 'no statement']['count'].sum()
        print(f"  {funder}: {open_data}/{total} ({open_data/total*100:.1f}%) with open data")

    # Create chart
    create_stacked_bar_chart(results_df, args.output)


if __name__ == '__main__':
    main()