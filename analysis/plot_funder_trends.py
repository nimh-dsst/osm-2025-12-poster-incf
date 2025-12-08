#!/usr/bin/env python3
"""
Plot Funder Trends from Pre-computed CSV Data

Generates line graphs from openss_funder_trends.py CSV output without
re-running the expensive calculation step.

Features:
- Consistent colors between counts and percentages graphs
- Multiple output formats: PNG, SVG, PDF
- Option to export legends as separate files for poster layout flexibility

Usage:
    # Generate both graphs as PNG
    python analysis/plot_funder_trends.py \
        --input-dir results/openss_funder_trends_v5 \
        --output-dir results/openss_funder_trends_v5 \
        --format png

    # Generate SVG with separate legend files
    python analysis/plot_funder_trends.py \
        --input-dir results/openss_funder_trends_v5 \
        --output-dir results/openss_funder_trends_v5 \
        --format svg \
        --separate-legends

    # Generate all formats
    python analysis/plot_funder_trends.py \
        --input-dir results/openss_funder_trends_v5 \
        --output-dir results/openss_funder_trends_v5 \
        --format png svg pdf

Author: INCF 2025 Poster Analysis
Date: 2025-12-08
"""

import argparse
import logging
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Color palette for top 10 funders (consistent across both graphs)
FUNDER_COLORS = [
    '#1f77b4',  # blue
    '#ff7f0e',  # orange
    '#2ca02c',  # green
    '#d62728',  # red
    '#9467bd',  # purple
    '#8c564b',  # brown
    '#e377c2',  # pink
    '#7f7f7f',  # gray
    '#bcbd22',  # olive
    '#17becf',  # cyan
]

# Line styles - all solid for clarity
LINE_STYLES = ['-'] * 10

# Markers for data points
MARKERS = ['o', 's', '^', 'D', 'v', '<', '>', 'p', 'h', '*']


def load_counts_data(input_dir: Path) -> pd.DataFrame:
    """Load counts CSV data."""
    counts_file = input_dir / 'openss_funder_counts_by_year.csv'
    if not counts_file.exists():
        raise FileNotFoundError(f"Counts file not found: {counts_file}")

    df = pd.read_csv(counts_file, index_col=0)
    logger.info(f"Loaded counts data: {len(df)} funders, {len(df.columns)} columns")
    return df


def load_percentages_data(input_dir: Path) -> pd.DataFrame:
    """Load percentages CSV data."""
    pct_file = input_dir / 'openss_funder_percentages_by_year.csv'
    if not pct_file.exists():
        raise FileNotFoundError(f"Percentages file not found: {pct_file}")

    df = pd.read_csv(pct_file, index_col=0)
    logger.info(f"Loaded percentages data: {len(df)} funders, {len(df.columns)} columns")
    return df


def get_top_funders_combined(counts_df: pd.DataFrame, pct_df: pd.DataFrame, n_each: int = 5) -> list:
    """
    Get combined top funders from both counts and percentages.

    Takes top N from each ranking and combines into a unique list.
    This ensures both high-volume funders and high-percentage funders are included.
    """
    # Get totals from counts
    if 'total' in counts_df.columns:
        count_totals = counts_df['total']
    else:
        year_cols = [c for c in counts_df.columns if str(c).isdigit()]
        count_totals = counts_df[year_cols].sum(axis=1)

    # Get average percentage (excluding zeros for fair comparison)
    year_cols_pct = [c for c in pct_df.columns if str(c).isdigit()]
    # Use mean of last 3 years for percentage ranking (2022-2024)
    recent_years = [c for c in year_cols_pct if int(c) >= 2022]
    if recent_years:
        pct_avg = pct_df[recent_years].mean(axis=1)
    else:
        pct_avg = pct_df[year_cols_pct].mean(axis=1)

    # Top N by count
    top_by_count = count_totals.nlargest(n_each).index.tolist()
    logger.info(f"Top {n_each} by count: {top_by_count}")

    # Top N by percentage
    top_by_pct = pct_avg.nlargest(n_each).index.tolist()
    logger.info(f"Top {n_each} by percentage: {top_by_pct}")

    # Combine and preserve order (counts first, then percentages)
    combined = []
    for f in top_by_count:
        if f not in combined:
            combined.append(f)
    for f in top_by_pct:
        if f not in combined:
            combined.append(f)

    logger.info(f"Combined top funders ({len(combined)} total): {combined}")
    return combined


def create_color_map(funders: list) -> dict:
    """Create consistent color mapping for funders."""
    return {funder: FUNDER_COLORS[i % len(FUNDER_COLORS)] for i, funder in enumerate(funders)}


def plot_counts_graph(df: pd.DataFrame, top_funders: list, color_map: dict,
                      output_path: Path, separate_legend: bool = False):
    """Create the counts line graph."""
    # Get year columns (exclude 'total')
    year_cols = [c for c in df.columns if c.isdigit() or (isinstance(c, int))]
    years = [int(c) for c in year_cols]

    fig, ax = plt.subplots(figsize=(12, 8))

    lines = []
    labels = []

    for i, funder in enumerate(top_funders):
        if funder in df.index:
            values = df.loc[funder, year_cols].values
            line, = ax.plot(years, values,
                           color=color_map[funder],
                           linestyle=LINE_STYLES[i % len(LINE_STYLES)],
                           marker=MARKERS[i % len(MARKERS)],
                           markersize=6,
                           linewidth=2,
                           label=funder)
            lines.append(line)
            labels.append(funder)

    ax.set_xlabel('Year', fontsize=12)
    ax.set_ylabel('Number of Open Data Articles', fontsize=12)
    ax.set_title('Open Data Research Articles from 10 Top Funders', fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3)
    ax.set_xlim(min(years) - 0.5, max(years) + 0.5)
    ax.set_ylim(bottom=0)

    # Format y-axis with comma separators
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format(int(x), ',')))

    if not separate_legend:
        ax.legend(loc='upper left', fontsize=9)

    plt.tight_layout()
    fig.savefig(output_path, dpi=300, bbox_inches='tight')
    logger.info(f"Saved counts graph: {output_path}")
    plt.close(fig)

    return lines, labels


def plot_percentages_graph(df: pd.DataFrame, top_funders: list, color_map: dict,
                           output_path: Path, separate_legend: bool = False):
    """Create the percentages line graph."""
    # Get year columns
    year_cols = [c for c in df.columns if c.isdigit() or (isinstance(c, int))]
    years = [int(c) for c in year_cols]

    fig, ax = plt.subplots(figsize=(12, 8))

    lines = []
    labels = []

    for i, funder in enumerate(top_funders):
        if funder in df.index:
            values = df.loc[funder, year_cols].values
            line, = ax.plot(years, values,
                           color=color_map[funder],
                           linestyle=LINE_STYLES[i % len(LINE_STYLES)],
                           marker=MARKERS[i % len(MARKERS)],
                           markersize=6,
                           linewidth=2,
                           label=funder)
            lines.append(line)
            labels.append(funder)

    ax.set_xlabel('Year', fontsize=12)
    ax.set_ylabel('Percent of Research Articles with Open Data', fontsize=12)
    ax.set_title('Percent of Research Articles w/Open Data from 10 Top Funders', fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3)
    ax.set_xlim(min(years) - 0.5, max(years) + 0.5)
    ax.set_ylim(bottom=0)

    # Format y-axis as percentage
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:.0f}%'))

    if not separate_legend:
        ax.legend(loc='upper left', fontsize=9)

    plt.tight_layout()
    fig.savefig(output_path, dpi=300, bbox_inches='tight')
    logger.info(f"Saved percentages graph: {output_path}")
    plt.close(fig)

    return lines, labels


def save_legend(lines, labels, output_path: Path, orientation: str = 'vertical'):
    """Save legend as a separate file."""
    # Create a figure just for the legend
    if orientation == 'vertical':
        fig_legend = plt.figure(figsize=(3, 4))
    else:
        fig_legend = plt.figure(figsize=(10, 1.5))

    ncol = 1 if orientation == 'vertical' else 5

    legend = fig_legend.legend(lines, labels,
                               loc='center',
                               ncol=ncol,
                               fontsize=10,
                               frameon=True,
                               fancybox=True,
                               shadow=False)

    fig_legend.savefig(output_path, dpi=300, bbox_inches='tight',
                       transparent=True)
    logger.info(f"Saved separate legend: {output_path}")
    plt.close(fig_legend)


def main():
    parser = argparse.ArgumentParser(
        description='Generate funder trends graphs from CSV data',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--input-dir', type=Path, required=True,
                        help='Directory containing CSV files from openss_funder_trends.py')
    parser.add_argument('--output-dir', type=Path, default=None,
                        help='Output directory (default: same as input)')
    parser.add_argument('--format', type=str, nargs='+', default=['png'],
                        choices=['png', 'svg', 'pdf'],
                        help='Output format(s) (default: png)')
    parser.add_argument('--separate-legends', action='store_true',
                        help='Save legends as separate files')
    parser.add_argument('--legend-orientation', type=str, default='vertical',
                        choices=['vertical', 'horizontal'],
                        help='Legend orientation when separate (default: vertical)')
    parser.add_argument('--top-n-each', type=int, default=5,
                        help='Number of top funders from each ranking (count & pct) (default: 5)')

    args = parser.parse_args()

    # Set output directory
    output_dir = args.output_dir or args.input_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 70)
    logger.info("PLOTTING FUNDER TRENDS FROM CSV DATA")
    logger.info("=" * 70)
    logger.info(f"Input directory: {args.input_dir}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Format(s): {', '.join(args.format)}")
    logger.info(f"Separate legends: {args.separate_legends}")

    # Load data
    counts_df = load_counts_data(args.input_dir)
    pct_df = load_percentages_data(args.input_dir)

    # Get combined top funders (top N from counts + top N from percentages)
    top_funders = get_top_funders_combined(counts_df, pct_df, args.top_n_each)
    color_map = create_color_map(top_funders)

    # Generate graphs for each format
    for fmt in args.format:
        logger.info(f"\nGenerating {fmt.upper()} output...")

        # Counts graph
        counts_path = output_dir / f'openss_funder_counts_by_year_v2.{fmt}'
        lines, labels = plot_counts_graph(
            counts_df, top_funders, color_map, counts_path,
            separate_legend=args.separate_legends
        )

        # Percentages graph
        pct_path = output_dir / f'openss_funder_percentages_by_year_v2.{fmt}'
        plot_percentages_graph(
            pct_df, top_funders, color_map, pct_path,
            separate_legend=args.separate_legends
        )

        # Save separate legends if requested
        if args.separate_legends:
            # Need to recreate lines for legend export
            fig_temp, ax_temp = plt.subplots()
            legend_lines = []
            legend_labels = []
            for i, funder in enumerate(top_funders):
                line, = ax_temp.plot([], [],
                                    color=color_map[funder],
                                    linestyle=LINE_STYLES[i % len(LINE_STYLES)],
                                    marker=MARKERS[i % len(MARKERS)],
                                    markersize=6,
                                    linewidth=2,
                                    label=funder)
                legend_lines.append(line)
                legend_labels.append(funder)
            plt.close(fig_temp)

            legend_path = output_dir / f'openss_funder_legend.{fmt}'
            save_legend(legend_lines, legend_labels, legend_path, args.legend_orientation)

    logger.info("\n" + "=" * 70)
    logger.info("COMPLETE")
    logger.info("=" * 70)

    # Print output files
    print(f"\nOutput files in {output_dir}:")
    for fmt in args.format:
        print(f"  - openss_funder_counts_by_year_v2.{fmt}")
        print(f"  - openss_funder_percentages_by_year_v2.{fmt}")
        if args.separate_legends:
            print(f"  - openss_funder_legend.{fmt}")


if __name__ == '__main__':
    main()
