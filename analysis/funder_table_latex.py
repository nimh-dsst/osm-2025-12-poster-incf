#!/usr/bin/env python3
"""
Generate LaTeX Table from Funder Data Sharing Summary

Outputs a formatted LaTeX table with:
- Funder names with country in parentheses
- Conditional formatting (red-white-blue color scale) for numeric columns
- Log scale for total_pubs, linear scale for data_sharing_pct

Usage:
    # Sort by total publications, no coloring
    python analysis/funder_table_latex.py \
        --input results/funder_data_sharing_summary_v3_all.csv \
        --aliases funder_analysis/funder_aliases_v3.csv \
        --sort-by total_pubs \
        --output results/funder_table.tex

    # Sort by percentage, with conditional formatting on both columns
    python analysis/funder_table_latex.py \
        --input results/funder_data_sharing_summary_v3_all.csv \
        --aliases funder_analysis/funder_aliases_v3.csv \
        --sort-by data_sharing_pct \
        --color-pubs \
        --color-pct \
        --output results/funder_table.tex

Author: INCF 2025 Poster Analysis
Date: 2025-12-08
"""

import argparse
import math
from pathlib import Path

import pandas as pd


def load_country_mapping(aliases_path: Path) -> dict:
    """Load funder -> country mapping from aliases file."""
    df = pd.read_csv(aliases_path)
    # Get unique canonical_name -> country mapping
    mapping = df.groupby('canonical_name')['country'].first().to_dict()
    return mapping


def get_color_bwr(value: float, min_val: float, max_val: float,
                  use_log: bool = False) -> str:
    """
    Get blue-white-red color for conditional formatting.

    Returns LaTeX color definition for cellcolor.
    - Low values: Blue (light blue)
    - Mid values: White
    - High values: Red/Pink (salmon)
    """
    if use_log:
        # Log scale
        if value <= 0:
            value = 1
        value = math.log10(value)
        min_val = math.log10(max(min_val, 1))
        max_val = math.log10(max(max_val, 1))

    # Normalize to 0-1 range
    if max_val == min_val:
        normalized = 0.5
    else:
        normalized = (value - min_val) / (max_val - min_val)

    # Map to color: 0 = blue, 0.5 = white, 1 = red
    if normalized < 0.5:
        # Blue to white (low to mid)
        # Blue component stays high, red/green increase
        t = normalized * 2  # 0 to 1
        r = 0.7 + 0.3 * t  # 0.7 to 1.0
        g = 0.7 + 0.3 * t  # 0.7 to 1.0
        b = 1.0
    else:
        # White to red (mid to high)
        # Red component stays high, green/blue decrease
        t = (normalized - 0.5) * 2  # 0 to 1
        r = 1.0
        g = 1.0 - 0.4 * t  # 1.0 to 0.6
        b = 1.0 - 0.6 * t  # 1.0 to 0.4

    # Convert to 0-255 range for xcolor
    r_int = int(r * 255)
    g_int = int(g * 255)
    b_int = int(b * 255)

    return f"{{rgb,255:red,{r_int};green,{g_int};blue,{b_int}}}"


def escape_latex(text: str) -> str:
    """Escape special LaTeX characters."""
    replacements = [
        ('&', r'\&'),
        ('%', r'\%'),
        ('$', r'\$'),
        ('#', r'\#'),
        ('_', r'\_'),
        ('{', r'\{'),
        ('}', r'\}'),
        ('~', r'\textasciitilde{}'),
        ('^', r'\textasciicircum{}'),
    ]
    for old, new in replacements:
        text = text.replace(old, new)
    return text


def format_number_with_comma(n: int) -> str:
    """Format number with comma separators."""
    return f"{n:,}"


def generate_latex_table(df: pd.DataFrame, country_map: dict,
                         sort_by: str = 'total_pubs',
                         color_pubs: bool = False,
                         color_pct: bool = False,
                         descending: bool = True) -> str:
    """Generate LaTeX table from dataframe."""

    # Sort dataframe
    df = df.sort_values(sort_by, ascending=not descending).reset_index(drop=True)

    # Get min/max for color scaling
    pubs_min = df['total_pubs'].min()
    pubs_max = df['total_pubs'].max()
    pct_min = df['data_sharing_pct'].min()
    pct_max = df['data_sharing_pct'].max()

    # Build LaTeX
    lines = []

    # Preamble with group wrapper
    lines.append(r"% Requires: \usepackage[table]{xcolor}, \usepackage{siunitx}, \usepackage{booktabs}")
    lines.append(r"% Define COL5 color in preamble, e.g.: \definecolor{COL5}{HTML}{4472C4}")
    lines.append("")
    lines.append(r"\begingroup")
    lines.append(r"\arrayrulecolor{COL5}")
    lines.append(r"\rowcolors{2}{COL5!10}{white}")
    lines.append(r"\setlength{\tabcolsep}{24pt}")
    lines.append(r"\begin{tabular}{llS[table-format=6.0]S[table-format=5.0]S[table-format=2.2]}")
    lines.append(r"\toprule")
    # Two-line header
    lines.append(r"\textbf{} & \textbf{}")
    lines.append(r"& \textbf{Research Pubs}")
    lines.append(r"& \textbf{Pubs w/}")
    lines.append(r"& \textbf{\% Pubs w/} \\")
    lines.append(r"\textbf{Funder Name}")
    lines.append(r"& \textbf{Country}")
    lines.append(r"& \textbf{2010--2024}")
    lines.append(r"& \textbf{Data Sharing}")
    lines.append(r"& \textbf{Data Sharing} \\")
    lines.append(r"\midrule")

    # Data rows
    for row_idx, (_, row) in enumerate(df.iterrows()):
        funder = row['funder']
        total_pubs = int(row['total_pubs'])
        data_pubs = int(row['data_sharing_pubs'])
        pct = row['data_sharing_pct']

        # Get country
        country = country_map.get(funder, '')

        # Format funder name (no country in parens - separate column now)
        funder_escaped = escape_latex(funder)

        # Format numbers (no commas for siunitx S columns)
        pubs_str = str(total_pubs)
        data_pubs_str = str(data_pubs)
        pct_str = f"{pct:.1f}\\%"

        # Apply conditional formatting (blue-white-red: low=blue, high=red)
        if color_pubs:
            color = get_color_bwr(total_pubs, pubs_min, pubs_max, use_log=True)
            pubs_str = f"\\cellcolor{color}{pubs_str}"

        if color_pct:
            color = get_color_bwr(pct, pct_min, pct_max, use_log=False)
            pct_str = f"\\cellcolor{color}{pct_str}"

        lines.append(f"{funder_escaped} & {country} & {pubs_str} & {data_pubs_str} & {pct_str} \\\\")

    lines.append(r"\bottomrule")
    lines.append(r"\end{tabular}")
    lines.append(r"\arrayrulecolor{black}")
    lines.append(r"\endgroup")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description='Generate LaTeX table from funder data sharing summary',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--input', '-i', type=Path, required=True,
                        help='Input CSV file (funder_data_sharing_summary_v3_all.csv)')
    parser.add_argument('--aliases', '-a', type=Path, required=True,
                        help='Funder aliases CSV with country info')
    parser.add_argument('--output', '-o', type=Path, default=None,
                        help='Output LaTeX file (default: stdout)')
    parser.add_argument('--sort-by', type=str, default='total_pubs',
                        choices=['total_pubs', 'data_sharing_pct', 'data_sharing_pubs'],
                        help='Column to sort by (default: total_pubs)')
    parser.add_argument('--ascending', action='store_true',
                        help='Sort in ascending order (default: descending)')
    parser.add_argument('--color-pubs', action='store_true',
                        help='Apply conditional formatting to total_pubs column (log scale)')
    parser.add_argument('--color-pct', action='store_true',
                        help='Apply conditional formatting to data_sharing_pct column (linear scale)')
    parser.add_argument('--limit', '-n', type=int, default=None,
                        help='Limit to top N funders after sorting')

    args = parser.parse_args()

    # Load data
    df = pd.read_csv(args.input)
    country_map = load_country_mapping(args.aliases)

    # Apply limit after sorting
    if args.limit:
        df = df.sort_values(args.sort_by, ascending=args.ascending).head(args.limit)

    # Generate table
    latex = generate_latex_table(
        df, country_map,
        sort_by=args.sort_by,
        color_pubs=args.color_pubs,
        color_pct=args.color_pct,
        descending=not args.ascending
    )

    # Output
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(latex)
        print(f"Wrote LaTeX table to {args.output}")
    else:
        print(latex)


if __name__ == '__main__':
    main()
