#!/usr/bin/env python3
"""
Generate corpus statistics table for INCF poster.

Outputs:
- results/corpus_stats.csv - CSV with statistics
- results/corpus_stats.tex - LaTeX table

Statistics included:
1. Serghiou et al. 2021 PMCIDs
2. PMC-OA June 2025 baseline total
3. OddPub v7.2.3 processed count
4. Open data detected count
5. Open data with funder identified
6. Corpus articles with canonical funder
7. Funder-year data points in trends analysis
"""

import argparse
import os
import sys

import duckdb
import pandas as pd


def get_stats(
    serghiou_path: str,
    registry_path: str,
    oddpub_path: str,
    openss_explore_dir: str,
    corpus_totals_path: str,
    funder_trends_path: str,
) -> dict:
    """Gather all statistics."""
    stats = {}

    conn = duckdb.connect(':memory:')

    # 1. Serghiou et al. 2021 PMCIDs
    print("Loading Serghiou et al. 2021...")
    serghiou_count = conn.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{serghiou_path}')
    """).fetchone()[0]
    stats['serghiou_pmcids'] = serghiou_count
    print(f"  Serghiou PMCIDs: {serghiou_count:,}")

    # 2. PMC-OA June 2025 baseline (from registry)
    print("Loading PMC-OA registry...")
    conn.execute(f"ATTACH '{registry_path}' AS registry (READ_ONLY)")

    pmcoa_total = conn.execute("SELECT COUNT(*) FROM registry.pmcids").fetchone()[0]
    stats['pmcoa_total'] = pmcoa_total
    print(f"  PMC-OA total: {pmcoa_total:,}")

    # License breakdown
    license_counts = conn.execute("""
        SELECT license, COUNT(*) as cnt
        FROM registry.pmcids
        GROUP BY license
        ORDER BY cnt DESC
    """).fetchall()
    for lic, cnt in license_counts:
        stats[f'pmcoa_{lic}'] = cnt
        print(f"    {lic}: {cnt:,}")

    # 3. OddPub v7.2.3 processed
    oddpub_processed = conn.execute("""
        SELECT COUNT(*) FROM registry.pmcids WHERE has_oddpub_v7 = true
    """).fetchone()[0]
    stats['oddpub_processed'] = oddpub_processed
    print(f"  OddPub v7.2.3 processed: {oddpub_processed:,}")

    # 4. Open data detected (is_open_data = true)
    print("Loading OddPub results...")
    open_data_count = conn.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{oddpub_path}')
        WHERE is_open_data = true
    """).fetchone()[0]
    stats['open_data_count'] = open_data_count
    print(f"  Open data detected: {open_data_count:,}")

    # Also get total in oddpub file
    oddpub_total = conn.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{oddpub_path}')
    """).fetchone()[0]
    stats['oddpub_total'] = oddpub_total

    # 5. Open data with funder identified (from openss_explore)
    print("Loading OpenSS funder exploration results...")
    openss_funders_path = os.path.join(openss_explore_dir, 'all_potential_funders.csv')
    if os.path.exists(openss_funders_path):
        # Count unique articles with funders from the exploration
        # This is trickier - need to check if there's a count in the output
        openss_matched_path = os.path.join(openss_explore_dir, 'openss_with_fund_text.parquet')
        if os.path.exists(openss_matched_path):
            openss_with_funder = conn.execute(f"""
                SELECT COUNT(*) FROM read_parquet('{openss_matched_path}')
            """).fetchone()[0]
        else:
            # Fall back to counting from funders CSV
            funders_df = pd.read_csv(openss_funders_path)
            openss_with_funder = funders_df['count'].sum() if 'count' in funders_df.columns else len(funders_df)
        stats['openss_with_funder'] = openss_with_funder
        print(f"  Open data with funder text: {openss_with_funder:,}")
    else:
        stats['openss_with_funder'] = 'N/A'
        print(f"  Open data with funder: N/A (file not found)")

    # 6. Corpus articles with canonical funder
    print("Loading canonical funder corpus totals...")
    if os.path.exists(corpus_totals_path):
        corpus_totals = conn.execute(f"""
            SELECT SUM(total) as total_with_funder
            FROM read_parquet('{corpus_totals_path}')
        """).fetchone()[0]
        stats['corpus_with_canonical_funder'] = int(corpus_totals) if corpus_totals else 0
        print(f"  Corpus with canonical funder: {stats['corpus_with_canonical_funder']:,}")

        # Also get unique articles (some may have multiple funders)
        # For now, use sum as approximation
    else:
        stats['corpus_with_canonical_funder'] = 'N/A'
        print(f"  Corpus with canonical funder: N/A (file not found)")

    # 7. Funder-year data points
    print("Loading funder trends data...")
    if os.path.exists(funder_trends_path):
        trends_df = pd.read_csv(funder_trends_path, index_col=0)
        # Data points = (rows - header) x (columns - index)
        n_years = len(trends_df)
        n_funders = len(trends_df.columns)
        data_points = n_years * n_funders
        stats['funder_year_datapoints'] = data_points
        stats['funder_trends_years'] = n_years
        stats['funder_trends_funders'] = n_funders
        print(f"  Funder-year data points: {data_points:,} ({n_funders} funders x {n_years} years)")
    else:
        stats['funder_year_datapoints'] = 'N/A'
        print(f"  Funder-year data points: N/A (file not found)")

    conn.close()
    return stats


def write_csv(stats: dict, output_path: str):
    """Write statistics to CSV."""
    rows = [
        ('Metric', 'Value', 'Description'),
        ('serghiou_pmcids', stats['serghiou_pmcids'], 'Serghiou et al. 2021 PMCID count'),
        ('pmcoa_total', stats['pmcoa_total'], 'PMC-OA June 2025 baseline total'),
        ('pmcoa_comm', stats.get('pmcoa_comm', 'N/A'), 'PMC-OA commercial license'),
        ('pmcoa_noncomm', stats.get('pmcoa_noncomm', 'N/A'), 'PMC-OA non-commercial license'),
        ('pmcoa_other', stats.get('pmcoa_other', 'N/A'), 'PMC-OA other license'),
        ('oddpub_processed', stats['oddpub_processed'], 'XMLs processed with OddPub v7.2.3'),
        ('open_data_count', stats['open_data_count'], 'Articles with is_open_data=true'),
        ('openss_with_funder', stats['openss_with_funder'], 'Open data articles with funder text'),
        ('corpus_with_canonical_funder', stats['corpus_with_canonical_funder'], 'Corpus articles with canonical funder'),
        ('funder_year_datapoints', stats['funder_year_datapoints'], 'Funder x Year data points in trends'),
    ]

    with open(output_path, 'w') as f:
        for row in rows:
            f.write(','.join(str(x) for x in row) + '\n')

    print(f"\nCSV written to: {output_path}")


def write_latex(stats: dict, output_path: str):
    """Write statistics to LaTeX table."""

    def fmt(val):
        """Format number with thousands separator."""
        if isinstance(val, int):
            return f"{val:,}".replace(',', '{,}')
        return str(val)

    latex = r"""\begin{table}[htbp]
\centering
\caption{Corpus Statistics for Open Science Metrics Analysis}
\label{tab:corpus_stats}
\begin{tabular}{lr}
\toprule
\textbf{Metric} & \textbf{Count} \\
\midrule
Serghiou et al. (2021) PMCIDs & """ + fmt(stats['serghiou_pmcids']) + r""" \\
\midrule
\multicolumn{2}{l}{\textit{PMC Open Access (June 2025 Baseline)}} \\
\quad Total & """ + fmt(stats['pmcoa_total']) + r""" \\
\quad Commercial license & """ + fmt(stats.get('pmcoa_comm', 'N/A')) + r""" \\
\quad Non-commercial license & """ + fmt(stats.get('pmcoa_noncomm', 'N/A')) + r""" \\
\quad Other license & """ + fmt(stats.get('pmcoa_other', 'N/A')) + r""" \\
\midrule
\multicolumn{2}{l}{\textit{OddPub v7.2.3 Analysis}} \\
\quad XMLs processed & """ + fmt(stats['oddpub_processed']) + r""" \\
\quad Open data detected & """ + fmt(stats['open_data_count']) + r""" \\
\midrule
\multicolumn{2}{l}{\textit{Funder Analysis}} \\
\quad Open data with funder text & """ + fmt(stats['openss_with_funder']) + r""" \\
\quad Corpus with canonical funder & """ + fmt(stats['corpus_with_canonical_funder']) + r""" \\
\quad Funder $\times$ Year data points & """ + fmt(stats['funder_year_datapoints']) + r""" \\
\bottomrule
\end{tabular}
\end{table}
"""

    with open(output_path, 'w') as f:
        f.write(latex)

    print(f"LaTeX written to: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Generate corpus statistics table")
    parser.add_argument(
        "--serghiou",
        default=os.path.expanduser("~/claude/datafiles/serghiou_et_al_2021_pmcids.parquet"),
        help="Path to Serghiou et al. 2021 PMCIDs parquet",
    )
    parser.add_argument(
        "--registry",
        default="hpc_scripts/pmcid_registry.duckdb",
        help="Path to PMCID registry DuckDB",
    )
    parser.add_argument(
        "--oddpub",
        default=os.path.expanduser("~/claude/pmcoaXMLs/oddpub_merged/oddpub_v7.2.3_all.parquet"),
        help="Path to merged OddPub results parquet",
    )
    parser.add_argument(
        "--openss-explore-dir",
        default="results/openss_explore_v2",
        help="Directory with OpenSS funder exploration results",
    )
    parser.add_argument(
        "--corpus-totals",
        default="results/canonical_funder_corpus_totals.parquet",
        help="Path to canonical funder corpus totals parquet",
    )
    parser.add_argument(
        "--funder-trends",
        default="results/openss_funder_trends_v3/openss_funder_counts_by_year.csv",
        help="Path to funder trends CSV",
    )
    parser.add_argument(
        "--output-dir",
        default="results",
        help="Output directory for CSV and LaTeX files",
    )
    args = parser.parse_args()

    # Gather statistics
    stats = get_stats(
        serghiou_path=args.serghiou,
        registry_path=args.registry,
        oddpub_path=args.oddpub,
        openss_explore_dir=args.openss_explore_dir,
        corpus_totals_path=args.corpus_totals,
        funder_trends_path=args.funder_trends,
    )

    # Write outputs
    os.makedirs(args.output_dir, exist_ok=True)
    write_csv(stats, os.path.join(args.output_dir, 'corpus_stats.csv'))
    write_latex(stats, os.path.join(args.output_dir, 'corpus_stats.tex'))

    # Print summary
    print("\n" + "=" * 60)
    print("CORPUS STATISTICS SUMMARY")
    print("=" * 60)
    for key, val in stats.items():
        if isinstance(val, int):
            print(f"  {key}: {val:,}")
        else:
            print(f"  {key}: {val}")


if __name__ == "__main__":
    main()
