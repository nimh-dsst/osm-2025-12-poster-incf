#!/usr/bin/env python3
"""
OpenSS (Open SubSet) Analysis

Analyzes publications where oddpub v7 has is_open_data=true to find trends
and patterns in metadata. Key questions:
1. Are there funders acknowledged in open data pubs not in our top 10?
2. What are the top journals in this subset?
3. What are the top institutions in this subset?
4. What fields/subject areas are represented?

Usage:
    python analysis/openss_analysis.py \
        --oddpub-file ~/claude/osm-oddpub-out/oddpub_v7.2.3_all.parquet \
        --rtrans-dir ~/claude/pmcoaXMLs/rtrans_out_full_parquets \
        --funders-csv funder_analysis/biomedical_research_funders.csv \
        --output-dir results/openss

Author: INCF 2025 Poster Analysis
Date: 2025-12-01
"""

import argparse
import gc
import glob
import logging
import re
import sys
from collections import Counter
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Top 10 funders from our existing analysis
TOP_10_FUNDERS = {
    'NIH': 'National Institutes of Health',
    'EC': 'European Commission',
    'NSFC': 'National Natural Science Foundation of China',
    'DFG': 'German Research Foundation',
    'AMED': 'Japan Agency for Medical Research and Development',
    'WT': 'Wellcome Trust',
    'CIHR': 'Canadian Institutes of Health Research',
    'MRC': 'Medical Research Council',
    'HHMI': 'Howard Hughes Medical Institute',
    'BMGF': 'Bill & Melinda Gates Foundation'
}


def load_open_data_pmcids(oddpub_file: Path) -> set:
    """Load PMCIDs from oddpub output where is_open_data=true."""
    logger.info(f"Loading oddpub results from {oddpub_file}")

    df = pd.read_parquet(oddpub_file)
    logger.info(f"Loaded {len(df):,} records")

    # Filter to is_open_data=true
    open_data_df = df[df['is_open_data'] == True]
    logger.info(f"Found {len(open_data_df):,} with is_open_data=true ({100*len(open_data_df)/len(df):.2f}%)")

    # Normalize PMCIDs
    pmcids = set()
    for pmcid in open_data_df['pmcid'].dropna():
        pmcid_str = str(pmcid).strip()
        if not pmcid_str.startswith('PMC'):
            pmcid_str = f'PMC{pmcid_str}'
        pmcids.add(pmcid_str)

    logger.info(f"Got {len(pmcids):,} unique PMCIDs with open data")
    return pmcids


def load_funders_list(funders_csv: Path) -> pd.DataFrame:
    """Load the funders list for pattern matching."""
    logger.info(f"Loading funders from {funders_csv}")
    funders_df = pd.read_csv(funders_csv)
    logger.info(f"Loaded {len(funders_df)} funders")
    return funders_df


def extract_funder_mentions(fund_text: str, funders_df: pd.DataFrame) -> list:
    """
    Extract funder mentions from funding text.

    Returns list of (acronym, name) tuples for matched funders.
    """
    if pd.isna(fund_text) or not fund_text:
        return []

    fund_text_lower = str(fund_text).lower()
    matches = []

    for _, row in funders_df.iterrows():
        name = row['Name']
        acronym = row['Acronym']

        # Case-insensitive name match
        if name.lower() in fund_text_lower:
            matches.append((acronym, name))
        # Case-sensitive acronym match (to avoid false positives)
        elif acronym in str(fund_text):
            matches.append((acronym, name))

    return matches


def process_rtrans_files(rtrans_dir: Path, open_data_pmcids: set, funders_df: pd.DataFrame,
                         limit: int = None) -> dict:
    """
    Process rtrans files to extract metadata for open data publications.

    Returns dict with:
    - journals: Counter of journal names
    - publishers: Counter of publisher names
    - institutions: Counter of institutions
    - funders: Counter of funder acronyms
    - years: Counter of publication years
    - records: List of dicts with record details
    """
    parquet_files = sorted(glob.glob(f'{rtrans_dir}/*.parquet'))

    if limit:
        parquet_files = parquet_files[:limit]
        logger.info(f"Limited to first {limit} files for testing")

    logger.info(f"Processing {len(parquet_files)} rtrans parquet files")

    # Initialize counters
    journals = Counter()
    publishers = Counter()
    institutions = Counter()
    funders_counter = Counter()
    years = Counter()
    records = []

    # Columns we need
    cols_needed = [
        'pmcid_pmc', 'pmid', 'doi',
        'journal', 'publisher',
        'affiliation_institution',
        'fund_text', 'fund_pmc_institute', 'fund_pmc_source', 'fund_pmc_anysource',
        'year_epub', 'year_ppub'
    ]

    matched_count = 0
    total_processed = 0

    for i, pf in enumerate(parquet_files):
        try:
            # Load only needed columns if possible
            df = pd.read_parquet(pf)

            # Select available columns
            available_cols = [c for c in cols_needed if c in df.columns]
            df = df[available_cols]

            total_processed += len(df)

            # Match with open data PMCIDs
            if 'pmcid_pmc' in df.columns:
                # Normalize PMCIDs in dataframe
                df['pmcid_norm'] = df['pmcid_pmc'].fillna('').astype(str).str.strip()
                df.loc[~df['pmcid_norm'].str.startswith('PMC'), 'pmcid_norm'] = 'PMC' + df.loc[~df['pmcid_norm'].str.startswith('PMC'), 'pmcid_norm']

                # Filter to open data records
                mask = df['pmcid_norm'].isin(open_data_pmcids)
                matched_df = df[mask]

                if len(matched_df) > 0:
                    matched_count += len(matched_df)

                    # Extract journal counts
                    if 'journal' in matched_df.columns:
                        journal_counts = matched_df['journal'].dropna().value_counts()
                        journals.update(journal_counts.to_dict())

                    # Extract publisher counts
                    if 'publisher' in matched_df.columns:
                        pub_counts = matched_df['publisher'].dropna().value_counts()
                        publishers.update(pub_counts.to_dict())

                    # Extract institution counts (may have multiple per record)
                    if 'affiliation_institution' in matched_df.columns:
                        for inst_text in matched_df['affiliation_institution'].dropna():
                            # Split on common separators
                            if pd.notna(inst_text) and inst_text:
                                # Take first institution if multiple
                                insts = str(inst_text).split(';')
                                for inst in insts[:3]:  # Limit to first 3 to avoid noise
                                    inst = inst.strip()
                                    if inst and len(inst) > 3:  # Skip very short strings
                                        institutions[inst] += 1

                    # Extract year counts
                    if 'year_epub' in matched_df.columns:
                        # Filter out empty strings and non-numeric values
                        valid_years = matched_df['year_epub'].dropna()
                        valid_years = valid_years[valid_years.astype(str).str.strip() != '']
                        valid_years = pd.to_numeric(valid_years, errors='coerce').dropna()
                        year_counts = valid_years.value_counts()
                        years.update({int(y): c for y, c in year_counts.items()})

                    # Extract funder mentions from funding text columns
                    funding_cols = ['fund_text', 'fund_pmc_institute', 'fund_pmc_source', 'fund_pmc_anysource']
                    available_funding_cols = [c for c in funding_cols if c in matched_df.columns]

                    for _, row in matched_df.iterrows():
                        row_funders = set()
                        for col in available_funding_cols:
                            if pd.notna(row.get(col)):
                                matches = extract_funder_mentions(row[col], funders_df)
                                for acronym, _ in matches:
                                    row_funders.add(acronym)

                        for acronym in row_funders:
                            funders_counter[acronym] += 1

                        # Store record details for further analysis
                        records.append({
                            'pmcid': row.get('pmcid_norm', ''),
                            'pmid': row.get('pmid', ''),
                            'journal': row.get('journal', ''),
                            'publisher': row.get('publisher', ''),
                            'year': row.get('year_epub', ''),
                            'funders': list(row_funders)
                        })

            del df
            gc.collect()

            if (i + 1) % 100 == 0:
                logger.info(f"  Processed {i+1}/{len(parquet_files)} files, matched {matched_count:,} records")

        except Exception as e:
            logger.warning(f"Error processing {Path(pf).name}: {e}")

    logger.info(f"Finished processing {len(parquet_files)} files")
    logger.info(f"Total rows processed: {total_processed:,}")
    logger.info(f"Matched open data records: {matched_count:,}")

    return {
        'journals': journals,
        'publishers': publishers,
        'institutions': institutions,
        'funders': funders_counter,
        'years': years,
        'records': records,
        'matched_count': matched_count,
        'total_processed': total_processed
    }


def analyze_funders_beyond_top10(funders_counter: Counter, top_n: int = 30) -> pd.DataFrame:
    """
    Analyze funders beyond the top 10 list.

    Returns DataFrame with funder stats including whether they're in top 10.
    """
    rows = []
    for acronym, count in funders_counter.most_common(top_n):
        in_top10 = acronym in TOP_10_FUNDERS
        name = TOP_10_FUNDERS.get(acronym, acronym)
        rows.append({
            'acronym': acronym,
            'name': name,
            'count': count,
            'in_top10': in_top10
        })

    df = pd.DataFrame(rows)
    return df


def generate_report(results: dict, output_dir: Path, funders_df: pd.DataFrame):
    """Generate analysis report and save outputs."""
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. Top journals
    logger.info("Generating top journals report...")
    top_journals = pd.DataFrame(
        results['journals'].most_common(50),
        columns=['journal', 'count']
    )
    top_journals['percentage'] = 100 * top_journals['count'] / results['matched_count']
    top_journals.to_csv(output_dir / 'top_journals.csv', index=False)
    logger.info(f"  Saved top_journals.csv ({len(top_journals)} journals)")

    # 2. Top publishers
    logger.info("Generating top publishers report...")
    top_publishers = pd.DataFrame(
        results['publishers'].most_common(30),
        columns=['publisher', 'count']
    )
    top_publishers['percentage'] = 100 * top_publishers['count'] / results['matched_count']
    top_publishers.to_csv(output_dir / 'top_publishers.csv', index=False)
    logger.info(f"  Saved top_publishers.csv ({len(top_publishers)} publishers)")

    # 3. Top institutions
    logger.info("Generating top institutions report...")
    top_institutions = pd.DataFrame(
        results['institutions'].most_common(50),
        columns=['institution', 'count']
    )
    top_institutions['percentage'] = 100 * top_institutions['count'] / results['matched_count']
    top_institutions.to_csv(output_dir / 'top_institutions.csv', index=False)
    logger.info(f"  Saved top_institutions.csv ({len(top_institutions)} institutions)")

    # 4. Funders analysis
    logger.info("Generating funders analysis...")
    funders_analysis = analyze_funders_beyond_top10(results['funders'], top_n=50)
    funders_analysis['percentage'] = 100 * funders_analysis['count'] / results['matched_count']
    funders_analysis.to_csv(output_dir / 'funders_in_open_data.csv', index=False)

    # Identify funders NOT in top 10
    beyond_top10 = funders_analysis[~funders_analysis['in_top10']]
    beyond_top10.to_csv(output_dir / 'funders_beyond_top10.csv', index=False)
    logger.info(f"  Saved funders_in_open_data.csv ({len(funders_analysis)} funders)")
    logger.info(f"  Saved funders_beyond_top10.csv ({len(beyond_top10)} funders not in top 10)")

    # 5. Year distribution
    logger.info("Generating year distribution...")
    years_df = pd.DataFrame(
        sorted(results['years'].items()),
        columns=['year', 'count']
    )
    years_df.to_csv(output_dir / 'year_distribution.csv', index=False)
    logger.info(f"  Saved year_distribution.csv")

    # 6. Generate summary report
    logger.info("Generating summary report...")

    summary = []
    summary.append("=" * 70)
    summary.append("OPENSS ANALYSIS: Publications with Open Data (oddpub v7.2.3)")
    summary.append("=" * 70)
    summary.append("")
    summary.append(f"Total records matched: {results['matched_count']:,}")
    summary.append(f"Total rtrans records scanned: {results['total_processed']:,}")
    summary.append("")

    # Top 10 journals
    summary.append("-" * 70)
    summary.append("TOP 10 JOURNALS IN OPEN DATA PUBLICATIONS")
    summary.append("-" * 70)
    for i, row in top_journals.head(10).iterrows():
        summary.append(f"  {i+1:2}. {row['journal'][:60]:<60} {row['count']:>6,} ({row['percentage']:.2f}%)")
    summary.append("")

    # Top 10 publishers
    summary.append("-" * 70)
    summary.append("TOP 10 PUBLISHERS IN OPEN DATA PUBLICATIONS")
    summary.append("-" * 70)
    for i, row in top_publishers.head(10).iterrows():
        summary.append(f"  {i+1:2}. {row['publisher'][:60]:<60} {row['count']:>6,} ({row['percentage']:.2f}%)")
    summary.append("")

    # Top 10 funders
    summary.append("-" * 70)
    summary.append("TOP 10 FUNDERS IN OPEN DATA PUBLICATIONS")
    summary.append("-" * 70)
    for i, row in funders_analysis.head(10).iterrows():
        in_top10_marker = "✓" if row['in_top10'] else "NEW"
        summary.append(f"  {i+1:2}. {row['acronym']:<8} {row['count']:>6,} ({row['percentage']:.2f}%) [{in_top10_marker}]")
    summary.append("")

    # Funders NOT in top 10 that are significant
    summary.append("-" * 70)
    summary.append("SIGNIFICANT FUNDERS NOT IN TOP 10 LIST")
    summary.append("-" * 70)
    significant_new = beyond_top10[beyond_top10['count'] >= 100]
    if len(significant_new) > 0:
        for i, row in significant_new.iterrows():
            summary.append(f"  - {row['acronym']:<8} {row['name']:<50} {row['count']:>6,} ({row['percentage']:.2f}%)")
    else:
        summary.append("  (None with >= 100 publications)")
    summary.append("")

    # Top institutions
    summary.append("-" * 70)
    summary.append("TOP 10 INSTITUTIONS IN OPEN DATA PUBLICATIONS")
    summary.append("-" * 70)
    for i, row in top_institutions.head(10).iterrows():
        summary.append(f"  {i+1:2}. {row['institution'][:60]:<60} {row['count']:>6,}")
    summary.append("")

    # Year distribution
    summary.append("-" * 70)
    summary.append("YEAR DISTRIBUTION")
    summary.append("-" * 70)
    recent_years = years_df[years_df['year'] >= 2015]
    for _, row in recent_years.iterrows():
        bar = "█" * int(row['count'] / 1000)
        summary.append(f"  {int(row['year'])}: {row['count']:>6,} {bar}")
    summary.append("")
    summary.append("=" * 70)

    # Write summary
    summary_text = "\n".join(summary)
    with open(output_dir / 'summary_report.txt', 'w') as f:
        f.write(summary_text)

    print(summary_text)
    logger.info(f"Saved summary_report.txt to {output_dir}")


def main():
    parser = argparse.ArgumentParser(
        description='OpenSS Analysis: Analyze publications with open data',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--oddpub-file', type=Path, required=True,
                        help='Path to oddpub merged parquet file')
    parser.add_argument('--rtrans-dir', type=Path, required=True,
                        help='Directory containing rtrans parquet files')
    parser.add_argument('--funders-csv', type=Path, required=True,
                        help='Path to biomedical_research_funders.csv')
    parser.add_argument('--output-dir', type=Path, default=Path('results/openss'),
                        help='Output directory for results')
    parser.add_argument('--limit', type=int, default=None,
                        help='Limit number of rtrans files to process (for testing)')

    args = parser.parse_args()

    logger.info("=" * 70)
    logger.info("OPENSS ANALYSIS")
    logger.info("=" * 70)
    logger.info(f"oddpub file: {args.oddpub_file}")
    logger.info(f"rtrans dir: {args.rtrans_dir}")
    logger.info(f"funders csv: {args.funders_csv}")
    logger.info(f"output dir: {args.output_dir}")
    if args.limit:
        logger.info(f"limit: {args.limit} files")
    logger.info("")

    # Load open data PMCIDs from oddpub
    open_data_pmcids = load_open_data_pmcids(args.oddpub_file)

    # Load funders list
    funders_df = load_funders_list(args.funders_csv)

    # Process rtrans files
    results = process_rtrans_files(args.rtrans_dir, open_data_pmcids, funders_df, args.limit)

    # Generate reports
    generate_report(results, args.output_dir, funders_df)

    logger.info("Analysis complete!")


if __name__ == '__main__':
    main()
