#!/usr/bin/env python3
"""
OpenSS Analysis: Top Journals and Institutions in Open Data Publications

This script analyzes oddpub v7 is_open_data=true publications to identify:
- Top journals publishing open data research
- Top institutions/affiliations producing open data research
- Year-over-year trends for top journals

Uses rtrans parquet files for metadata (journal, affiliation_institution).
"""

import argparse
import logging
import os
import re
from collections import Counter
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def normalize_pmcid(pmcid: str) -> str:
    """Normalize PMCID to PMC######### format.

    Handles various formats:
    - PMC12345
    - 12345
    - PMCPMC12345.txt (from oddpub article column)
    """
    if pd.isna(pmcid):
        return None
    pmcid = str(pmcid).strip().upper()

    # Handle PMCPMC12345.txt format from oddpub
    if pmcid.startswith('PMCPMC'):
        pmcid = pmcid[3:]  # Remove first "PMC" prefix

    # Remove .txt suffix
    if pmcid.endswith('.TXT'):
        pmcid = pmcid[:-4]

    # Ensure PMC prefix
    if not pmcid.startswith('PMC'):
        pmcid = 'PMC' + pmcid

    return pmcid


def clean_journal_name(name: str) -> str:
    """Clean and normalize journal name."""
    if pd.isna(name) or not name:
        return None
    name = str(name).strip()
    # Remove trailing periods
    name = name.rstrip('.')
    # Normalize common variations
    name = re.sub(r'\s+', ' ', name)  # Multiple spaces to single
    return name if name else None


def extract_institution_name(affiliation: str) -> list:
    """Extract institution names from affiliation text.

    Returns list of potential institution names found in the affiliation.
    """
    if pd.isna(affiliation) or not affiliation:
        return []

    text = str(affiliation)
    institutions = []

    # Look for university patterns
    uni_pattern = r'((?:University|Universit[äéà]t|Universidad|Universidade|Universit[éy]|Université)\s+(?:of\s+)?[A-Z][a-zA-Z\-\']+(?:\s+[A-Z][a-zA-Z\-\']+)*)'
    uni_matches = re.findall(uni_pattern, text, re.IGNORECASE)
    institutions.extend(uni_matches)

    # Look for specific institution types
    inst_pattern = r'((?:National\s+)?(?:Institute|Center|Centre|Laboratory|Hospital|College|School)\s+(?:of|for|de|für)\s+[A-Z][a-zA-Z\-\']+(?:\s+[A-Z][a-zA-Z\-\']+)*)'
    inst_matches = re.findall(inst_pattern, text, re.IGNORECASE)
    institutions.extend(inst_matches)

    # Look for well-known institutions by name
    known_patterns = [
        r'(Harvard\s+(?:University|Medical\s+School)?)',
        r'(MIT|Massachusetts\s+Institute\s+of\s+Technology)',
        r'(Stanford\s+University)',
        r'(Johns\s+Hopkins\s+(?:University|Hospital)?)',
        r'(Yale\s+University)',
        r'(Oxford\s+(?:University)?)',
        r'(Cambridge\s+(?:University)?)',
        r'(NIH|National\s+Institutes\s+of\s+Health)',
        r'(CDC|Centers?\s+for\s+Disease\s+Control)',
        r'(Max\s+Planck\s+(?:Institute|Society))',
        r'(Chinese\s+Academy\s+of\s+Sciences?)',
        r'(CNRS)',
        r'(INSERM)',
        r'(Karolinska\s+Institut[e]?)',
    ]

    for pattern in known_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        institutions.extend(matches)

    # Clean up results
    cleaned = []
    for inst in institutions:
        inst = str(inst).strip()
        if len(inst) >= 5 and len(inst) <= 100:  # Reasonable length
            cleaned.append(inst)

    return cleaned


def normalize_country(country_str: str) -> list:
    """Normalize country strings, handling semicolon-separated lists.

    Returns list of normalized country names.
    """
    if pd.isna(country_str) or not country_str:
        return []

    # Normalize mapping
    country_map = {
        'CHINA': 'China',
        'P.R. CHINA': 'China',
        'PRC': 'China',
        'UNITED STATES': 'USA',
        'U.S.A': 'USA',
        'U.S.': 'USA',
        'US': 'USA',
        'UNITED KINGDOM': 'UK',
        'U.K.': 'UK',
        'UK': 'UK',
        'ENGLAND': 'UK',
        'SCOTLAND': 'UK',
        'WALES': 'UK',
        'GERMANY': 'Germany',
        'DEUTSCHLAND': 'Germany',
        'JAPAN': 'Japan',
        'FRANCE': 'France',
        'CANADA': 'Canada',
        'AUSTRALIA': 'Australia',
        'ITALY': 'Italy',
        'ITALIA': 'Italy',
        'SPAIN': 'Spain',
        'ESPAÑA': 'Spain',
        'NETHERLANDS': 'Netherlands',
        'HOLLAND': 'Netherlands',
        'SWITZERLAND': 'Switzerland',
        'SCHWEIZ': 'Switzerland',
        'SOUTH KOREA': 'South Korea',
        'KOREA, REPUBLIC OF': 'South Korea',
        'REPUBLIC OF KOREA': 'South Korea',
        'INDIA': 'India',
        'BRAZIL': 'Brazil',
        'BRASIL': 'Brazil',
        'SWEDEN': 'Sweden',
        'BELGIUM': 'Belgium',
        'AUSTRIA': 'Austria',
        'POLAND': 'Poland',
        'DENMARK': 'Denmark',
        'NORWAY': 'Norway',
        'FINLAND': 'Finland',
        'ISRAEL': 'Israel',
        'SINGAPORE': 'Singapore',
        'TAIWAN': 'Taiwan',
        'MEXICO': 'Mexico',
        'PORTUGAL': 'Portugal',
        'IRAN': 'Iran',
        'TURKEY': 'Turkey',
        'RUSSIA': 'Russia',
        'RUSSIAN FEDERATION': 'Russia',
        'GREECE': 'Greece',
        'IRELAND': 'Ireland',
        'CZECH REPUBLIC': 'Czech Republic',
        'CZECHIA': 'Czech Republic',
        'NEW ZEALAND': 'New Zealand',
        'ARGENTINA': 'Argentina',
        'CHILE': 'Chile',
        'THAILAND': 'Thailand',
        'MALAYSIA': 'Malaysia',
        'SOUTH AFRICA': 'South Africa',
        'EGYPT': 'Egypt',
        'NIGERIA': 'Nigeria',
        'SAUDI ARABIA': 'Saudi Arabia',
    }

    # Split by semicolon if present
    parts = str(country_str).split(';')
    countries = []

    for part in parts:
        part_upper = part.strip().upper()
        if part_upper in country_map:
            countries.append(country_map[part_upper])
        elif part_upper not in ['', 'NONE', 'NAN']:
            # Keep title case if not in mapping but valid
            # Also check for partial matches (e.g., "United States" in "United States of America")
            found = False
            for pattern, normalized in country_map.items():
                if pattern in part_upper:
                    countries.append(normalized)
                    found = True
                    break
            if not found:
                countries.append(part.strip().title())

    return countries


def extract_country(affiliation: str) -> str:
    """Extract country from affiliation text (fallback when no column available)."""
    if pd.isna(affiliation) or not affiliation:
        return None

    text = str(affiliation).upper()

    # Common country patterns
    patterns_to_country = {
        'USA': ['USA', 'U.S.A', 'UNITED STATES', 'U.S.'],
        'China': ['CHINA', 'P.R. CHINA', 'PRC'],
        'UK': ['UNITED KINGDOM', 'U.K.', 'ENGLAND', 'SCOTLAND', 'WALES'],
        'Germany': ['GERMANY', 'DEUTSCHLAND'],
        'Japan': ['JAPAN'],
        'France': ['FRANCE'],
        'Canada': ['CANADA'],
        'Australia': ['AUSTRALIA'],
        'Italy': ['ITALY', 'ITALIA'],
        'Spain': ['SPAIN', 'ESPAÑA'],
        'Netherlands': ['NETHERLANDS', 'HOLLAND'],
        'Switzerland': ['SWITZERLAND', 'SCHWEIZ'],
        'South Korea': ['SOUTH KOREA', 'KOREA, REPUBLIC OF', 'REPUBLIC OF KOREA'],
        'India': ['INDIA'],
        'Brazil': ['BRAZIL', 'BRASIL'],
        'Sweden': ['SWEDEN'],
    }

    for country, patterns in patterns_to_country.items():
        for pattern in patterns:
            if pattern in text:
                return country

    return None


def load_open_data_pmcids(oddpub_file: str) -> set:
    """Load PMCIDs with is_open_data=true from oddpub results."""
    logger.info(f"Loading oddpub results from {oddpub_file}")

    df = pd.read_parquet(oddpub_file)
    logger.info(f"Loaded {len(df):,} records")

    # Filter to open data
    open_data_df = df[df['is_open_data'] == True]
    logger.info(f"Found {len(open_data_df):,} with is_open_data=true ({len(open_data_df)/len(df)*100:.2f}%)")

    # Get unique PMCIDs - check article column first (has PMCPMC format)
    pmcids = set()
    for col in ['article', 'pmcid', 'filename']:
        if col in open_data_df.columns:
            for val in open_data_df[col].dropna().unique():
                pmcid = normalize_pmcid(val)
                if pmcid:
                    pmcids.add(pmcid)

    logger.info(f"Got {len(pmcids):,} unique PMCIDs with open data")
    return pmcids


def process_rtrans_files(
    rtrans_dir: str,
    open_data_pmcids: set,
    limit: int = None
) -> pd.DataFrame:
    """Process rtrans parquet files and extract journal/institution data."""

    rtrans_path = Path(rtrans_dir)
    parquet_files = sorted(rtrans_path.glob("*.parquet"))

    if limit:
        parquet_files = parquet_files[:limit]

    logger.info(f"Processing {len(parquet_files)} rtrans parquet files")

    all_records = []

    for i, pfile in enumerate(parquet_files):
        try:
            df = pd.read_parquet(pfile)

            # Find PMCID column
            pmcid_col = None
            for col in ['pmcid_pmc', 'pmcid', 'pmcid_uid']:
                if col in df.columns:
                    pmcid_col = col
                    break

            if not pmcid_col:
                continue

            # Normalize PMCIDs and filter to open data subset
            df['pmcid_norm'] = df[pmcid_col].apply(normalize_pmcid)
            matched = df[df['pmcid_norm'].isin(open_data_pmcids)]

            if len(matched) > 0:
                # Extract relevant columns
                records = []
                for _, row in matched.iterrows():
                    record = {
                        'pmcid': row['pmcid_norm'],
                        'journal': clean_journal_name(row.get('journal')),
                        'publisher': row.get('publisher'),
                        'affiliation': row.get('affiliation_institution'),
                        'country': row.get('affiliation_country'),
                        'year': row.get('year_epub') or row.get('year_ppub'),
                    }
                    records.append(record)

                all_records.extend(records)

            if (i + 1) % 100 == 0:
                logger.info(f"  Processed {i+1}/{len(parquet_files)} files, collected {len(all_records):,} records")

        except Exception as e:
            logger.warning(f"Error processing {pfile}: {e}")
            continue

    logger.info(f"Collected {len(all_records):,} records total")
    return pd.DataFrame(all_records)


def analyze_journals(df: pd.DataFrame) -> dict:
    """Analyze journal distribution in open data publications."""

    # Count journals
    journal_counts = Counter(df['journal'].dropna())

    # Top journals overall
    top_journals = pd.DataFrame([
        {'journal': j, 'count': c}
        for j, c in journal_counts.most_common(100)
    ])

    # Journal trends by year
    journal_year = df.groupby(['journal', 'year']).size().reset_index(name='count')

    # Get top 20 journals for trend analysis
    top_20_journals = [j for j, _ in journal_counts.most_common(20)]
    journal_trends = journal_year[journal_year['journal'].isin(top_20_journals)]

    return {
        'top_journals': top_journals,
        'journal_trends': journal_trends,
        'total_with_journal': df['journal'].notna().sum(),
    }


def analyze_institutions(df: pd.DataFrame) -> dict:
    """Analyze institution distribution in open data publications."""

    # Extract institutions from affiliation text
    institution_counts = Counter()
    country_counts = Counter()

    for _, row in df.iterrows():
        affiliation = row.get('affiliation')

        # Extract countries from column (handles semicolon-separated lists)
        country_col = row.get('country')
        countries_found = normalize_country(country_col)

        # Fall back to parsing affiliation text if no column data
        if not countries_found:
            parsed_country = extract_country(affiliation)
            if parsed_country:
                countries_found = [parsed_country]

        # Count each unique country once per record
        for country in set(countries_found):
            country_counts[country] += 1

        # Extract institutions
        institutions = extract_institution_name(affiliation)
        for inst in institutions:
            institution_counts[inst] += 1

    # Top institutions
    top_institutions = pd.DataFrame([
        {'institution': i, 'count': c}
        for i, c in institution_counts.most_common(100)
    ])

    # Top countries
    top_countries = pd.DataFrame([
        {'country': c, 'count': n}
        for c, n in country_counts.most_common(50)
    ])

    return {
        'top_institutions': top_institutions,
        'top_countries': top_countries,
        'total_with_affiliation': df['affiliation'].notna().sum(),
    }


def analyze_publishers(df: pd.DataFrame) -> dict:
    """Analyze publisher distribution."""

    publisher_counts = Counter(df['publisher'].dropna())

    top_publishers = pd.DataFrame([
        {'publisher': p, 'count': c}
        for p, c in publisher_counts.most_common(50)
    ])

    return {
        'top_publishers': top_publishers,
        'total_with_publisher': df['publisher'].notna().sum(),
    }


def save_results(results: dict, output_dir: str):
    """Save analysis results to files."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Save CSVs
    results['top_journals'].to_csv(output_path / 'top_journals.csv', index=False)
    results['journal_trends'].to_csv(output_path / 'journal_trends.csv', index=False)
    results['top_institutions'].to_csv(output_path / 'top_institutions.csv', index=False)
    results['top_countries'].to_csv(output_path / 'top_countries.csv', index=False)
    results['top_publishers'].to_csv(output_path / 'top_publishers.csv', index=False)

    # Write summary
    with open(output_path / 'journals_institutions_summary.txt', 'w') as f:
        f.write("=" * 70 + "\n")
        f.write("OPENSS JOURNALS & INSTITUTIONS ANALYSIS\n")
        f.write("=" * 70 + "\n\n")

        f.write(f"Total open data records analyzed: {results['total_records']:,}\n")
        f.write(f"Records with journal info: {results['total_with_journal']:,}\n")
        f.write(f"Records with affiliation info: {results['total_with_affiliation']:,}\n")
        f.write(f"Records with publisher info: {results['total_with_publisher']:,}\n\n")

        f.write("-" * 70 + "\n")
        f.write("TOP 30 JOURNALS\n")
        f.write("-" * 70 + "\n")
        for _, row in results['top_journals'].head(30).iterrows():
            f.write(f"  {row['count']:>6}  {row['journal']}\n")
        f.write("\n")

        f.write("-" * 70 + "\n")
        f.write("TOP 30 INSTITUTIONS\n")
        f.write("-" * 70 + "\n")
        for _, row in results['top_institutions'].head(30).iterrows():
            f.write(f"  {row['count']:>6}  {row['institution']}\n")
        f.write("\n")

        f.write("-" * 70 + "\n")
        f.write("TOP 20 COUNTRIES\n")
        f.write("-" * 70 + "\n")
        for _, row in results['top_countries'].head(20).iterrows():
            f.write(f"  {row['count']:>6}  {row['country']}\n")
        f.write("\n")

        f.write("-" * 70 + "\n")
        f.write("TOP 20 PUBLISHERS\n")
        f.write("-" * 70 + "\n")
        for _, row in results['top_publishers'].head(20).iterrows():
            f.write(f"  {row['count']:>6}  {row['publisher']}\n")
        f.write("\n")

        f.write("=" * 70 + "\n")

    logger.info(f"Results saved to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description='Analyze journals and institutions in open data publications'
    )
    parser.add_argument(
        '--oddpub-file',
        default='~/claude/pmcoaXMLs/oddpub_merged/oddpub_v7.2.3_all.parquet',
        help='Path to merged oddpub parquet file'
    )
    parser.add_argument(
        '--rtrans-dir',
        default='~/claude/pmcoaXMLs/rtrans_out_full_parquets',
        help='Directory containing rtrans parquet files'
    )
    parser.add_argument(
        '--output-dir',
        default='results/openss_journals',
        help='Output directory for results'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Limit number of rtrans files to process (for testing)'
    )

    args = parser.parse_args()

    # Expand paths
    oddpub_file = os.path.expanduser(args.oddpub_file)
    rtrans_dir = os.path.expanduser(args.rtrans_dir)

    logger.info("=" * 70)
    logger.info("OPENSS JOURNALS & INSTITUTIONS ANALYSIS")
    logger.info("=" * 70)
    logger.info(f"oddpub file: {oddpub_file}")
    logger.info(f"rtrans dir: {rtrans_dir}")
    logger.info(f"output dir: {args.output_dir}")
    logger.info("")

    # Load open data PMCIDs
    open_data_pmcids = load_open_data_pmcids(oddpub_file)

    # Process rtrans files
    df = process_rtrans_files(rtrans_dir, open_data_pmcids, args.limit)

    if len(df) == 0:
        logger.error("No matching records found")
        return

    # Analyze journals
    logger.info("Analyzing journals...")
    journal_results = analyze_journals(df)

    # Analyze institutions
    logger.info("Analyzing institutions...")
    institution_results = analyze_institutions(df)

    # Analyze publishers
    logger.info("Analyzing publishers...")
    publisher_results = analyze_publishers(df)

    # Combine results
    results = {
        'total_records': len(df),
        **journal_results,
        **institution_results,
        **publisher_results,
    }

    # Save results
    save_results(results, args.output_dir)

    # Print summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"\nTotal open data records: {len(df):,}")
    print(f"\nTop 10 Journals:")
    for _, row in results['top_journals'].head(10).iterrows():
        print(f"  {row['count']:>6}  {row['journal']}")
    print(f"\nTop 10 Countries:")
    for _, row in results['top_countries'].head(10).iterrows():
        print(f"  {row['count']:>6}  {row['country']}")


if __name__ == '__main__':
    main()
