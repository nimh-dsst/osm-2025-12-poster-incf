#!/usr/bin/env python3
"""
Build corpus summary statistics from metadata parquet files.

This script aggregates the full PMCOA corpus to provide denominators
for calculating open data percentages by journal, country, publisher, etc.

Output: Parquet file with aggregated counts by category.
"""

import argparse
import logging
from pathlib import Path
from typing import Dict, List
from collections import Counter

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def normalize_country(country_str: str) -> List[str]:
    """Normalize country strings, handling semicolon-separated lists.

    This function uses the same normalization logic as openss_journals_institutions.py
    to ensure consistent country matching between corpus summary and OpenSS results.
    """
    if pd.isna(country_str) or not country_str:
        return []

    # Comprehensive country name normalization mapping
    # Must match openss_journals_institutions.py for consistent percentage calculations
    country_map = {
        'CHINA': 'China',
        'P.R. CHINA': 'China',
        'PRC': 'China',
        "PEOPLE'S REPUBLIC OF CHINA": 'China',
        'UNITED STATES': 'USA',
        'UNITED STATES OF AMERICA': 'USA',
        'U.S.A': 'USA',
        'U.S.A.': 'USA',
        'U.S.': 'USA',
        'US': 'USA',
        'USA': 'USA',
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
        'THE NETHERLANDS': 'Netherlands',
        'HOLLAND': 'Netherlands',
        'SWITZERLAND': 'Switzerland',
        'SCHWEIZ': 'Switzerland',
        'SOUTH KOREA': 'South Korea',
        'KOREA': 'South Korea',
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
        'PAKISTAN': 'Pakistan',
        'INDONESIA': 'Indonesia',
        'VIETNAM': 'Vietnam',
        'HUNGARY': 'Hungary',
        'COLOMBIA': 'Colombia',
        'KENYA': 'Kenya',
        'BANGLADESH': 'Bangladesh',
        'SLOVENIA': 'Slovenia',
    }

    parts = str(country_str).split(';')
    countries = []

    for part in parts:
        part_clean = part.strip()
        part_upper = part_clean.upper()

        if part_upper in country_map:
            countries.append(country_map[part_upper])
        elif part_upper not in ['', 'NONE', 'NAN']:
            # Check for partial matches (e.g., "The Netherlands" contains "NETHERLANDS")
            found = False
            for pattern, normalized in country_map.items():
                if pattern in part_upper:
                    countries.append(normalized)
                    found = True
                    break
            if not found and part_clean:
                countries.append(part_clean.title())

    return list(set(countries))  # Deduplicate


def build_corpus_summary(metadata_dir: Path) -> pd.DataFrame:
    """
    Build summary statistics from all metadata parquet files.

    Returns DataFrame with columns:
    - category_type: journal | country | publisher | institution | year | license
    - category_value: the actual value
    - total_count: number of articles
    """
    logger.info(f"Building corpus summary from {metadata_dir}")

    # Find all parquet files
    files = sorted(metadata_dir.glob('*.parquet'))
    files = [f for f in files if not f.name.startswith('INDEX') and 'cache' not in f.name]
    logger.info(f"Found {len(files)} metadata parquet files")

    # Counters for each category
    counters = {
        'journal': Counter(),
        'country': Counter(),
        'publisher': Counter(),
        'institution': Counter(),
        'year': Counter(),
        'license': Counter(),
    }

    total_records = 0

    for i, f in enumerate(files, 1):
        logger.info(f"Processing {i}/{len(files)}: {f.name}")

        # Determine license type from filename
        if '_comm_' in f.name and 'noncomm' not in f.name:
            license_type = 'comm'
        elif '_noncomm_' in f.name:
            license_type = 'noncomm'
        elif '_other_' in f.name:
            license_type = 'other'
        else:
            license_type = 'unknown'

        # Read file
        df = pd.read_parquet(f)
        total_records += len(df)

        # Count by license
        counters['license'][license_type] += len(df)

        # Count by journal
        if 'journal' in df.columns:
            for val in df['journal'].dropna():
                if val:
                    counters['journal'][str(val).strip()] += 1

        # Count by publisher
        if 'publisher' in df.columns:
            for val in df['publisher'].dropna():
                if val:
                    counters['publisher'][str(val).strip()] += 1

        # Count by country (handle semicolon-separated)
        if 'affiliation_country' in df.columns:
            for val in df['affiliation_country'].dropna():
                for country in normalize_country(val):
                    counters['country'][country] += 1

        # Count by institution
        if 'affiliation_institution' in df.columns:
            for val in df['affiliation_institution'].dropna():
                if val:
                    # Take first institution if semicolon-separated
                    inst = str(val).split(';')[0].strip()
                    if inst:
                        counters['institution'][inst] += 1

        # Count by year (prefer epub, fallback to ppub)
        if 'year_epub' in df.columns or 'year_ppub' in df.columns:
            for _, row in df.iterrows():
                year = row.get('year_epub') or row.get('year_ppub')
                if pd.notna(year):
                    try:
                        year_int = int(year)
                        if 1900 <= year_int <= 2030:
                            counters['year'][str(year_int)] += 1
                    except (ValueError, TypeError):
                        pass

    logger.info(f"Total records processed: {total_records:,}")

    # Convert to DataFrame
    rows = []

    # Add total count
    rows.append({
        'category_type': 'total',
        'category_value': 'all',
        'total_count': total_records
    })

    # Add counts by category
    for cat_type, counter in counters.items():
        for cat_value, count in counter.most_common():
            rows.append({
                'category_type': cat_type,
                'category_value': cat_value,
                'total_count': count
            })

    df = pd.DataFrame(rows)

    # Print summary
    logger.info(f"\nCorpus Summary:")
    logger.info(f"  Total articles: {total_records:,}")
    logger.info(f"  Unique journals: {len(counters['journal']):,}")
    logger.info(f"  Unique countries: {len(counters['country']):,}")
    logger.info(f"  Unique publishers: {len(counters['publisher']):,}")
    logger.info(f"  Unique institutions: {len(counters['institution']):,}")
    logger.info(f"  Years covered: {len(counters['year']):,}")

    return df


def main():
    parser = argparse.ArgumentParser(
        description='Build corpus summary statistics from metadata files'
    )
    parser.add_argument('--metadata-dir', '-m', required=True,
                       help='Directory containing metadata parquet files')
    parser.add_argument('--output', '-o', required=True,
                       help='Output parquet file path')
    parser.add_argument('--csv', action='store_true',
                       help='Also output as CSV')

    args = parser.parse_args()

    metadata_dir = Path(args.metadata_dir)
    if not metadata_dir.exists():
        logger.error(f"Metadata directory not found: {metadata_dir}")
        return 1

    # Build summary
    df = build_corpus_summary(metadata_dir)

    # Save output
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(output_path, index=False)
    logger.info(f"Saved {len(df):,} rows to {output_path}")

    if args.csv:
        csv_path = output_path.with_suffix('.csv')
        df.to_csv(csv_path, index=False)
        logger.info(f"Also saved to {csv_path}")

    # Print top entries for each category
    print("\n" + "=" * 70)
    print("TOP ENTRIES BY CATEGORY")
    print("=" * 70)

    for cat_type in ['journal', 'country', 'publisher', 'license']:
        cat_df = df[df['category_type'] == cat_type].head(10)
        print(f"\n{cat_type.upper()}:")
        for _, row in cat_df.iterrows():
            print(f"  {row['total_count']:>10,}  {row['category_value']}")

    return 0


if __name__ == '__main__':
    exit(main())
