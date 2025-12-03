#!/usr/bin/env python3
"""
Build dashboard data file from PMC Open Access corpus.

This script produces a parquet file for the OSM dashboard containing:
- pmid: PubMed ID
- journal: Journal name
- affiliation_country: Country from author affiliation
- is_open_data: Boolean from oddpub v7.2.3
- is_open_code: Boolean from oddpub v7.2.3
- year: Publication year (epub preferred)
- funder: List of matched canonical funders
- data_tags: Metadata tags (license type, source)
- created_at: Timestamp

Input sources:
- PMC filelist CSVs (for PMCID list with license info)
- rtrans parquets (for journal, country, year, funding text)
- oddpub merged parquet (for is_open_data/code)
- Funder aliases CSV (for canonical funder matching)

Usage:
    python build_dashboard_data.py \
        --filelist-dir ~/claude/pmcoaXMLs/raw_download \
        --rtrans-dir ~/claude/pmcoaXMLs/rtrans_out_full_parquets \
        --oddpub-file ~/claude/pmcoaXMLs/oddpub_merged/oddpub_v7.2.3_all.parquet \
        --output ~/claude/matches_new4.parquet \
        --licenses comm,noncomm
"""

import argparse
import os
import re
import sys
from datetime import datetime
from glob import glob
from pathlib import Path

import numpy as np
import pandas as pd

# Add parent directory for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from funder_analysis.normalize_funders import FunderNormalizer


def parse_args():
    parser = argparse.ArgumentParser(description="Build dashboard data file")
    parser.add_argument(
        "--filelist-dir",
        required=True,
        help="Directory containing PMC filelist CSVs",
    )
    parser.add_argument(
        "--rtrans-dir",
        required=True,
        help="Directory containing rtrans parquet files",
    )
    parser.add_argument(
        "--oddpub-file",
        required=True,
        help="Path to merged oddpub parquet file",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output parquet file path",
    )
    parser.add_argument(
        "--licenses",
        default="comm,noncomm",
        help="Comma-separated list of license types to include (comm, noncomm, other)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of PMCIDs to process (for testing)",
    )
    return parser.parse_args()


def load_pmcid_list(filelist_dir: str, licenses: list[str]) -> pd.DataFrame:
    """Load all PMCIDs from filelist CSVs with license information."""
    print(f"Loading PMCIDs from {filelist_dir}...")

    all_records = []

    for license_type in licenses:
        pattern = f"oa_{license_type}_xml.PMC*.baseline.*.filelist.csv"
        csv_files = glob(os.path.join(filelist_dir, pattern))
        print(f"  Found {len(csv_files)} {license_type} filelist CSVs")

        for csv_file in csv_files:
            try:
                # PMC filelist CSVs have: File,Article Citation,Accession ID,Last Updated,PMID,License
                df = pd.read_csv(csv_file)
                if len(df) == 0:
                    continue

                # Extract PMCID from Accession ID column
                if 'Accession ID' in df.columns:
                    pmcids = df['Accession ID'].astype(str)
                elif 'AccessionID' in df.columns:
                    pmcids = df['AccessionID'].astype(str)
                else:
                    # Try first column if no Accession ID
                    pmcids = df.iloc[:, 0].astype(str)

                # Get PMIDs if available
                pmids = df.get('PMID', pd.Series([None] * len(df)))

                for pmcid, pmid in zip(pmcids, pmids):
                    if pd.notna(pmcid) and pmcid.startswith('PMC'):
                        all_records.append({
                            'pmcid': pmcid.strip(),
                            'pmid': int(pmid) if pd.notna(pmid) and str(pmid).isdigit() else None,
                            'license': license_type,
                        })
            except Exception as e:
                print(f"    Warning: Error reading {csv_file}: {e}")

    df = pd.DataFrame(all_records)
    df = df.drop_duplicates(subset=['pmcid'])
    print(f"  Total unique PMCIDs: {len(df):,}")

    # Show license distribution
    print(f"  License distribution:")
    for license_type, count in df['license'].value_counts().items():
        print(f"    {license_type}: {count:,}")

    return df


def load_rtrans_data(rtrans_dir: str, pmcids: set) -> pd.DataFrame:
    """Load relevant columns from rtrans parquet files."""
    print(f"Loading rtrans data from {rtrans_dir}...")

    parquet_files = sorted(glob(os.path.join(rtrans_dir, "*.parquet")))
    print(f"  Found {len(parquet_files)} parquet files")

    # Columns we need
    columns = [
        'pmid', 'pmcid_pmc', 'journal', 'affiliation_country',
        'year_epub', 'year_ppub', 'fund_text', 'fund_pmc_source',
        'fund_pmc_institute', 'fund_pmc_anysource'
    ]

    all_dfs = []
    for i, pfile in enumerate(parquet_files):
        if (i + 1) % 100 == 0:
            print(f"    Processing file {i+1}/{len(parquet_files)}...")

        try:
            df = pd.read_parquet(pfile, columns=columns)

            # Normalize PMCID
            df['pmcid'] = df['pmcid_pmc'].apply(
                lambda x: f"PMC{x}" if pd.notna(x) and not str(x).startswith('PMC') else str(x) if pd.notna(x) else None
            )

            # Filter to PMCIDs we care about
            df = df[df['pmcid'].isin(pmcids)]

            if len(df) > 0:
                all_dfs.append(df)
        except Exception as e:
            print(f"    Warning: Error reading {pfile}: {e}")

    if not all_dfs:
        return pd.DataFrame()

    result = pd.concat(all_dfs, ignore_index=True)
    result = result.drop_duplicates(subset=['pmcid'])
    print(f"  Loaded {len(result):,} records from rtrans")
    return result


def load_oddpub_data(oddpub_file: str, pmcids: set) -> pd.DataFrame:
    """Load oddpub data for open data/code detection."""
    print(f"Loading oddpub data from {oddpub_file}...")

    df = pd.read_parquet(oddpub_file)
    print(f"  Total records in oddpub: {len(df):,}")

    # Extract PMCID from 'article' column if pmcid is empty
    # Format: PMCPMC544856.txt -> PMC544856
    if 'article' in df.columns:
        def extract_pmcid(article):
            if pd.isna(article):
                return None
            # Remove PMCPMC prefix and .txt suffix
            match = re.match(r'PMC(PMC\d+)\.txt', str(article))
            if match:
                return match.group(1)
            # Try direct PMC pattern
            match = re.match(r'(PMC\d+)', str(article))
            if match:
                return match.group(1)
            return None

        df['pmcid'] = df['article'].apply(extract_pmcid)
        print(f"  Extracted PMCIDs from article column")

    # Filter to PMCIDs we care about
    df = df[df['pmcid'].isin(pmcids)]

    # Keep only columns we need
    keep_cols = ['pmcid', 'is_open_data', 'is_open_code']
    df = df[[c for c in keep_cols if c in df.columns]]

    print(f"  Matched {len(df):,} records")
    return df


def match_funders(text: str, normalizer: FunderNormalizer) -> list:
    """Match canonical funders in funding text."""
    if pd.isna(text) or not text:
        return []

    matched = []
    for canonical in normalizer.get_all_canonical_names():
        if normalizer.mentions_funder(text, canonical):
            matched.append(canonical)

    return matched


def build_dashboard_data(
    pmcid_df: pd.DataFrame,
    rtrans_df: pd.DataFrame,
    oddpub_df: pd.DataFrame,
    normalizer: FunderNormalizer,
) -> pd.DataFrame:
    """Merge all data sources and build final output."""
    print("Building dashboard data...")

    # Start with PMCID list
    result = pmcid_df.copy()

    # Merge rtrans data
    if len(rtrans_df) > 0:
        rtrans_cols = ['pmcid', 'pmid', 'journal', 'affiliation_country',
                       'year_epub', 'year_ppub', 'fund_text', 'fund_pmc_source',
                       'fund_pmc_institute', 'fund_pmc_anysource']
        rtrans_subset = rtrans_df[[c for c in rtrans_cols if c in rtrans_df.columns]]

        # Use rtrans pmid if we don't have one
        result = result.merge(rtrans_subset, on='pmcid', how='left', suffixes=('', '_rtrans'))

        # Prefer rtrans pmid if original is missing
        if 'pmid_rtrans' in result.columns:
            result['pmid'] = result['pmid'].combine_first(result['pmid_rtrans'])
            result = result.drop(columns=['pmid_rtrans'])

    # Merge oddpub data
    if len(oddpub_df) > 0:
        result = result.merge(oddpub_df, on='pmcid', how='left')

    # Fill missing open data/code with False
    if 'is_open_data' not in result.columns:
        result['is_open_data'] = False
    else:
        result['is_open_data'] = result['is_open_data'].fillna(False).astype(bool)

    if 'is_open_code' not in result.columns:
        result['is_open_code'] = False
    else:
        result['is_open_code'] = result['is_open_code'].fillna(False).astype(bool)

    # Calculate year (prefer epub)
    if 'year_epub' in result.columns and 'year_ppub' in result.columns:
        result['year'] = result['year_epub'].combine_first(result['year_ppub'])
        # Convert to numeric
        result['year'] = pd.to_numeric(result['year'], errors='coerce')
    else:
        result['year'] = None

    # Match funders
    print("  Matching funders...")

    def get_combined_funding_text(row):
        """Combine all funding text columns."""
        texts = []
        for col in ['fund_text', 'fund_pmc_source', 'fund_pmc_institute', 'fund_pmc_anysource']:
            if col in row and pd.notna(row[col]) and row[col]:
                texts.append(str(row[col]))
        return ' '.join(texts)

    # Process in chunks for progress
    chunk_size = 50000
    funder_results = []

    for i in range(0, len(result), chunk_size):
        chunk = result.iloc[i:i+chunk_size]
        chunk_funders = []

        for _, row in chunk.iterrows():
            combined_text = get_combined_funding_text(row)
            funders = match_funders(combined_text, normalizer)
            chunk_funders.append(np.array(funders, dtype=object))

        funder_results.extend(chunk_funders)

        if (i + chunk_size) % 100000 == 0 or i + chunk_size >= len(result):
            print(f"    Processed {min(i + chunk_size, len(result)):,}/{len(result):,} records")

    result['funder'] = funder_results

    # Build data_tags
    result['data_tags'] = result.apply(
        lambda row: np.array(['pmc_oa', row.get('license', 'unknown')], dtype=object),
        axis=1
    )

    # Add created_at timestamp
    result['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + '-04'

    # Select final columns in correct order
    final_columns = [
        'pmid', 'journal', 'affiliation_country',
        'is_open_data', 'is_open_code', 'year',
        'funder', 'data_tags', 'created_at'
    ]

    # Ensure all columns exist
    for col in final_columns:
        if col not in result.columns:
            result[col] = None

    result = result[final_columns]

    # Convert pmid to int64 (with NaN handling)
    result['pmid'] = pd.to_numeric(result['pmid'], errors='coerce').astype('Int64')

    print(f"  Final dataset: {len(result):,} records")
    print(f"  Open data: {result['is_open_data'].sum():,} ({100*result['is_open_data'].mean():.2f}%)")
    print(f"  Open code: {result['is_open_code'].sum():,} ({100*result['is_open_code'].mean():.2f}%)")

    # Count records with funders
    has_funder = result['funder'].apply(lambda x: len(x) > 0 if isinstance(x, np.ndarray) else False)
    print(f"  With funders: {has_funder.sum():,} ({100*has_funder.mean():.2f}%)")

    return result


def main():
    args = parse_args()

    # Parse licenses
    licenses = [l.strip() for l in args.licenses.split(',')]
    print(f"Including licenses: {licenses}")

    # Load PMCID list
    pmcid_df = load_pmcid_list(args.filelist_dir, licenses)

    if args.limit:
        print(f"Limiting to {args.limit} PMCIDs for testing")
        pmcid_df = pmcid_df.head(args.limit)

    pmcids = set(pmcid_df['pmcid'].dropna())
    print(f"Total PMCIDs to process: {len(pmcids):,}")

    # Load rtrans data
    rtrans_df = load_rtrans_data(args.rtrans_dir, pmcids)

    # Load oddpub data
    oddpub_df = load_oddpub_data(args.oddpub_file, pmcids)

    # Load funder normalizer
    aliases_csv = Path(__file__).parent.parent / 'funder_analysis' / 'funder_aliases.csv'
    if aliases_csv.exists():
        normalizer = FunderNormalizer(str(aliases_csv))
        print(f"Loaded {len(normalizer.get_all_canonical_names())} canonical funders")
    else:
        print(f"Warning: Funder aliases file not found at {aliases_csv}")
        normalizer = FunderNormalizer()

    # Build final dataset
    result = build_dashboard_data(pmcid_df, rtrans_df, oddpub_df, normalizer)

    # Save output
    print(f"Saving to {args.output}...")
    result.to_parquet(args.output, index=False)

    # Verify output
    verify = pd.read_parquet(args.output)
    print(f"Verification: {len(verify):,} records, {len(verify.columns)} columns")
    print(f"Columns: {verify.columns.tolist()}")


if __name__ == "__main__":
    main()
