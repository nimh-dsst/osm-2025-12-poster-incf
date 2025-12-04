#!/usr/bin/env python3
"""
Validate dashboard parquet output against a reference parquet file.

This script compares a new dashboard output against a reference file to:
1. Check schema compatibility
2. Compare row counts and coverage
3. Validate is_open_data/is_open_code distributions
4. Compare funder matching rates
5. Check for specific PMIDs and their values

Usage:
    # Compare new output to reference
    python validate_dashboard_output.py \
        --new /path/to/new_dashboard.parquet \
        --reference ~/claude/matches_new3.parquet

    # Validate single file
    python validate_dashboard_output.py \
        --new /path/to/dashboard.parquet

    # Sample specific PMIDs
    python validate_dashboard_output.py \
        --new /path/to/dashboard.parquet \
        --sample-pmids 12345678,23456789,34567890
"""

import argparse
import sys
from typing import List, Optional

import pandas as pd
import pyarrow.parquet as pq


def load_parquet(path: str) -> pd.DataFrame:
    """Load parquet file into DataFrame."""
    print(f"Loading {path}...")
    df = pq.read_table(path).to_pandas()
    print(f"  Loaded {len(df):,} records")
    return df


def validate_schema(df: pd.DataFrame, name: str) -> dict:
    """Validate schema and return summary."""
    expected_columns = [
        'pmid', 'journal', 'affiliation_country',
        'is_open_data', 'is_open_code', 'year',
        'funder', 'data_tags', 'created_at'
    ]

    print(f"\n=== Schema Validation: {name} ===")
    print(f"Columns: {df.columns.tolist()}")

    missing = [c for c in expected_columns if c not in df.columns]
    extra = [c for c in df.columns if c not in expected_columns]

    if missing:
        print(f"  WARNING: Missing columns: {missing}")
    if extra:
        print(f"  INFO: Extra columns: {extra}")

    print(f"\nColumn types:")
    for col in df.columns:
        print(f"  {col}: {df[col].dtype}")

    return {
        'columns': df.columns.tolist(),
        'missing': missing,
        'extra': extra
    }


def validate_distributions(df: pd.DataFrame, name: str) -> dict:
    """Validate data distributions."""
    print(f"\n=== Distribution Validation: {name} ===")

    stats = {}

    # Row count
    stats['total_rows'] = len(df)
    print(f"Total records: {stats['total_rows']:,}")

    # is_open_data
    if 'is_open_data' in df.columns:
        open_data = df['is_open_data'].sum()
        open_data_pct = 100 * open_data / len(df) if len(df) > 0 else 0
        stats['open_data_count'] = open_data
        stats['open_data_pct'] = open_data_pct
        print(f"is_open_data=true: {open_data:,} ({open_data_pct:.2f}%)")

        # Check for all-false issue
        if open_data == 0 and len(df) > 1000:
            print("  ERROR: is_open_data is ALL FALSE - likely a join bug!")

    # is_open_code
    if 'is_open_code' in df.columns:
        open_code = df['is_open_code'].sum()
        open_code_pct = 100 * open_code / len(df) if len(df) > 0 else 0
        stats['open_code_count'] = open_code
        stats['open_code_pct'] = open_code_pct
        print(f"is_open_code=true: {open_code:,} ({open_code_pct:.2f}%)")

    # Funders
    if 'funder' in df.columns:
        has_funder = df['funder'].apply(
            lambda x: len(x) > 0 if hasattr(x, '__len__') and not isinstance(x, str) else False
        )
        funder_count = has_funder.sum()
        funder_pct = 100 * funder_count / len(df) if len(df) > 0 else 0
        stats['with_funder_count'] = funder_count
        stats['with_funder_pct'] = funder_pct
        print(f"Records with funders: {funder_count:,} ({funder_pct:.2f}%)")

    # Year distribution
    if 'year' in df.columns:
        valid_years = df['year'].notna().sum()
        stats['valid_years'] = valid_years
        print(f"Records with valid year: {valid_years:,} ({100*valid_years/len(df):.1f}%)")

        if valid_years > 0:
            year_range = df['year'].dropna()
            print(f"  Year range: {int(year_range.min())} - {int(year_range.max())}")

    # Journal coverage
    if 'journal' in df.columns:
        valid_journals = df['journal'].notna().sum()
        stats['valid_journals'] = valid_journals
        print(f"Records with journal: {valid_journals:,} ({100*valid_journals/len(df):.1f}%)")

    return stats


def compare_files(new_df: pd.DataFrame, ref_df: pd.DataFrame) -> dict:
    """Compare two dashboard files."""
    print("\n" + "=" * 70)
    print("COMPARISON: New vs Reference")
    print("=" * 70)

    comparison = {}

    # Row counts
    print(f"\nRow counts:")
    print(f"  New:       {len(new_df):,}")
    print(f"  Reference: {len(ref_df):,}")
    print(f"  Difference: {len(new_df) - len(ref_df):+,}")

    # PMID overlap
    if 'pmid' in new_df.columns and 'pmid' in ref_df.columns:
        new_pmids = set(new_df['pmid'].dropna().astype(int))
        ref_pmids = set(ref_df['pmid'].dropna().astype(int))

        common = new_pmids & ref_pmids
        new_only = new_pmids - ref_pmids
        ref_only = ref_pmids - new_pmids

        print(f"\nPMID overlap:")
        print(f"  Common PMIDs: {len(common):,}")
        print(f"  Only in new: {len(new_only):,}")
        print(f"  Only in reference: {len(ref_only):,}")

        comparison['common_pmids'] = len(common)
        comparison['overlap_pct'] = 100 * len(common) / len(ref_pmids) if ref_pmids else 0

        # Compare values for common PMIDs
        if common:
            print(f"\nComparing values for {len(common):,} common PMIDs...")

            # Filter to common PMIDs and drop duplicates (keep first)
            new_subset = new_df[new_df['pmid'].isin(common)].drop_duplicates(subset=['pmid'])
            ref_subset = ref_df[ref_df['pmid'].isin(common)].drop_duplicates(subset=['pmid'])

            # Merge on PMID to align
            merged = new_subset.merge(
                ref_subset[['pmid', 'is_open_data', 'is_open_code']],
                on='pmid',
                suffixes=('_new', '_ref')
            )

            # Compare is_open_data
            if 'is_open_data_new' in merged.columns and 'is_open_data_ref' in merged.columns:
                agreement = (merged['is_open_data_new'] == merged['is_open_data_ref']).sum()
                agreement_pct = 100 * agreement / len(merged) if len(merged) > 0 else 0

                print(f"\nis_open_data agreement: {agreement:,}/{len(merged):,} ({agreement_pct:.2f}%)")

                # Show disagreements
                disagreements = merged[merged['is_open_data_new'] != merged['is_open_data_ref']]
                if len(disagreements) > 0:
                    print(f"  Disagreements: {len(disagreements):,}")

                    # Sample disagreements
                    sample = disagreements['pmid'].head(5).tolist()
                    print(f"  Sample disagreeing PMIDs: {sample}")

                    # Show distribution of disagreements
                    new_true_ref_false = ((merged['is_open_data_new'] == True) & (merged['is_open_data_ref'] == False)).sum()
                    new_false_ref_true = ((merged['is_open_data_new'] == False) & (merged['is_open_data_ref'] == True)).sum()
                    print(f"    New=true, Ref=false: {new_true_ref_false:,}")
                    print(f"    New=false, Ref=true: {new_false_ref_true:,}")

                comparison['open_data_agreement_pct'] = agreement_pct

    return comparison


def sample_pmids(df: pd.DataFrame, pmids: List[int], name: str):
    """Show values for specific PMIDs."""
    print(f"\n=== Sample PMIDs: {name} ===")

    if 'pmid' not in df.columns:
        print("  No pmid column!")
        return

    df_indexed = df.set_index('pmid')

    for pmid in pmids:
        if pmid in df_indexed.index:
            row = df_indexed.loc[pmid]
            print(f"\nPMID {pmid}:")
            for col in ['is_open_data', 'is_open_code', 'journal', 'year', 'funder']:
                if col in row.index:
                    val = row[col]
                    if col == 'funder' and hasattr(val, '__len__'):
                        val = list(val) if len(val) > 0 else []
                    print(f"  {col}: {val}")
        else:
            print(f"\nPMID {pmid}: NOT FOUND")


def main():
    parser = argparse.ArgumentParser(
        description="Validate dashboard parquet output",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Compare new output to reference
    python validate_dashboard_output.py \\
        --new /path/to/new_dashboard.parquet \\
        --reference ~/claude/matches_new3.parquet

    # Validate single file
    python validate_dashboard_output.py \\
        --new /path/to/dashboard.parquet

    # Sample specific PMIDs
    python validate_dashboard_output.py \\
        --new /path/to/dashboard.parquet \\
        --sample-pmids 12345678,23456789
        """
    )
    parser.add_argument(
        "--new",
        required=True,
        help="Path to new dashboard parquet file to validate"
    )
    parser.add_argument(
        "--reference",
        help="Path to reference parquet file for comparison"
    )
    parser.add_argument(
        "--sample-pmids",
        help="Comma-separated list of PMIDs to show values for"
    )
    args = parser.parse_args()

    # Load new file
    new_df = load_parquet(args.new)

    # Validate schema
    validate_schema(new_df, "New")

    # Validate distributions
    new_stats = validate_distributions(new_df, "New")

    # Check for critical issues
    issues = []
    if new_stats.get('open_data_count', 0) == 0 and new_stats.get('total_rows', 0) > 1000:
        issues.append("CRITICAL: is_open_data is ALL FALSE")
    if new_stats.get('open_data_pct', 0) < 1.0 and new_stats.get('total_rows', 0) > 10000:
        issues.append("WARNING: is_open_data rate < 1% - expected 3-5%")

    # Load and compare reference if provided
    if args.reference:
        ref_df = load_parquet(args.reference)
        validate_schema(ref_df, "Reference")
        ref_stats = validate_distributions(ref_df, "Reference")
        comparison = compare_files(new_df, ref_df)

        # Check comparison issues
        if comparison.get('open_data_agreement_pct', 100) < 90:
            issues.append(f"WARNING: is_open_data agreement only {comparison['open_data_agreement_pct']:.1f}%")

    # Sample specific PMIDs
    if args.sample_pmids:
        pmids = [int(p.strip()) for p in args.sample_pmids.split(',')]
        sample_pmids(new_df, pmids, "New")
        if args.reference:
            sample_pmids(ref_df, pmids, "Reference")

    # Summary
    print("\n" + "=" * 70)
    print("VALIDATION SUMMARY")
    print("=" * 70)

    print(f"\nNew file: {args.new}")
    print(f"  Records: {new_stats['total_rows']:,}")
    print(f"  Open data: {new_stats.get('open_data_count', 'N/A'):,} ({new_stats.get('open_data_pct', 0):.2f}%)")
    print(f"  Open code: {new_stats.get('open_code_count', 'N/A'):,} ({new_stats.get('open_code_pct', 0):.2f}%)")

    if issues:
        print(f"\n⚠️  ISSUES FOUND:")
        for issue in issues:
            print(f"  - {issue}")
        sys.exit(1)
    else:
        print(f"\n✓ Validation passed")
        sys.exit(0)


if __name__ == "__main__":
    main()
