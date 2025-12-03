#!/usr/bin/env python3
"""
Validate pmcid_registry.duckdb against PMC filelist CSVs.

This script compares the PMCIDs in the DuckDB registry against the
ground truth from PMC Open Access filelist CSVs to identify:
- Missing PMCIDs in the registry
- License type discrepancies
- Coverage statistics

Uses DuckDB for memory-efficient processing.

Usage:
    python validate_pmcid_registry.py \
        --registry pmcid_registry.duckdb \
        --filelist-dir ~/claude/pmcoaXMLs/raw_download

    # Generate detailed report
    python validate_pmcid_registry.py \
        --registry pmcid_registry.duckdb \
        --filelist-dir ~/claude/pmcoaXMLs/raw_download \
        --output validation_report.csv
"""

import argparse
import os
import sys
from glob import glob

import duckdb


def parse_args():
    parser = argparse.ArgumentParser(description="Validate pmcid_registry against filelist CSVs")
    parser.add_argument(
        "--registry",
        default="pmcid_registry.duckdb",
        help="Path to pmcid_registry.duckdb file",
    )
    parser.add_argument(
        "--filelist-dir",
        required=True,
        help="Directory containing PMC filelist CSVs",
    )
    parser.add_argument(
        "--output",
        help="Output CSV for detailed discrepancy report",
    )
    parser.add_argument(
        "--licenses",
        default="comm,noncomm,other",
        help="Comma-separated license types to check (default: comm,noncomm,other)",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    licenses = [l.strip() for l in args.licenses.split(',')]
    print(f"Checking licenses: {licenses}")

    # Create in-memory database for analysis
    conn = duckdb.connect(':memory:')

    # Load filelist CSVs into a table
    print(f"\nLoading filelist CSVs from {args.filelist_dir}...")

    # Create temp table for filelists
    conn.execute("""
        CREATE TABLE filelists (
            pmcid VARCHAR,
            pmid VARCHAR,
            license VARCHAR,
            source_file VARCHAR
        )
    """)

    for license_type in licenses:
        pattern = f"oa_{license_type}_xml.PMC*.baseline.*.filelist.csv"
        csv_files = glob(os.path.join(args.filelist_dir, pattern))
        print(f"  Found {len(csv_files)} {license_type} filelist CSVs")

        for csv_file in csv_files:
            try:
                # Use DuckDB to read CSV directly
                # Column name varies: "Accession ID" or "AccessionID"
                source_file = os.path.basename(csv_file)
                conn.execute(f"""
                    INSERT INTO filelists
                    SELECT
                        TRIM(AccessionID) as pmcid,
                        CAST(PMID AS VARCHAR) as pmid,
                        '{license_type}' as license,
                        '{source_file}' as source_file
                    FROM read_csv_auto('{csv_file}', header=true)
                    WHERE AccessionID IS NOT NULL
                      AND AccessionID LIKE 'PMC%'
                """)
            except Exception as e:
                print(f"    Warning: Error reading {csv_file}: {e}")

    # Deduplicate filelists
    filelist_total = conn.execute("SELECT COUNT(*) FROM filelists").fetchone()[0]
    print(f"  Total records loaded: {filelist_total:,}")

    conn.execute("""
        CREATE TABLE filelists_dedup AS
        SELECT DISTINCT ON (pmcid) pmcid, pmid, license, source_file
        FROM filelists
        ORDER BY pmcid
    """)

    filelist_unique = conn.execute("SELECT COUNT(*) FROM filelists_dedup").fetchone()[0]
    print(f"  Unique PMCIDs: {filelist_unique:,}")

    # Show license distribution
    print(f"\nFilelist license distribution:")
    for row in conn.execute("SELECT license, COUNT(*) as cnt FROM filelists_dedup GROUP BY license ORDER BY cnt DESC").fetchall():
        print(f"  {row[0]}: {row[1]:,}")

    # Load registry
    print(f"\nLoading registry from {args.registry}...")
    if not os.path.exists(args.registry):
        print(f"Error: Registry file not found: {args.registry}")
        sys.exit(1)

    # Attach registry database
    conn.execute(f"ATTACH '{args.registry}' AS registry (READ_ONLY)")

    # Get table info (DuckDB doesn't have sqlite_master, use information_schema)
    tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_catalog='registry'").fetchall()
    table_names = [t[0] for t in tables]
    print(f"  Tables in registry: {table_names}")

    if 'pmcids' in table_names:
        table_name = 'pmcids'
    elif 'pmcid_registry' in table_names:
        table_name = 'pmcid_registry'
    else:
        print(f"Error: No pmcids or pmcid_registry table found")
        sys.exit(1)

    registry_total = conn.execute(f"SELECT COUNT(*) FROM registry.{table_name}").fetchone()[0]
    print(f"  Registry PMCIDs: {registry_total:,}")

    # Check registry license distribution if available
    try:
        schema = conn.execute(f"DESCRIBE registry.{table_name}").fetchall()
        columns = [s[0] for s in schema]
        print(f"  Registry columns: {columns}")

        if 'license' in columns or 'license_subset' in columns:
            license_col = 'license' if 'license' in columns else 'license_subset'
            print(f"\nRegistry license distribution:")
            for row in conn.execute(f"SELECT {license_col}, COUNT(*) as cnt FROM registry.{table_name} GROUP BY {license_col} ORDER BY cnt DESC").fetchall():
                print(f"  {row[0]}: {row[1]:,}")
    except Exception as e:
        print(f"  Note: Could not get registry schema: {e}")

    # Find missing PMCIDs
    print("\nComparing registry against filelists...")

    matched = conn.execute(f"""
        SELECT COUNT(*) FROM filelists_dedup f
        INNER JOIN registry.{table_name} r ON f.pmcid = r.pmcid
    """).fetchone()[0]

    missing_from_registry = conn.execute(f"""
        SELECT COUNT(*) FROM filelists_dedup f
        LEFT JOIN registry.{table_name} r ON f.pmcid = r.pmcid
        WHERE r.pmcid IS NULL
    """).fetchone()[0]

    extra_in_registry = conn.execute(f"""
        SELECT COUNT(*) FROM registry.{table_name} r
        LEFT JOIN filelists_dedup f ON r.pmcid = f.pmcid
        WHERE f.pmcid IS NULL
    """).fetchone()[0]

    print(f"\nResults:")
    print(f"  Filelist PMCIDs: {filelist_unique:,}")
    print(f"  Registry PMCIDs: {registry_total:,}")
    print(f"  Matched: {matched:,}")
    print(f"  Missing from registry: {missing_from_registry:,}")
    print(f"  Extra in registry: {extra_in_registry:,}")

    # Show missing by license type
    if missing_from_registry > 0:
        print(f"\nMissing PMCIDs by license:")
        for row in conn.execute(f"""
            SELECT f.license, COUNT(*) as cnt
            FROM filelists_dedup f
            LEFT JOIN registry.{table_name} r ON f.pmcid = r.pmcid
            WHERE r.pmcid IS NULL
            GROUP BY f.license
            ORDER BY cnt DESC
        """).fetchall():
            print(f"  {row[0]}: {row[1]:,}")

    # Export missing PMCIDs if requested
    if args.output and missing_from_registry > 0:
        print(f"\nExporting missing PMCIDs to {args.output}...")
        conn.execute(f"""
            COPY (
                SELECT f.pmcid, f.pmid, f.license, f.source_file
                FROM filelists_dedup f
                LEFT JOIN registry.{table_name} r ON f.pmcid = r.pmcid
                WHERE r.pmcid IS NULL
            ) TO '{args.output}' (HEADER, DELIMITER ',')
        """)
        print(f"  Done")

    # Check source_tarball correctness
    print("\nValidating source_tarball field...")
    if 'source_tarball' in columns:
        # Check for mismatched source_tarball values
        # PMCIDs from noncomm/other filelists should have source_tarball starting with oa_noncomm/oa_other
        mismatch_query = f"""
            SELECT f.license,
                   COUNT(*) as total,
                   SUM(CASE WHEN r.source_tarball LIKE 'oa_' || f.license || '%' THEN 1 ELSE 0 END) as correct,
                   SUM(CASE WHEN r.source_tarball NOT LIKE 'oa_' || f.license || '%' THEN 1 ELSE 0 END) as incorrect
            FROM filelists_dedup f
            INNER JOIN registry.{table_name} r ON f.pmcid = r.pmcid
            GROUP BY f.license
            ORDER BY f.license
        """
        print("\n  Source tarball correctness by license:")
        mismatches_total = 0
        for row in conn.execute(mismatch_query).fetchall():
            license_type, total, correct, incorrect = row
            pct_correct = 100 * correct / total if total > 0 else 0
            print(f"    {license_type}: {correct:,}/{total:,} correct ({pct_correct:.1f}%), {incorrect:,} incorrect")
            mismatches_total += incorrect

        if mismatches_total > 0:
            print(f"\n  WARNING: {mismatches_total:,} PMCIDs have incorrect source_tarball values")

            # Show unique source_tarball values in registry
            print("\n  Unique source_tarball values in registry:")
            for row in conn.execute(f"SELECT DISTINCT source_tarball FROM registry.{table_name} ORDER BY source_tarball").fetchall():
                print(f"    {row[0]}")

    # Summary
    coverage = 100 * matched / filelist_unique if filelist_unique > 0 else 0
    print(f"\n{'='*50}")
    print(f"SUMMARY")
    print(f"{'='*50}")
    print(f"Registry coverage: {coverage:.2f}%")
    if missing_from_registry > 0:
        print(f"WARNING: {missing_from_registry:,} PMCIDs missing from registry")
    if extra_in_registry > 0:
        print(f"NOTE: {extra_in_registry:,} PMCIDs in registry not in filelists (may be from incremental updates)")
    if mismatches_total > 0:
        print(f"WARNING: {mismatches_total:,} PMCIDs have incorrect source_tarball values")

    conn.close()


if __name__ == "__main__":
    main()
