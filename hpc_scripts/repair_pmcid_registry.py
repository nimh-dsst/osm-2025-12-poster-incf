#!/usr/bin/env python3
"""
Repair pmcid_registry.duckdb by fixing source_tarball and adding license column.

The registry was built with incorrect source_tarball values for noncomm/other PMCIDs.
This script:
1. Reads the correct PMCID->license mapping from filelist CSVs
2. Updates source_tarball to correct values based on actual license
3. Adds a 'license' column with values: comm, noncomm, other

Usage:
    # Dry run (show what would change)
    python repair_pmcid_registry.py \
        --registry pmcid_registry.duckdb \
        --filelist-dir ~/claude/pmcoaXMLs/raw_download \
        --dry-run

    # Apply fixes
    python repair_pmcid_registry.py \
        --registry pmcid_registry.duckdb \
        --filelist-dir ~/claude/pmcoaXMLs/raw_download
"""

import argparse
import os
import re
import sys
from glob import glob

import duckdb


def parse_args():
    parser = argparse.ArgumentParser(description="Repair pmcid_registry source_tarball and add license column")
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
        "--dry-run",
        action="store_true",
        help="Show what would change without modifying the database",
    )
    parser.add_argument(
        "--licenses",
        default="comm,noncomm,other",
        help="Comma-separated license types (default: comm,noncomm,other)",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    licenses = [l.strip() for l in args.licenses.split(',')]
    print(f"Processing licenses: {licenses}")

    if not os.path.exists(args.registry):
        print(f"Error: Registry file not found: {args.registry}")
        sys.exit(1)

    # Create in-memory database for filelist data
    mem_conn = duckdb.connect(':memory:')

    # Load filelist CSVs
    print(f"\nLoading filelist CSVs from {args.filelist_dir}...")
    mem_conn.execute("""
        CREATE TABLE filelists (
            pmcid VARCHAR,
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
                source_file = os.path.basename(csv_file)
                # Derive tarball name from filelist name
                # oa_comm_xml.PMC000xxxxxx.baseline.2025-06-26.filelist.csv
                # -> oa_comm_xml.PMC000xxxxxx.baseline.2025-06-26.tar.gz
                tarball_name = source_file.replace('.filelist.csv', '.tar.gz')

                mem_conn.execute(f"""
                    INSERT INTO filelists
                    SELECT
                        TRIM(AccessionID) as pmcid,
                        '{license_type}' as license,
                        '{tarball_name}' as source_file
                    FROM read_csv_auto('{csv_file}', header=true)
                    WHERE AccessionID IS NOT NULL
                      AND AccessionID LIKE 'PMC%'
                """)
            except Exception as e:
                print(f"    Warning: Error reading {csv_file}: {e}")

    # Deduplicate - keep first occurrence
    mem_conn.execute("""
        CREATE TABLE filelists_dedup AS
        SELECT DISTINCT ON (pmcid) pmcid, license, source_file
        FROM filelists
        ORDER BY pmcid
    """)

    filelist_count = mem_conn.execute("SELECT COUNT(*) FROM filelists_dedup").fetchone()[0]
    print(f"  Total unique PMCIDs from filelists: {filelist_count:,}")

    # Show license distribution
    print(f"\nFilelist license distribution:")
    for row in mem_conn.execute("SELECT license, COUNT(*) as cnt FROM filelists_dedup GROUP BY license ORDER BY cnt DESC").fetchall():
        print(f"  {row[0]}: {row[1]:,}")

    # Connect to registry
    if args.dry_run:
        print(f"\n[DRY RUN] Opening registry in read-only mode...")
        reg_conn = duckdb.connect(args.registry, read_only=True)
    else:
        print(f"\nOpening registry for modification...")
        reg_conn = duckdb.connect(args.registry)

    # Get table name
    tables = reg_conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()
    table_names = [t[0] for t in tables]
    print(f"  Tables: {table_names}")

    if 'pmcids' in table_names:
        table_name = 'pmcids'
    else:
        print("Error: pmcids table not found")
        sys.exit(1)

    # Check current schema
    schema = reg_conn.execute(f"DESCRIBE {table_name}").fetchall()
    columns = [s[0] for s in schema]
    print(f"  Current columns: {columns}")

    # Check current state
    print("\nCurrent registry state:")
    for row in reg_conn.execute(f"SELECT LEFT(source_tarball, 15) as prefix, COUNT(*) as cnt FROM {table_name} GROUP BY prefix ORDER BY cnt DESC").fetchall():
        print(f"  {row[0]}...: {row[1]:,}")

    # Attach memory database to registry connection
    # We need to export filelists to a temp file since DuckDB can't attach in-memory DBs
    print("\nPreparing update data...")
    mem_conn.execute("COPY filelists_dedup TO '/tmp/filelists_repair.parquet' (FORMAT PARQUET)")

    # Load into registry connection
    reg_conn.execute("CREATE TEMP TABLE filelist_data AS SELECT * FROM read_parquet('/tmp/filelists_repair.parquet')")

    # Count records that need updating
    needs_update = reg_conn.execute(f"""
        SELECT COUNT(*) FROM {table_name} r
        INNER JOIN filelist_data f ON r.pmcid = f.pmcid
        WHERE r.source_tarball != f.source_file
    """).fetchone()[0]

    print(f"\nRecords needing source_tarball update: {needs_update:,}")

    # Show breakdown by license
    print("\nUpdates needed by license type:")
    for row in reg_conn.execute(f"""
        SELECT f.license, COUNT(*) as cnt
        FROM {table_name} r
        INNER JOIN filelist_data f ON r.pmcid = f.pmcid
        WHERE r.source_tarball != f.source_file
        GROUP BY f.license
        ORDER BY cnt DESC
    """).fetchall():
        print(f"  {row[0]}: {row[1]:,}")

    if args.dry_run:
        print("\n[DRY RUN] Would perform the following changes:")
        print(f"  1. Update source_tarball for {needs_update:,} records")
        if 'license' not in columns:
            print(f"  2. Add 'license' column to {table_name}")
            print(f"  3. Populate license column from filelist data")
        print("\nRun without --dry-run to apply changes.")
    else:
        # Apply updates
        print("\nApplying updates...")

        # Update source_tarball
        print("  Updating source_tarball...")
        reg_conn.execute(f"""
            UPDATE {table_name} r
            SET source_tarball = (
                SELECT f.source_file
                FROM filelist_data f
                WHERE f.pmcid = r.pmcid
            )
            WHERE EXISTS (
                SELECT 1 FROM filelist_data f
                WHERE f.pmcid = r.pmcid
                  AND f.source_file != r.source_tarball
            )
        """)
        print(f"    Updated {needs_update:,} records")

        # Add license column if not exists
        if 'license' not in columns:
            print("  Adding license column...")
            reg_conn.execute(f"ALTER TABLE {table_name} ADD COLUMN license VARCHAR")

        # Populate license column
        print("  Populating license column...")
        reg_conn.execute(f"""
            UPDATE {table_name} r
            SET license = (
                SELECT f.license
                FROM filelist_data f
                WHERE f.pmcid = r.pmcid
            )
            WHERE EXISTS (
                SELECT 1 FROM filelist_data f
                WHERE f.pmcid = r.pmcid
            )
        """)

        # Verify
        print("\nVerifying updates...")
        print("\nNew source_tarball distribution:")
        for row in reg_conn.execute(f"SELECT LEFT(source_tarball, 15) as prefix, COUNT(*) as cnt FROM {table_name} GROUP BY prefix ORDER BY cnt DESC").fetchall():
            print(f"  {row[0]}...: {row[1]:,}")

        print("\nLicense distribution:")
        for row in reg_conn.execute(f"SELECT license, COUNT(*) as cnt FROM {table_name} GROUP BY license ORDER BY cnt DESC").fetchall():
            print(f"  {row[0]}: {row[1]:,}")

        print("\nRepair complete!")

    # Cleanup
    reg_conn.close()
    mem_conn.close()
    os.remove('/tmp/filelists_repair.parquet')


if __name__ == "__main__":
    main()
