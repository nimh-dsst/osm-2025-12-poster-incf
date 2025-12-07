#!/usr/bin/env python3
"""
PMCID Registry - DuckDB-based tracking of processing status across pipelines.

This creates and maintains a DuckDB database that tracks:
- All PMCIDs from PMC Open Access file lists
- Which processing pipelines have completed for each PMCID
- Source file paths for each PMCID

Pipelines tracked:
- oddpub_v7: oddpub v7.2.3 (standalone R package)
- oddpub_v5: oddpub v5 (embedded in rtransparent)
- rtransparent: rtransparent R package output
- metadata: XML metadata extraction
- compact: compact_rtrans dataset

Usage:
    # Initialize registry from file lists
    python pmcid_registry.py init /data/NIMH_scratch/adamt/pmcoa

    # Update status from oddpub v7 output directory
    python pmcid_registry.py update-oddpub-v7 ~/claude/osm-oddpub-out/

    # Show verification summary
    python pmcid_registry.py status

    # Generate retry swarm for missing PMCIDs (batched efficiently)
    python pmcid_registry.py generate-retry oddpub_v7 \
        --xml-base-dir /data/NIMH_scratch/adamt/pmcoa \
        --output-dir /data/NIMH_scratch/adamt/osm/oddpub_output \
        --container /data/adamt/containers/oddpub_optimized.sif \
        --batch-size 50
"""

import argparse
import logging
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Set

import duckdb
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Default database path
DEFAULT_DB_PATH = Path(__file__).parent / "pmcid_registry.duckdb"


def get_connection(db_path: Path = DEFAULT_DB_PATH, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Get DuckDB connection, creating database if needed."""
    return duckdb.connect(str(db_path), read_only=read_only)


def init_schema(con: duckdb.DuckDBPyConnection):
    """Initialize database schema."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS pmcids (
            pmcid VARCHAR PRIMARY KEY,
            pmc_dir VARCHAR NOT NULL,
            xml_path VARCHAR,
            source_tarball VARCHAR,
            csv_file VARCHAR,

            -- Processing status flags
            has_oddpub_v7 BOOLEAN DEFAULT FALSE,
            has_oddpub_v5 BOOLEAN DEFAULT FALSE,
            has_rtransparent BOOLEAN DEFAULT FALSE,
            has_metadata BOOLEAN DEFAULT FALSE,
            has_compact BOOLEAN DEFAULT FALSE,

            -- Timestamps for when each pipeline completed
            oddpub_v7_updated_at TIMESTAMP,
            oddpub_v5_updated_at TIMESTAMP,
            rtransparent_updated_at TIMESTAMP,
            metadata_updated_at TIMESTAMP,
            compact_updated_at TIMESTAMP,

            -- Article metadata
            article_type VARCHAR,
            license VARCHAR
        )
    """)

    # Add columns if they don't exist (for existing databases)
    try:
        con.execute("ALTER TABLE pmcids ADD COLUMN IF NOT EXISTS article_type VARCHAR")
        con.execute("ALTER TABLE pmcids ADD COLUMN IF NOT EXISTS license VARCHAR")
    except Exception:
        pass  # Column might already exist

    # Create indexes for common queries
    con.execute("CREATE INDEX IF NOT EXISTS idx_pmc_dir ON pmcids(pmc_dir)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_oddpub_v7 ON pmcids(has_oddpub_v7)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_oddpub_v5 ON pmcids(has_oddpub_v5)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_article_type ON pmcids(article_type)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_license ON pmcids(license)")

    # Summary view by PMC directory
    con.execute("""
        CREATE OR REPLACE VIEW pmc_dir_summary AS
        SELECT
            pmc_dir,
            COUNT(*) as total_count,
            SUM(CASE WHEN has_oddpub_v7 THEN 1 ELSE 0 END) as oddpub_v7_count,
            SUM(CASE WHEN has_oddpub_v5 THEN 1 ELSE 0 END) as oddpub_v5_count,
            SUM(CASE WHEN has_rtransparent THEN 1 ELSE 0 END) as rtransparent_count,
            SUM(CASE WHEN has_metadata THEN 1 ELSE 0 END) as metadata_count,
            SUM(CASE WHEN has_compact THEN 1 ELSE 0 END) as compact_count
        FROM pmcids
        GROUP BY pmc_dir
    """)

    logger.info("Schema initialized")


def init_from_filelists(con: duckdb.DuckDBPyConnection, xml_base_dir: Path):
    """
    Initialize registry from CSV file lists.

    Reads all *.baseline.*.filelist.csv files and populates the pmcids table.
    Supports two naming conventions:
    - PMC001xxxxxx.baseline.YYYY-MM-DD.filelist.csv (HPC style)
    - oa_comm_xml.PMC001xxxxxx.baseline.YYYY-MM-DD.filelist.csv (download style)
    """
    logger.info(f"Initializing from file lists in {xml_base_dir}")

    # Find all CSV file lists (both naming conventions)
    csv_files = sorted(xml_base_dir.glob("*.baseline.*.filelist.csv"))
    logger.info(f"Found {len(csv_files)} CSV file lists")

    if not csv_files:
        logger.error("No CSV file lists found")
        return

    total_inserted = 0

    for csv_file in csv_files:
        # Extract PMC directory name from filename
        # Format: PMC001xxxxxx.baseline.2024-09-25.filelist.csv
        # Or: oa_comm_xml.PMC001xxxxxx.baseline.2024-09-25.filelist.csv
        basename = csv_file.name
        parts = basename.split('.')

        # Find the PMC directory part
        pmc_dir = None
        date_part = None
        for i, part in enumerate(parts):
            if part.startswith('PMC') and 'x' in part:
                pmc_dir = part
                # Date is usually 2 parts after PMC dir (after 'baseline')
                if i + 2 < len(parts):
                    date_part = parts[i + 2]
                break

        if not pmc_dir:
            logger.warning(f"Could not find PMC dir in filename: {basename}")
            continue

        source_tarball = f"oa_comm_xml.{pmc_dir}.baseline.{date_part or 'unknown'}.tar.gz"

        # Read CSV and process
        try:
            # Use DuckDB to read CSV directly
            df = con.execute(f"""
                SELECT * FROM read_csv_auto('{csv_file}', header=true)
            """).fetchdf()

            if df.empty:
                continue

            # Get the file column - could be "Article File", "file", or first column
            file_col = None
            for col in ['Article File', 'file', df.columns[0]]:
                if col in df.columns:
                    file_col = col
                    break

            if file_col is None:
                logger.warning(f"Could not find file column in {csv_file}")
                continue

            # Build DataFrame for bulk insert
            # Extract PMCIDs from file paths
            df_insert = pd.DataFrame({
                'file_path': df[file_col].astype(str)
            })
            df_insert['pmcid'] = df_insert['file_path'].apply(lambda x: Path(x).stem)
            # Ensure PMC prefix
            df_insert['pmcid'] = df_insert['pmcid'].apply(
                lambda x: x if x.startswith('PMC') else f'PMC{x}'
            )
            df_insert['xml_path'] = df_insert['file_path'].apply(
                lambda x: str(xml_base_dir / x)
            )
            df_insert['pmc_dir'] = pmc_dir
            df_insert['source_tarball'] = source_tarball
            df_insert['csv_file'] = str(csv_file)

            # Drop intermediate column
            df_insert = df_insert.drop(columns=['file_path'])

            # Bulk insert using DuckDB's efficient DataFrame registration
            if len(df_insert) > 0:
                con.register('insert_df', df_insert)
                con.execute("""
                    INSERT OR REPLACE INTO pmcids
                    (pmcid, pmc_dir, xml_path, source_tarball, csv_file)
                    SELECT pmcid, pmc_dir, xml_path, source_tarball, csv_file
                    FROM insert_df
                """)
                con.unregister('insert_df')
                total_inserted += len(df_insert)

            logger.info(f"Processed {csv_file.name}: {len(df_insert)} PMCIDs")

        except Exception as e:
            logger.warning(f"Error processing {csv_file}: {e}")
            continue

    con.commit()
    logger.info(f"Initialized {total_inserted:,} PMCIDs")


def update_from_oddpub_v7(con: duckdb.DuckDBPyConnection, output_dir: Path, old_output_dir: Optional[Path] = None):
    """
    Update oddpub v7.2.3 status from output parquet files.

    Scans output directory for *_results.parquet files and marks PMCIDs as processed.
    """
    logger.info(f"Updating oddpub v7.2.3 status from {output_dir}")

    from glob import glob

    # Build list of parquet files to scan
    patterns = [str(output_dir / "*_results.parquet")]
    if old_output_dir and old_output_dir.exists():
        patterns.append(str(old_output_dir / "*_results.parquet"))
        logger.info(f"Also scanning old output dir: {old_output_dir}")

    total_updated = 0

    for pattern in patterns:
        files = glob(pattern)
        if not files:
            logger.info(f"No files matching {pattern}")
            continue

        logger.info(f"Processing {len(files)} parquet files from {Path(pattern).parent}")

        try:
            # Extract PMCIDs from parquet files using DuckDB
            # Handle both 'pmcid' column and 'article' column formats
            result = con.execute(f"""
                WITH parquet_pmcids AS (
                    SELECT DISTINCT
                        CASE
                            WHEN pmcid IS NOT NULL AND pmcid != '' THEN
                                CASE WHEN pmcid LIKE 'PMC%' THEN pmcid ELSE 'PMC' || pmcid END
                            WHEN article LIKE 'PMCPMC%' THEN SUBSTRING(article, 4, LENGTH(article) - 7)
                            WHEN article LIKE 'PMC%.txt' THEN REPLACE(article, '.txt', '')
                            WHEN article LIKE 'PMC%' THEN article
                            ELSE NULL
                        END as pmcid
                    FROM read_parquet('{pattern}')
                )
                UPDATE pmcids
                SET has_oddpub_v7 = TRUE,
                    oddpub_v7_updated_at = CURRENT_TIMESTAMP
                WHERE pmcid IN (SELECT pmcid FROM parquet_pmcids WHERE pmcid IS NOT NULL)
            """)

            # Count how many were in the parquet files
            count_result = con.execute(f"""
                WITH parquet_pmcids AS (
                    SELECT DISTINCT
                        CASE
                            WHEN pmcid IS NOT NULL AND pmcid != '' THEN
                                CASE WHEN pmcid LIKE 'PMC%' THEN pmcid ELSE 'PMC' || pmcid END
                            WHEN article LIKE 'PMCPMC%' THEN SUBSTRING(article, 4, LENGTH(article) - 7)
                            WHEN article LIKE 'PMC%.txt' THEN REPLACE(article, '.txt', '')
                            WHEN article LIKE 'PMC%' THEN article
                            ELSE NULL
                        END as pmcid
                    FROM read_parquet('{pattern}')
                )
                SELECT COUNT(DISTINCT pmcid) FROM parquet_pmcids WHERE pmcid IS NOT NULL
            """).fetchone()

            logger.info(f"  Found {count_result[0]:,} unique PMCIDs in parquet files")
            total_updated += count_result[0]

        except Exception as e:
            logger.warning(f"Error processing {pattern}: {e}")
            continue

    con.commit()

    # Report statistics
    stats = con.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN has_oddpub_v7 THEN 1 ELSE 0 END) as processed
        FROM pmcids
    """).fetchone()

    logger.info(f"oddpub v7.2.3: {stats[1]:,}/{stats[0]:,} PMCIDs processed ({100*stats[1]/stats[0]:.1f}%)")


def update_from_oddpub_v5(con: duckdb.DuckDBPyConnection, rtrans_dir: Path):
    """
    Update oddpub v5 status from rtransparent output directory.
    """
    logger.info(f"Updating oddpub v5 status from {rtrans_dir}")

    pattern = str(rtrans_dir / "*.parquet")

    try:
        con.execute(f"""
            UPDATE pmcids
            SET has_oddpub_v5 = TRUE,
                oddpub_v5_updated_at = CURRENT_TIMESTAMP
            WHERE pmcid IN (
                SELECT DISTINCT
                    CASE WHEN pmcid LIKE 'PMC%' THEN pmcid ELSE 'PMC' || pmcid END
                FROM read_parquet('{pattern}')
                WHERE pmcid IS NOT NULL
            )
        """)
        con.commit()

        stats = con.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN has_oddpub_v5 THEN 1 ELSE 0 END) as processed
            FROM pmcids
        """).fetchone()

        logger.info(f"oddpub v5: {stats[1]:,}/{stats[0]:,} PMCIDs processed ({100*stats[1]/stats[0]:.1f}%)")

    except Exception as e:
        logger.error(f"Error updating oddpub v5 status: {e}")


def update_from_rtransparent(con: duckdb.DuckDBPyConnection, rtrans_dir: Path):
    """Update rtransparent status from output parquet directory."""
    logger.info(f"Updating rtransparent status from {rtrans_dir}")

    pattern = str(rtrans_dir / "*.parquet")

    try:
        con.execute(f"""
            UPDATE pmcids
            SET has_rtransparent = TRUE,
                rtransparent_updated_at = CURRENT_TIMESTAMP
            WHERE pmcid IN (
                SELECT DISTINCT
                    CASE WHEN pmcid LIKE 'PMC%' THEN pmcid ELSE 'PMC' || pmcid END
                FROM read_parquet('{pattern}')
                WHERE pmcid IS NOT NULL
            )
        """)
        con.commit()

        stats = con.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN has_rtransparent THEN 1 ELSE 0 END) as processed
            FROM pmcids
        """).fetchone()

        logger.info(f"rtransparent: {stats[1]:,}/{stats[0]:,} PMCIDs processed ({100*stats[1]/stats[0]:.1f}%)")

    except Exception as e:
        logger.error(f"Error updating rtransparent status: {e}")


def update_from_metadata(con: duckdb.DuckDBPyConnection, metadata_dir: Path):
    """Update metadata extraction status from parquet directory."""
    logger.info(f"Updating metadata status from {metadata_dir}")

    pattern = str(metadata_dir / "*.parquet")

    try:
        con.execute(f"""
            UPDATE pmcids
            SET has_metadata = TRUE,
                metadata_updated_at = CURRENT_TIMESTAMP
            WHERE pmcid IN (
                SELECT DISTINCT pmcid_pmc FROM read_parquet('{pattern}')
                WHERE pmcid_pmc IS NOT NULL
            )
        """)
        con.commit()

        stats = con.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN has_metadata THEN 1 ELSE 0 END) as processed
            FROM pmcids
        """).fetchone()

        logger.info(f"metadata: {stats[1]:,}/{stats[0]:,} PMCIDs processed ({100*stats[1]/stats[0]:.1f}%)")

    except Exception as e:
        logger.error(f"Error updating metadata status: {e}")


def update_from_compact(con: duckdb.DuckDBPyConnection, compact_dir: Path):
    """Update compact_rtrans status from parquet directory."""
    logger.info(f"Updating compact status from {compact_dir}")

    pattern = str(compact_dir / "*.parquet")

    try:
        con.execute(f"""
            UPDATE pmcids
            SET has_compact = TRUE,
                compact_updated_at = CURRENT_TIMESTAMP
            WHERE pmcid IN (
                SELECT DISTINCT pmcid FROM read_parquet('{pattern}')
                WHERE pmcid IS NOT NULL
            )
        """)
        con.commit()

        stats = con.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN has_compact THEN 1 ELSE 0 END) as processed
            FROM pmcids
        """).fetchone()

        logger.info(f"compact: {stats[1]:,}/{stats[0]:,} PMCIDs processed ({100*stats[1]/stats[0]:.1f}%)")

    except Exception as e:
        logger.error(f"Error updating compact status: {e}")


def update_article_types(con: duckdb.DuckDBPyConnection, rtrans_dir: Path):
    """
    Update article_type from rtransparent parquet files.

    The rtrans files have a 'type' column with values like:
    - research-article, review-article, case-report, abstract, other,
    - brief-report, editorial, letter, correction, book-review, etc.
    """
    logger.info(f"Updating article types from {rtrans_dir}")

    pattern = str(rtrans_dir / "*.parquet")

    try:
        # Update article_type using JOIN with rtrans data
        # The rtrans files use pmcid_pmc for the PMCID
        # Use GROUP BY to deduplicate PMCIDs that appear in multiple parquet files
        con.execute(f"""
            UPDATE pmcids
            SET article_type = rtrans.type
            FROM (
                SELECT
                    CASE WHEN pmcid_pmc LIKE 'PMC%' THEN pmcid_pmc ELSE 'PMC' || pmcid_pmc END as pmcid,
                    FIRST(type) as type
                FROM read_parquet('{pattern}')
                WHERE pmcid_pmc IS NOT NULL AND type IS NOT NULL
                GROUP BY CASE WHEN pmcid_pmc LIKE 'PMC%' THEN pmcid_pmc ELSE 'PMC' || pmcid_pmc END
            ) AS rtrans
            WHERE pmcids.pmcid = rtrans.pmcid
        """)
        con.commit()

        # Report statistics
        stats = con.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN article_type IS NOT NULL THEN 1 ELSE 0 END) as with_type
            FROM pmcids
        """).fetchone()

        logger.info(f"article_type: {stats[1]:,}/{stats[0]:,} PMCIDs have type ({100*stats[1]/stats[0]:.1f}%)")

        # Show distribution
        type_counts = con.execute("""
            SELECT article_type, COUNT(*) as cnt
            FROM pmcids
            WHERE article_type IS NOT NULL
            GROUP BY article_type
            ORDER BY cnt DESC
            LIMIT 15
        """).fetchall()

        logger.info("Article type distribution:")
        for row in type_counts:
            logger.info(f"  {row[0]}: {row[1]:,}")

    except Exception as e:
        logger.error(f"Error updating article types: {e}")


def show_status(con: duckdb.DuckDBPyConnection):
    """Display overall processing status."""
    print("\n" + "=" * 70)
    print("PMCID REGISTRY STATUS")
    print("=" * 70)

    # Overall counts
    stats = con.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(DISTINCT pmc_dir) as pmc_dirs,
            SUM(CASE WHEN has_oddpub_v7 THEN 1 ELSE 0 END) as oddpub_v7,
            SUM(CASE WHEN has_oddpub_v5 THEN 1 ELSE 0 END) as oddpub_v5,
            SUM(CASE WHEN has_rtransparent THEN 1 ELSE 0 END) as rtransparent,
            SUM(CASE WHEN has_metadata THEN 1 ELSE 0 END) as metadata,
            SUM(CASE WHEN has_compact THEN 1 ELSE 0 END) as compact
        FROM pmcids
    """).fetchone()

    total = stats[0]
    print(f"\nTotal PMCIDs:     {total:,}")
    print(f"PMC directories:  {stats[1]:,}")

    print("\nPipeline Status:")
    print("-" * 60)
    pipelines = [
        ('oddpub v7.2.3', stats[2]),
        ('oddpub v5', stats[3]),
        ('rtransparent', stats[4]),
        ('metadata', stats[5]),
        ('compact', stats[6])
    ]

    for name, count in pipelines:
        pct = 100 * count / total if total > 0 else 0
        missing = total - count
        bar_len = int(pct / 2)
        bar = '█' * bar_len + '░' * (50 - bar_len)
        print(f"  {name:15} {count:>10,} ({pct:5.1f}%)  missing: {missing:>10,}")

    # By PMC range for oddpub v7
    print("\noddpub v7.2.3 by PMC range:")
    print("-" * 60)

    range_stats = con.execute("""
        SELECT
            SUBSTRING(pmc_dir, 1, 6) || 'xxxxxx' as pmc_range,
            COUNT(*) as total,
            SUM(CASE WHEN has_oddpub_v7 THEN 1 ELSE 0 END) as processed
        FROM pmcids
        GROUP BY 1
        ORDER BY 1
    """).fetchall()

    for row in range_stats:
        pct = 100 * row[2] / row[1] if row[1] > 0 else 0
        missing = row[1] - row[2]
        print(f"  {row[0]}: {row[2]:>8,}/{row[1]:>8,} ({pct:5.1f}%)  missing: {missing:>8,}")

    print("\n" + "=" * 70)


def get_queued_chunks_from_squeue(user: str) -> Set[str]:
    """Get PMCIDs currently being processed from squeue."""
    queued_pmcids = set()

    try:
        result = subprocess.run(
            ['squeue', '-u', user, '-h', '-o', '%i'],
            capture_output=True, text=True, timeout=30
        )

        if result.returncode != 0:
            return queued_pmcids

        job_ids = [j for j in result.stdout.strip().split('\n') if j]
        if not job_ids:
            return queued_pmcids

        logger.info(f"Found {len(job_ids)} jobs in queue")

        # Check swarm directories for file lists
        checked_dirs = set()
        for job_id in job_ids:
            result = subprocess.run(
                ['scontrol', 'show', 'job', job_id],
                capture_output=True, text=True, timeout=10
            )
            if result.returncode != 0:
                continue

            match = re.search(r'Command=(\S+)', result.stdout)
            if not match:
                continue

            swarm_dir = Path(match.group(1)).parent
            if not swarm_dir.exists() or str(swarm_dir) in checked_dirs:
                continue

            checked_dirs.add(str(swarm_dir))

            # Read file lists from cmd.* files
            for cmd_file in swarm_dir.glob('cmd.*'):
                try:
                    content = cmd_file.read_text()
                    # Look for file list paths and read them
                    for match in re.finditer(r'--file-list\s+(\S+)', content):
                        filelist_path = Path(match.group(1))
                        if filelist_path.exists():
                            for line in filelist_path.read_text().strip().split('\n'):
                                pmcid = Path(line.strip()).stem
                                if pmcid.startswith('PMC'):
                                    queued_pmcids.add(pmcid)
                except Exception:
                    pass

        logger.info(f"Found {len(queued_pmcids)} PMCIDs in queue")

    except Exception as e:
        logger.warning(f"Error checking queue: {e}")

    return queued_pmcids


def generate_retry_swarm(
    con: duckdb.DuckDBPyConnection,
    pipeline: str,
    xml_base_dir: Path,
    output_dir: Path,
    container_sif: Path,
    batch_size: int = 50,
    jobs_per_line: int = 4,
    user: Optional[str] = None,
    skip_queue_check: bool = False,
    dry_run: bool = False
):
    """
    Generate retry swarm for missing PMCIDs.

    Instead of re-running whole chunks, this batches missing PMCIDs efficiently.
    """
    flag_col = f'has_{pipeline}'

    print("=" * 70)
    print(f"RETRY SWARM GENERATOR - {pipeline}")
    print("=" * 70)

    # Get missing PMCIDs
    missing_df = con.execute(f"""
        SELECT pmcid, xml_path, pmc_dir
        FROM pmcids
        WHERE {flag_col} = FALSE
        ORDER BY pmc_dir, pmcid
    """).fetchdf()

    total_missing = len(missing_df)
    print(f"\nTotal missing PMCIDs: {total_missing:,}")

    if total_missing == 0:
        print("All PMCIDs processed! No retry needed.")
        return

    # Check queue for already-running PMCIDs
    queued_pmcids = set()
    if not skip_queue_check and user:
        print(f"\nChecking HPC queue for user {user}...")
        queued_pmcids = get_queued_chunks_from_squeue(user)
        if queued_pmcids:
            missing_df = missing_df[~missing_df['pmcid'].isin(queued_pmcids)]
            print(f"  {len(queued_pmcids)} PMCIDs already in queue")
            print(f"  {len(missing_df)} PMCIDs need retry")

    if len(missing_df) == 0:
        print("All missing PMCIDs are already queued!")
        return

    # Summary by PMC range
    print("\nMissing by PMC range:")
    for pmc_range, group in missing_df.groupby(missing_df['pmc_dir'].str[:6] + 'xxxxxx'):
        print(f"  {pmc_range}: {len(group):,}")

    if dry_run:
        num_batches = (len(missing_df) + batch_size - 1) // batch_size
        num_swarm_lines = (num_batches + jobs_per_line - 1) // jobs_per_line
        print(f"\n[DRY RUN] Would create:")
        print(f"  - {num_batches} batches of up to {batch_size} PMCIDs each")
        print(f"  - {num_swarm_lines} swarm lines ({jobs_per_line} jobs per line)")
        return

    # Create batches
    print(f"\nCreating batches of {batch_size} PMCIDs...")

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filelist_dir = output_dir / f".retry_filelists_{timestamp}"
    filelist_dir.mkdir(parents=True, exist_ok=True)

    commands = []
    batch_num = 0

    for i in range(0, len(missing_df), batch_size):
        batch = missing_df.iloc[i:i + batch_size]
        batch_num += 1

        # Create file list for this batch
        # Rewrite XML paths to use the provided xml_base_dir
        # Registry stores: /home/ec2-user/.../PMC001xxxxxx/PMC1234567.xml
        # Need: <xml_base_dir>/PMC001xxxxxx/PMC1234567.xml
        filelist_path = filelist_dir / f"batch_{batch_num:05d}.txt"
        with open(filelist_path, 'w') as f:
            for _, row in batch.iterrows():
                # Extract relative path: PMC001xxxxxx/PMC1234567.xml
                pmc_dir = row['pmc_dir']
                pmcid = row['pmcid']
                xml_file = f"{xml_base_dir}/{pmc_dir}/{pmcid}.xml"
                f.write(f"{xml_file}\n")

        # Output file for this batch
        output_file = output_dir / f"retry_{timestamp}_batch_{batch_num:05d}_results.parquet"

        # Build command
        cmd = (
            f"(. /usr/local/current/apptainer/app_conf/sing_binds && "
            f"apptainer exec {container_sif} python3 /scripts/process_extracted_xmls_with_oddpub.py "
            f"--file-list {filelist_path} --batch-size 500 --output-file {output_file})"
        )
        commands.append(cmd)

    print(f"Created {len(commands)} batches")

    # Generate swarm file
    swarm_file = Path(f"oddpub_retry_{timestamp}.swarm")

    with open(swarm_file, 'w') as f:
        for i in range(0, len(commands), jobs_per_line):
            line_cmds = commands[i:i + jobs_per_line]
            f.write(' & '.join(line_cmds) + ' & wait\n')

    packed_lines = (len(commands) + jobs_per_line - 1) // jobs_per_line

    print()
    print("=" * 70)
    print("RETRY SWARM GENERATED")
    print("=" * 70)
    print(f"Swarm file:     {swarm_file}")
    print(f"File lists dir: {filelist_dir}")
    print(f"Batches:        {len(commands)} ({batch_size} PMCIDs each)")
    print(f"Swarm lines:    {packed_lines} ({jobs_per_line} jobs per line)")
    print(f"Total PMCIDs:   {len(missing_df):,}")
    print()

    # Copy to remote
    remote_host = "osm2025"
    remote_dir = "/home/ec2-user/claude/osm-2025-12-poster-incf/hpc_scripts"

    try:
        result = subprocess.run(
            ['scp', str(swarm_file), f"{remote_host}:{remote_dir}/"],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0:
            print(f"Copied to {remote_host}:{remote_dir}/{swarm_file.name}")
    except Exception:
        print(f"Copy manually: scp {swarm_file} {remote_host}:{remote_dir}/")

    print()
    print("Submit with:")
    print()
    print(f"  swarm -f {swarm_file} \\")
    print("      -g 16 \\")
    print("      -t 2 \\")
    print("      --time 02:00:00 \\")
    print("      --gres=lscratch:5 \\")
    print("      --module apptainer \\")
    print("      --logdir /data/NIMH_scratch/adamt/osm/logs/oddpub_retry")
    print()
    print("=" * 70)


def export_missing(con: duckdb.DuckDBPyConnection, pipeline: str, output_file: Path):
    """Export missing PMCIDs for a pipeline."""
    flag_col = f'has_{pipeline}'

    result = con.execute(f"""
        SELECT pmcid, pmc_dir, xml_path
        FROM pmcids
        WHERE {flag_col} = FALSE
        ORDER BY pmc_dir, pmcid
    """).fetchdf()

    result.to_csv(output_file, index=False)
    logger.info(f"Exported {len(result):,} missing PMCIDs to {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description='PMCID Registry - Track processing status across pipelines'
    )
    parser.add_argument(
        '--db',
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f'Database file path (default: {DEFAULT_DB_PATH})'
    )

    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # init command
    init_parser = subparsers.add_parser('init', help='Initialize registry from file lists')
    init_parser.add_argument('xml_base_dir', type=Path, help='Directory with CSV file lists and XML subdirs')

    # update-oddpub-v7 command
    v7_parser = subparsers.add_parser('update-oddpub-v7', help='Update oddpub v7.2.3 status')
    v7_parser.add_argument('output_dir', type=Path, help='oddpub output directory')
    v7_parser.add_argument('--old-output-dir', type=Path, help='Old output directory')

    # update-oddpub-v5 command
    v5_parser = subparsers.add_parser('update-oddpub-v5', help='Update oddpub v5 status')
    v5_parser.add_argument('rtrans_dir', type=Path, help='rtransparent output directory')

    # update-rtransparent command
    rt_parser = subparsers.add_parser('update-rtransparent', help='Update rtransparent status')
    rt_parser.add_argument('rtrans_dir', type=Path, help='rtransparent output directory')

    # update-metadata command
    meta_parser = subparsers.add_parser('update-metadata', help='Update metadata status')
    meta_parser.add_argument('metadata_dir', type=Path, help='Metadata parquet directory')

    # update-compact command
    compact_parser = subparsers.add_parser('update-compact', help='Update compact status')
    compact_parser.add_argument('compact_dir', type=Path, help='compact_rtrans directory')

    # update-article-type command
    type_parser = subparsers.add_parser('update-article-type', help='Update article_type from rtrans files')
    type_parser.add_argument('rtrans_dir', type=Path, help='rtransparent output directory with type column')

    # status command
    subparsers.add_parser('status', help='Show processing status')

    # generate-retry command
    retry_parser = subparsers.add_parser('generate-retry', help='Generate retry swarm for missing PMCIDs')
    retry_parser.add_argument('pipeline', choices=['oddpub_v7', 'oddpub_v5', 'rtransparent', 'metadata', 'compact'])
    retry_parser.add_argument('--xml-base-dir', type=Path, required=True)
    retry_parser.add_argument('--output-dir', type=Path, required=True)
    retry_parser.add_argument('--container', type=Path, required=True)
    retry_parser.add_argument('--batch-size', type=int, default=50, help='PMCIDs per batch (default: 50)')
    retry_parser.add_argument('--jobs-per-line', type=int, default=4, help='Jobs per swarm line (default: 4)')
    retry_parser.add_argument('--user', default=os.environ.get('USER'))
    retry_parser.add_argument('--skip-queue-check', action='store_true')
    retry_parser.add_argument('--dry-run', action='store_true')

    # export-missing command
    export_parser = subparsers.add_parser('export-missing', help='Export missing PMCIDs')
    export_parser.add_argument('pipeline', choices=['oddpub_v7', 'oddpub_v5', 'rtransparent', 'metadata', 'compact'])
    export_parser.add_argument('-o', '--output', type=Path, required=True)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Determine if we need write access
    read_only = args.command in ['status', 'export-missing']
    if args.command == 'generate-retry':
        read_only = True

    # Connect to database
    if args.command == 'init':
        # For init, create new database
        if args.db.exists():
            logger.warning(f"Database already exists: {args.db}")
            response = input("Overwrite? [y/N] ")
            if response.lower() != 'y':
                sys.exit(0)
            args.db.unlink()

    con = get_connection(args.db, read_only=read_only)

    # Always ensure schema exists (for non-read-only)
    if not read_only:
        init_schema(con)

    if args.command == 'init':
        init_schema(con)
        init_from_filelists(con, args.xml_base_dir)
        show_status(con)

    elif args.command == 'update-oddpub-v7':
        update_from_oddpub_v7(con, args.output_dir, getattr(args, 'old_output_dir', None))
        show_status(con)

    elif args.command == 'update-oddpub-v5':
        update_from_oddpub_v5(con, args.rtrans_dir)
        show_status(con)

    elif args.command == 'update-rtransparent':
        update_from_rtransparent(con, args.rtrans_dir)
        show_status(con)

    elif args.command == 'update-metadata':
        update_from_metadata(con, args.metadata_dir)
        show_status(con)

    elif args.command == 'update-compact':
        update_from_compact(con, args.compact_dir)
        show_status(con)

    elif args.command == 'update-article-type':
        update_article_types(con, args.rtrans_dir)
        show_status(con)

    elif args.command == 'status':
        show_status(con)

    elif args.command == 'generate-retry':
        generate_retry_swarm(
            con,
            args.pipeline,
            args.xml_base_dir,
            args.output_dir,
            args.container,
            batch_size=args.batch_size,
            jobs_per_line=args.jobs_per_line,
            user=args.user,
            skip_queue_check=args.skip_queue_check,
            dry_run=args.dry_run
        )

    elif args.command == 'export-missing':
        export_missing(con, args.pipeline, args.output)

    con.close()


if __name__ == '__main__':
    main()
