#!/usr/bin/env python3
"""
Process PMCOA XMLs with oddpub R package from tar.gz archives

Streams through tar.gz archives, extracts body text from XMLs,
processes with oddpub R package for open data/code detection.
Does not require decompression of tarballs to disk.

Usage:
    python process_pmcoa_with_oddpub.py [options] <tar_directory>

Examples:
    python process_pmcoa_with_oddpub.py ~/claude/pmcoaXMLs/raw_download/
    python process_pmcoa_with_oddpub.py --limit 2 --batch-size 100 ~/claude/pmcoaXMLs/raw_download/
"""

import argparse
import sys
import tarfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import pandas as pd
import subprocess
import tempfile
import shutil
import time
import logging
import traceback
import os

# Set up logger
logger = logging.getLogger(__name__)


def setup_logging(log_level='INFO'):
    """Configure logging."""
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {log_level}')

    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    root_logger.handlers = []

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)


NAMESPACES = {
    'xlink': 'http://www.w3.org/1999/xlink',
    'mml': 'http://www.w3.org/1998/Math/MathML',
    'ali': 'http://www.niso.org/schemas/ali/1.0/'
}


def extract_text(element: Optional[ET.Element]) -> str:
    """Extract all text content from an element and its children."""
    if element is None:
        return ''

    texts = []
    if element.text:
        texts.append(element.text.strip())

    for child in element:
        child_text = extract_text(child)
        if child_text:
            texts.append(child_text)
        if child.tail:
            texts.append(child.tail.strip())

    return ' '.join(filter(None, texts))


def extract_article_id(root: ET.Element, pub_id_type: str) -> str:
    """Extract specific article ID from XML."""
    article_ids = root.findall(".//article-meta/article-id")
    for aid in article_ids:
        if aid.get('pub-id-type', '') == pub_id_type:
            return (aid.text or '').strip()
    return ''


def extract_body_text_from_xml(xml_data: bytes, source_name: str) -> Tuple[str, str, str]:
    """
    Extract body text from PMC XML.

    Returns:
        (pmid, pmcid, body_text)
    """
    try:
        root = ET.fromstring(xml_data)

        # Get identifiers
        pmid = extract_article_id(root, 'pmid')
        pmcid = extract_article_id(root, 'pmc')

        # Extract body text
        body = root.find(".//body")
        if body is not None:
            body_text = extract_text(body)
        else:
            body_text = ''

        return pmid, pmcid, body_text

    except Exception as e:
        logger.error(f"Error extracting text from {source_name}: {e}")
        return '', '', ''


def write_text_files(records: List[Dict], temp_dir: Path) -> List[Path]:
    """
    Write text records to temporary text files.

    Args:
        records: List of dicts with keys: filename, pmid, pmcid, body_text
        temp_dir: Directory to write text files

    Returns:
        List of paths to written text files
    """
    written_files = []

    for record in records:
        if not record['body_text']:
            continue

        # Use PMCID as filename if available, otherwise use source filename
        if record['pmcid']:
            txt_filename = f"PMC{record['pmcid']}.txt"
        elif record['pmid']:
            txt_filename = f"PMID{record['pmid']}.txt"
        else:
            # Fall back to sanitized source name
            safe_name = record['filename'].replace(':', '_').replace('/', '_')
            txt_filename = safe_name.replace('.xml', '.txt')

        txt_path = temp_dir / txt_filename

        try:
            with open(txt_path, 'w', encoding='utf-8') as f:
                f.write(record['body_text'])
            written_files.append(txt_path)
            logger.debug(f"Wrote {txt_path.name}")
        except Exception as e:
            logger.error(f"Error writing {txt_path}: {e}")

    return written_files


def run_oddpub_r(text_dir: Path, output_file: Path) -> bool:
    """
    Run oddpub R package on text files.

    Args:
        text_dir: Directory containing .txt files
        output_file: Path to save results (CSV)

    Returns:
        True if successful
    """
    r_script = f"""
library(oddpub)
library(future)
library(progressr)

# Set up parallel processing
plan(multisession, workers = 4)
handlers(global = TRUE)

# Load text files
cat("Loading text files from {text_dir}\\n")
text_corpus <- pdf_load("{text_dir}", lowercase = TRUE)
cat("Loaded", length(text_corpus), "documents\\n")

# Run open data search
cat("Running open data search...\\n")
results <- open_data_search(text_corpus, extract_sentences = TRUE, screen_das = "priority")

# Save results
cat("Saving results to {output_file}\\n")
write.csv(results, "{output_file}", row.names = FALSE)
cat("Complete!\\n")
"""

    # Write R script to temp file
    r_script_path = text_dir.parent / "run_oddpub.R"
    with open(r_script_path, 'w') as f:
        f.write(r_script)

    try:
        logger.info(f"Running oddpub R script on {len(list(text_dir.glob('*.txt')))} files")

        # Run R script (Rscript is in PATH in both container and native environments)
        # Timeout: 3600 sec (60 min) for 500-file batches at ~6.7 sec/file = 55 min typical
        result = subprocess.run(
            ['Rscript', str(r_script_path)],
            capture_output=True,
            text=True,
            timeout=3600  # 60 minute timeout
        )

        if result.returncode != 0:
            logger.error(f"R script failed with return code {result.returncode}")
            logger.error(f"STDOUT: {result.stdout}")
            logger.error(f"STDERR: {result.stderr}")
            return False

        logger.info("R script completed successfully")
        logger.debug(f"R output: {result.stdout}")

        # Check if output file was created
        if not output_file.exists():
            logger.error(f"Output file not created: {output_file}")
            return False

        return True

    except subprocess.TimeoutExpired:
        logger.error("R script timed out after 60 minutes")
        return False
    except Exception as e:
        logger.error(f"Error running R script: {e}")
        logger.debug(traceback.format_exc())
        return False
    finally:
        # Clean up R script
        if r_script_path.exists():
            r_script_path.unlink()


def process_batch(records: List[Dict], batch_num: int, output_dir: Path) -> Optional[pd.DataFrame]:
    """
    Process a batch of records with oddpub.

    Args:
        records: List of record dicts
        batch_num: Batch number for logging
        output_dir: Directory for output files

    Returns:
        DataFrame with oddpub results, or None if failed
    """
    logger.info(f"Processing batch {batch_num} ({len(records)} records)")

    # Use /lscratch on HPC if available (faster local SSD), otherwise use default /tmp
    temp_base = None
    if 'SLURM_JOB_ID' in os.environ:
        lscratch_dir = Path(f"/lscratch/{os.environ['SLURM_JOB_ID']}")
        if lscratch_dir.exists():
            temp_base = str(lscratch_dir)
            logger.debug(f"Using /lscratch for temporary files: {temp_base}")

    # Create temporary directory for text files
    with tempfile.TemporaryDirectory(prefix=f'oddpub_batch_{batch_num}_', dir=temp_base) as temp_dir:
        temp_path = Path(temp_dir)
        logger.debug(f"Created temp directory: {temp_path}")

        # Write text files
        logger.info(f"Writing {len(records)} text files")
        written_files = write_text_files(records, temp_path)
        logger.info(f"Wrote {len(written_files)} text files")

        if len(written_files) == 0:
            logger.warning("No text files written - skipping batch")
            return None

        # Run oddpub
        output_csv = temp_path / f'oddpub_results_batch_{batch_num}.csv'
        success = run_oddpub_r(temp_path, output_csv)

        if not success:
            logger.error(f"oddpub processing failed for batch {batch_num}")
            return None

        # Load results
        try:
            df = pd.read_csv(output_csv)
            logger.info(f"Loaded {len(df)} results from oddpub")

            # Add metadata from records
            # Match by filename (oddpub uses filename as 'article' column)
            record_map = {
                (Path(r['filename']).stem if r['pmcid'] else r['filename']): r
                for r in records
            }

            # Add PMID and PMCID columns
            df['pmid'] = df['article'].map(lambda x: record_map.get(x, {}).get('pmid', ''))
            df['pmcid'] = df['article'].map(lambda x: record_map.get(x, {}).get('pmcid', ''))

            return df

        except Exception as e:
            logger.error(f"Error loading results from {output_csv}: {e}")
            logger.debug(traceback.format_exc())
            return None


def process_tarball(tarball_path: Path, batch_size: int, output_dir: Path, all_results: List[pd.DataFrame],
                    max_files: int = None, start_index: int = 0, chunk_size: int = None) -> int:
    """
    Process one tarball, extracting and processing XMLs in batches.

    Args:
        tarball_path: Path to tar.gz file
        batch_size: Number of files to process per batch
        output_dir: Output directory for results
        all_results: List to append result DataFrames to
        max_files: Maximum files to process
        start_index: Index of first XML to process (0-based, for chunking)
        chunk_size: Number of XMLs to process from start_index (for chunking)

    Returns:
        Number of files processed
    """
    logger.info(f"Opening tarball: {tarball_path.name}")
    if start_index > 0 or chunk_size:
        logger.info(f"Chunking: processing XMLs {start_index} to {start_index + (chunk_size or 'end')}")

    count = 0
    batch_records = []
    batch_num = 0

    try:
        with tarfile.open(tarball_path, 'r:gz') as tar:
            members = tar.getmembers()
            xml_members = [m for m in members if m.name.endswith('.xml') and m.isfile()]
            logger.info(f"Found {len(xml_members)} XML files in {tarball_path.name}")

            # Calculate end index for chunking
            end_index = len(xml_members)
            if chunk_size:
                end_index = min(start_index + chunk_size, len(xml_members))

            # Slice to the chunk we want
            xml_members = xml_members[start_index:end_index]
            logger.info(f"Processing {len(xml_members)} XML files (index {start_index} to {end_index-1})")

            for i, member in enumerate(xml_members, 1):
                try:
                    # Extract file to memory
                    f = tar.extractfile(member)
                    if f is None:
                        continue

                    xml_data = f.read()
                    source_name = f"{tarball_path.name}:{member.name}"

                    # Extract text
                    pmid, pmcid, body_text = extract_body_text_from_xml(xml_data, source_name)

                    if body_text:
                        batch_records.append({
                            'filename': source_name,
                            'pmid': pmid,
                            'pmcid': pmcid,
                            'body_text': body_text
                        })
                        count += 1

                    # Check if we've hit max_files limit
                    if max_files and count >= max_files:
                        logger.info(f"Reached max_files limit ({max_files})")
                        break

                    # Process batch when it reaches batch_size
                    if len(batch_records) >= batch_size:
                        batch_num += 1
                        results_df = process_batch(batch_records, batch_num, output_dir)
                        if results_df is not None:
                            all_results.append(results_df)
                        batch_records = []

                    # Log progress
                    if i % 100 == 0:
                        logger.info(f"  Processed {i}/{len(xml_members)} files ({100*i/len(xml_members):.1f}%)")

                except Exception as e:
                    logger.error(f"Error processing {member.name}: {e}")
                    logger.debug(traceback.format_exc())

            # Process remaining records
            if batch_records:
                batch_num += 1
                results_df = process_batch(batch_records, batch_num, output_dir)
                if results_df is not None:
                    all_results.append(results_df)

    except Exception as e:
        logger.error(f"Error opening tarball {tarball_path}: {e}")
        logger.debug(traceback.format_exc())

    logger.info(f"Completed {tarball_path.name}: {count} files processed")
    return count


def main():
    parser = argparse.ArgumentParser(
        description='Process PMCOA XMLs with oddpub R package from tar.gz archives',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s ~/claude/pmcoaXMLs/raw_download/
  %(prog)s --limit 2 --batch-size 100 ~/claude/pmcoaXMLs/raw_download/
  %(prog)s --pattern "*baseline*" ~/claude/pmcoaXMLs/raw_download/
        """
    )

    parser.add_argument(
        'tar_path',
        type=str,
        help='Directory containing .tar.gz archives, or path to a single .tar.gz file'
    )

    parser.add_argument(
        '-o', '--output-dir',
        type=str,
        default='../../pmcoaXMLs/oddpub_out',
        help='Output directory (default: ../../pmcoaXMLs/oddpub_out)'
    )

    parser.add_argument(
        '--output-file',
        type=str,
        default=None,
        help='Output file path (for single tar.gz processing). Overrides --output-dir.'
    )

    parser.add_argument(
        '--batch-size',
        type=int,
        default=500,
        help='Number of files to process per oddpub batch (default: 500)'
    )

    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Limit number of tar.gz files to process (for testing)'
    )

    parser.add_argument(
        '--max-files',
        type=int,
        default=None,
        help='Maximum total files to process across all archives (for testing)'
    )

    parser.add_argument(
        '--start-index',
        type=int,
        default=0,
        help='Start processing from this XML file index (0-based, for chunking large tar.gz files)'
    )

    parser.add_argument(
        '--chunk-size',
        type=int,
        default=None,
        help='Process only this many XMLs starting from start-index (for chunking large tar.gz files)'
    )

    parser.add_argument(
        '--pattern',
        type=str,
        default='*.tar.gz',
        help='Glob pattern for tar.gz files (default: *.tar.gz)'
    )

    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level (default: INFO)'
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(log_level=args.log_level)
    logger.info("="*70)
    logger.info("PMCOA XML Processing with oddpub")
    logger.info("="*70)

    tar_path = Path(args.tar_path)

    # Determine if processing single file or directory
    if tar_path.is_file() and tar_path.suffix == '.gz':
        # Single tar.gz file mode
        logger.info(f"Processing single tar.gz file: {tar_path}")
        tarballs = [tar_path]

        # Handle output file
        if args.output_file:
            output_file = Path(args.output_file)
            output_dir = output_file.parent
        else:
            output_dir = Path(args.output_dir)
            output_file = output_dir / f"{tar_path.stem.replace('.tar', '')}_results.parquet"

    elif tar_path.is_dir():
        # Directory mode
        tar_dir = tar_path
        logger.info(f"Processing directory: {tar_dir}")

        # Find tarballs
        logger.info(f"Searching for files matching pattern: {args.pattern}")
        tarballs = sorted(tar_dir.glob(args.pattern))

        if not tarballs:
            logger.error(f"No tar.gz files found matching pattern '{args.pattern}' in {tar_dir}")
            return 1

        logger.info(f"Found {len(tarballs)} tar.gz file(s)")

        if args.limit:
            logger.info(f"Limiting to first {args.limit} files")
            tarballs = tarballs[:args.limit]

        # Output directory mode
        output_dir = Path(args.output_dir)
        output_file = output_dir / "oddpub_results_all.parquet"
    else:
        logger.error(f"Path is neither a .tar.gz file nor a directory: {tar_path}")
        return 1

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Output file: {output_file}")

    print(f"Found {len(tarballs)} tar.gz file(s) to process")
    print(f"Output directory: {output_dir}")
    print(f"Batch size: {args.batch_size}")
    print("=" * 70)

    # Process all tarballs
    start_time = time.time()
    all_results = []
    total_files = 0

    for i, tarball in enumerate(tarballs, 1):
        print(f"\n[{i}/{len(tarballs)}] Processing: {tarball.name}")
        logger.info(f"[{i}/{len(tarballs)}] Starting tarball: {tarball.name}")

        count = process_tarball(tarball, args.batch_size, output_dir, all_results,
                               args.max_files, args.start_index, args.chunk_size)
        total_files += count

        # Check if we've hit max_files limit
        if args.max_files and total_files >= args.max_files:
            logger.info(f"Reached max_files limit ({args.max_files}), stopping")
            break

        print(f"  Processed {count} XML files from {tarball.name}")

    elapsed = time.time() - start_time

    # Combine all results
    if all_results:
        print("\n" + "=" * 70)
        print("Combining results...")
        logger.info("Combining all results")

        combined_df = pd.concat(all_results, ignore_index=True)

        # Save combined results
        combined_df.to_parquet(output_file, index=False)

        print(f"Saved {len(combined_df)} results to {output_file}")
        logger.info(f"Saved {len(combined_df)} results to {output_file}")

        # Print summary
        print("\n" + "=" * 70)
        print("SUMMARY")
        print("=" * 70)
        print(f"  Tar.gz files processed: {len(tarballs)}")
        print(f"  Total XML files: {total_files}")
        print(f"  Total results: {len(combined_df)}")
        print(f"  Open data detected: {combined_df['is_open_data'].sum()}")
        print(f"  Open code detected: {combined_df['is_open_code'].sum()}")
        print(f"  Processing time: {elapsed:.2f} seconds ({elapsed/60:.2f} minutes)")
        print(f"  Output file: {output_file}")

        logger.info("Processing complete!")

        return 0
    else:
        logger.error("No results generated")
        print("\nNo results generated. Check logs for errors.", file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
