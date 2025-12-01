#!/usr/bin/env python3
"""
Process extracted PMC XML files with oddpub (no tar.gz extraction needed)

This script processes XML files that have already been extracted to disk,
avoiding the overhead of tar.gz extraction.
"""

import argparse
import logging
import os
import sys
import tempfile
import traceback
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import subprocess
import time

import pandas as pd
from lxml import etree

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def extract_body_text_from_xml(xml_path: Path) -> Tuple[str, str, str]:
    """
    Extract PMID, PMCID and body text from PMC XML file.

    Returns:
        Tuple of (pmid, pmcid, body_text)
    """
    try:
        # Parse XML
        parser = etree.XMLParser(recover=True, encoding='utf-8')
        root = etree.parse(str(xml_path), parser=parser).getroot()

        # Extract PMID
        pmid = ""
        pmid_elem = root.find('.//article-id[@pub-id-type="pmid"]')
        if pmid_elem is not None and pmid_elem.text:
            pmid = pmid_elem.text.strip()

        # Extract PMCID
        pmcid = ""
        pmcid_elem = root.find('.//article-id[@pub-id-type="pmc"]')
        if pmcid_elem is not None and pmcid_elem.text:
            pmcid = f"PMC{pmcid_elem.text.strip()}"

        # Extract body text
        body_text_parts = []

        # Get all paragraphs from body
        for p in root.findall('.//body//p'):
            text = ' '.join(p.itertext()).strip()
            if text:
                body_text_parts.append(text)

        # Also get figure captions
        for caption in root.findall('.//fig//caption//p'):
            text = ' '.join(caption.itertext()).strip()
            if text:
                body_text_parts.append(text)

        # Join all text
        body_text = '\n\n'.join(body_text_parts)

        if not body_text:
            logger.debug(f"No body text found in {xml_path}")

        return pmid, pmcid, body_text

    except Exception as e:
        logger.error(f"Error parsing XML from {xml_path}: {str(e)}")
        return "", "", ""


def run_oddpub_r(input_dir: Path, output_file: Path) -> bool:
    """
    Run the oddpub R script on a directory of text files.

    Returns True if successful, False otherwise.
    """
    # Create R script that processes the files
    # Uses pdf_load() to load text files - this is what oddpub expects
    r_script_content = '''
library(oddpub)

args <- commandArgs(trailingOnly = TRUE)
input_dir <- args[1]
output_file <- args[2]

cat("R script starting\\n")
cat("Input directory:", input_dir, "\\n")
cat("Output file:", output_file, "\\n")

# Load text files using oddpub's pdf_load function
cat("Loading text files with pdf_load...\\n")
text_corpus <- pdf_load(input_dir)
cat("Loaded", length(text_corpus), "documents\\n")

if (length(text_corpus) == 0) {
    stop("No text files loaded from input directory")
}

# Run open data search
cat("Running open_data_search...\\n")
results <- open_data_search(text_corpus, extract_sentences = TRUE, screen_das = "priority")

cat("oddpub returned", nrow(results), "rows\\n")

# Write results
write.csv(results, output_file, row.names = FALSE)

cat("Successfully processed", nrow(results), "articles\\n")
'''

    # Write R script to temporary file
    r_script_path = input_dir / 'run_oddpub.R'
    r_script_path.write_text(r_script_content)

    try:
        # Run R script (Rscript is in PATH in container)
        # Timeout: 3600 sec (60 min) for 500-file batches
        result = subprocess.run(
            ['Rscript', str(r_script_path), str(input_dir), str(output_file)],
            capture_output=True,
            text=True,
            timeout=3600  # 60 minute timeout
        )

        if result.returncode == 0:
            logger.info(f"R script completed successfully")
            if result.stdout:
                logger.info(f"R stdout: {result.stdout}")
            return True
        else:
            logger.error(f"R script failed with return code {result.returncode}")
            logger.error(f"R stdout: {result.stdout}")
            logger.error(f"R stderr: {result.stderr}")
            return False

    except subprocess.TimeoutExpired:
        logger.error(f"R script timed out after 60 minutes")
        return False
    except Exception as e:
        logger.error(f"Error running R script: {e}")
        logger.debug(traceback.format_exc())
        return False
    finally:
        # Clean up R script
        if r_script_path.exists():
            r_script_path.unlink()


def write_text_files(records: List[Dict], output_dir: Path) -> List[Path]:
    """Write text content to files for oddpub processing."""
    written_files = []

    for record in records:
        # Use PMCID as filename if available, otherwise use full source name
        if record.get('pmcid'):
            filename = f"{record['pmcid']}.txt"
        else:
            # Sanitize source name for use as filename
            filename = record['filename'].replace('/', '_').replace(':', '_') + '.txt'

        filepath = output_dir / filename

        try:
            filepath.write_text(record['body_text'], encoding='utf-8')
            written_files.append(filepath)
        except Exception as e:
            logger.error(f"Error writing {filepath}: {e}")

    return written_files


def process_batch(records: List[Dict], batch_num: int, output_dir: Path) -> Optional[pd.DataFrame]:
    """Process a batch of records through oddpub."""
    logger.info(f"Processing batch {batch_num} ({len(records)} records)")

    # Determine temp directory location
    # Priority: /lscratch (HPC SLURM) > output_dir (always accessible) > /tmp (may not be bound in container)
    temp_base = None
    if 'SLURM_JOB_ID' in os.environ:
        lscratch_dir = Path(f"/lscratch/{os.environ['SLURM_JOB_ID']}")
        if lscratch_dir.exists():
            temp_base = str(lscratch_dir)
            logger.debug(f"Using /lscratch for temporary files: {temp_base}")

    # If not on SLURM, use output_dir parent to ensure container can access it
    if temp_base is None:
        temp_base = str(output_dir)
        logger.info(f"Using output directory for temporary files: {temp_base}")

    # Create temporary directory for text files
    with tempfile.TemporaryDirectory(prefix=f'oddpub_batch_{batch_num}_', dir=temp_base) as temp_dir:
        temp_path = Path(temp_dir)
        logger.info(f"Created temp directory: {temp_path}")

        # Write text files
        written_files = write_text_files(records, temp_path)
        logger.info(f"Wrote {len(written_files)} text files to {temp_path}")

        # List files for debugging
        txt_files = list(temp_path.glob("*.txt"))
        logger.info(f"Text files in temp dir: {len(txt_files)} files")

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
            df['filename'] = df['article'].map(lambda x: record_map.get(x, {}).get('filename', ''))

            return df

        except Exception as e:
            logger.error(f"Error loading results from {output_csv}: {e}")
            logger.debug(traceback.format_exc())
            return None


def process_xml_files(xml_files: List[Path], batch_size: int, output_file: Path):
    """Process a list of XML files through oddpub."""

    logger.info(f"Processing {len(xml_files)} XML files")
    logger.info(f"Batch size: {batch_size}")
    logger.info(f"Output file: {output_file}")

    all_results = []
    batch_records = []
    batch_num = 0
    processed_count = 0

    start_time = time.time()

    for i, xml_file in enumerate(xml_files, 1):
        try:
            # Extract text
            pmid, pmcid, body_text = extract_body_text_from_xml(xml_file)

            if body_text:
                batch_records.append({
                    'filename': str(xml_file),
                    'pmid': pmid,
                    'pmcid': pmcid,
                    'body_text': body_text
                })
                processed_count += 1

            # Process batch when it reaches batch_size
            if len(batch_records) >= batch_size:
                batch_num += 1
                results_df = process_batch(batch_records, batch_num, output_file.parent)
                if results_df is not None:
                    all_results.append(results_df)
                batch_records = []

            # Progress indicator
            if i % 1000 == 0:
                elapsed = time.time() - start_time
                rate = i / elapsed
                logger.info(f"  Processed {i}/{len(xml_files)} files ({i*100/len(xml_files):.1f}%), "
                          f"Rate: {rate:.1f} files/sec")

        except Exception as e:
            logger.error(f"Error processing {xml_file}: {e}")
            logger.debug(traceback.format_exc())
            continue

    # Process final batch
    if batch_records:
        batch_num += 1
        results_df = process_batch(batch_records, batch_num, output_file.parent)
        if results_df is not None:
            all_results.append(results_df)

    # Save all results
    if all_results:
        final_df = pd.concat(all_results, ignore_index=True)
        final_df.to_parquet(output_file, index=False)
        logger.info(f"Successfully wrote {len(final_df)} results to {output_file}")

        elapsed = time.time() - start_time
        logger.info(f"Total time: {elapsed/60:.1f} minutes ({processed_count/elapsed:.1f} files/sec)")
    else:
        logger.warning("No results to save")


def main():
    parser = argparse.ArgumentParser(
        description='Process extracted PMC XML files with oddpub R package'
    )
    parser.add_argument('xml_dir', nargs='?', default=None,
                      help='Directory containing extracted XML files (optional if --file-list is used)')
    parser.add_argument('--file-list', '-f', type=str, default=None,
                      help='Path to a text file containing one XML file path per line')
    parser.add_argument('--output-file', '-o', required=True,
                      help='Output parquet file path')
    parser.add_argument('--batch-size', '-b', type=int, default=500,
                      help='Number of files to process per oddpub batch (default: 500)')
    parser.add_argument('--pattern', '-p', default='*.xml',
                      help='Pattern to match XML files when using xml_dir (default: *.xml)')
    parser.add_argument('--max-files', '-m', type=int, default=None,
                      help='Maximum number of files to process (for testing)')

    args = parser.parse_args()

    # Print header
    logger.info("======================================================================")
    logger.info("PMC XML Processing with oddpub (Extracted Files Version)")
    logger.info("======================================================================")

    # Determine input mode: file list or directory
    if args.file_list:
        # Read file list from text file
        file_list_path = Path(args.file_list)
        if not file_list_path.exists():
            logger.error(f"File list not found: {file_list_path}")
            sys.exit(1)

        with open(file_list_path, 'r') as f:
            xml_files = [Path(line.strip()) for line in f if line.strip()]

        logger.info(f"Read {len(xml_files)} file paths from {file_list_path}")

    elif args.xml_dir:
        # Find XML files in directory
        xml_dir = Path(args.xml_dir)
        if not xml_dir.exists():
            logger.error(f"XML directory not found: {xml_dir}")
            sys.exit(1)

        xml_files = sorted(xml_dir.glob(args.pattern))
        logger.info(f"Found {len(xml_files)} XML files in {xml_dir}")

    else:
        logger.error("Either xml_dir or --file-list must be provided")
        sys.exit(1)

    if args.max_files:
        xml_files = xml_files[:args.max_files]

    if len(xml_files) == 0:
        logger.error("No XML files found")
        sys.exit(1)

    # Process files
    output_file = Path(args.output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    process_xml_files(xml_files, args.batch_size, output_file)

    logger.info("Processing complete!")


if __name__ == '__main__':
    main()
