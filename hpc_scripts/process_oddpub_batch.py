#!/usr/bin/env python3
"""Process a single batch of XMLs with oddpub for HPC parallel processing.

This script is designed to be run as part of a SLURM swarm job.
"""

import sys
import os
import tempfile
import subprocess
from pathlib import Path
import pandas as pd
import xml.etree.ElementTree as ET
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def extract_article_id(root, id_type):
    """Extract article ID from XML."""
    for article_id in root.findall(".//article-meta/article-id"):
        if article_id.get("pub-id-type") == id_type:
            text = article_id.text
            if text:
                return text.strip()
    return ""


def extract_text(element):
    """Recursively extract all text from an XML element."""
    text_parts = []
    if element.text:
        text_parts.append(element.text)
    for child in element:
        text_parts.append(extract_text(child))
        if child.tail:
            text_parts.append(child.tail)
    return ' '.join(text_parts)


def extract_body_text(xml_file):
    """Extract body text and IDs from XML file."""
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()

        pmid = extract_article_id(root, 'pmid')
        pmcid = extract_article_id(root, 'pmc')

        body = root.find(".//body")
        if body is not None:
            body_text = extract_text(body)
        else:
            body_text = ''

        return pmid, pmcid, body_text
    except Exception as e:
        logger.error(f"Error extracting from {xml_file}: {e}")
        return '', '', ''


def run_oddpub(text_dir, output_file):
    """Run oddpub R package on text files."""
    r_script = f"""
library(oddpub)
library(future)
library(progressr)

plan(multisession, workers = 4)
handlers(global = TRUE)

text_corpus <- pdf_load("{text_dir}", lowercase = TRUE)
results <- open_data_search(text_corpus, extract_sentences = TRUE, screen_das = "priority")
write.csv(results, "{output_file}", row.names = FALSE)
"""

    with tempfile.NamedTemporaryFile(mode='w', suffix='.R', delete=False) as f:
        f.write(r_script)
        r_script_path = f.name

    try:
        logger.info("Running oddpub R script...")
        result = subprocess.run(
            ['/usr/bin/Rscript', r_script_path],
            capture_output=True,
            text=True,
            timeout=600
        )

        if result.returncode != 0:
            logger.error(f"R script stderr: {result.stderr}")
            raise RuntimeError(f"R script failed with code {result.returncode}")

        logger.info("oddpub completed successfully")
        return True
    except subprocess.TimeoutExpired:
        logger.error("R script timed out after 600 seconds")
        raise
    finally:
        os.unlink(r_script_path)


def main(batch_file, output_file):
    """Process a batch of XML files with oddpub."""

    logger.info(f"Processing batch: {batch_file}")
    logger.info(f"Output file: {output_file}")

    # Read batch manifest
    with open(batch_file) as f:
        xml_files = [line.strip() for line in f if line.strip()]

    logger.info(f"Found {len(xml_files)} XML files in batch")

    # Create temporary directory for text files
    with tempfile.TemporaryDirectory() as temp_dir:
        logger.info(f"Extracting body text to {temp_dir}")

        # Extract body text from XMLs
        records = []
        successful = 0
        for i, xml_file in enumerate(xml_files):
            if (i + 1) % 100 == 0:
                logger.info(f"Extracted {i + 1}/{len(xml_files)} files...")

            pmid, pmcid, body_text = extract_body_text(xml_file)

            # Write text file for oddpub
            filename = Path(xml_file).stem
            text_file = Path(temp_dir) / f"{filename}.txt"

            try:
                with open(text_file, 'w') as f:
                    f.write(body_text)
                successful += 1
            except Exception as e:
                logger.error(f"Error writing {text_file}: {e}")
                continue

            records.append({
                'filename': filename,
                'pmid': pmid,
                'pmcid': pmcid
            })

        logger.info(f"Successfully extracted {successful}/{len(xml_files)} files")

        if successful == 0:
            logger.error("No files were successfully extracted")
            sys.exit(1)

        # Run oddpub
        oddpub_csv = Path(temp_dir) / 'oddpub_results.csv'
        run_oddpub(temp_dir, oddpub_csv)

        # Load oddpub results
        logger.info("Loading oddpub results...")
        oddpub_df = pd.read_csv(oddpub_csv)
        logger.info(f"oddpub returned {len(oddpub_df)} results")

        # Merge with PMIDs
        records_df = pd.DataFrame(records)

        # Clean article column for matching
        oddpub_df['filename'] = oddpub_df['article'].str.replace('.txt$', '', regex=True)

        # Merge
        logger.info("Merging oddpub results with PMIDs...")
        results = oddpub_df.merge(records_df, on='filename', how='left')

        # Drop duplicate filename column
        results = results.drop(columns=['filename'])

        # Save to parquet
        logger.info(f"Saving {len(results)} results to {output_file}...")
        results.to_parquet(output_file, index=False)

        # Summary
        open_data_count = results['is_open_data'].sum()
        open_code_count = results['is_open_code'].sum()
        logger.info(f"Summary:")
        logger.info(f"  Total results: {len(results)}")
        logger.info(f"  Open data detected: {open_data_count} ({100*open_data_count/len(results):.1f}%)")
        logger.info(f"  Open code detected: {open_code_count} ({100*open_code_count/len(results):.1f}%)")

    logger.info("Batch processing complete!")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: process_oddpub_batch.py <batch_file> <output_file>")
        print("")
        print("Arguments:")
        print("  batch_file   : Text file listing XML files to process (one per line)")
        print("  output_file  : Output parquet file path")
        sys.exit(1)

    batch_file = sys.argv[1]
    output_file = sys.argv[2]

    if not os.path.exists(batch_file):
        logger.error(f"Batch file does not exist: {batch_file}")
        sys.exit(1)

    # Create output directory if needed
    output_dir = Path(output_file).parent
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        main(batch_file, output_file)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
