#!/usr/bin/env python3
"""
Streaming XML Metadata Extractor from Tar.gz Archives

Extracts XML files from tar.gz archives and processes them directly in memory
without writing intermediate files to disk. Supports incremental saving to prevent
data loss during long-running extractions.

Usage:
    python extract_from_tarballs.py [options] <tar_directory>

Examples:
    python extract_from_tarballs.py /Volumes/DSST_backup2025/osm/pmcoa/raw_download/
    python extract_from_tarballs.py --format parquet -o output.parquet /path/to/tarballs/
    python extract_from_tarballs.py --limit 5 /path/to/tarballs/  # Process only first 5 tar.gz files
    python extract_from_tarballs.py --save-every 100000 /path/to/tarballs/  # Save every 100k records
"""

import argparse
import sys
import tarfile
import gzip
import io
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Optional, Union, BinaryIO
import pandas as pd
from datetime import datetime
import time
import logging
import traceback
import psutil
import os


# Import the column definitions and extractor class from our original script
# We'll need to modify it to work with in-memory data

# Set up logger (will be configured in main)
logger = logging.getLogger(__name__)


def setup_logging(log_level='INFO', log_file=None):
    """Configure logging for the application."""
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {log_level}')

    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)

    # Clear existing handlers
    root_logger.handlers = []

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # File handler (optional)
    if log_file:
        file_handler = logging.FileHandler(log_file, mode='a')
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
        logger.info(f"Logging to file: {log_file}")


def get_memory_usage():
    """Get current memory usage in MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024


def log_resource_usage(prefix=""):
    """Log current resource usage."""
    mem_mb = get_memory_usage()
    process = psutil.Process(os.getpid())
    cpu_percent = process.cpu_percent(interval=0.1)

    logger.debug(f"{prefix}Memory: {mem_mb:.1f} MB, CPU: {cpu_percent:.1f}%")

    return mem_mb, cpu_percent


# Define all 120 columns in order (same as original)
ALL_COLUMNS = [
    # Basic metadata (copied)
    'pmid', 'pmcid_pmc', 'pmcid_uid', 'doi', 'filename', 'journal', 'publisher',
    'affiliation_institution', 'affiliation_country', 'year_epub', 'year_ppub',

    # COI columns
    'is_coi_pred', 'coi_text', 'is_coi_pmc_fn', 'is_coi_pmc_title', 'is_relevant_coi',
    'is_relevant_coi_hi', 'is_relevant_coi_lo', 'is_explicit_coi', 'coi_1', 'coi_2',
    'coi_disclosure_1', 'commercial_1', 'benefit_1', 'consultant_1', 'grants_1',
    'brief_1', 'fees_1', 'consults_1', 'connect_1', 'connect_2', 'commercial_ack_1',
    'rights_1', 'founder_1', 'advisor_1', 'paid_1', 'board_1', 'no_coi_1',
    'no_funder_role_1',

    # Funding columns
    'is_fund_pred', 'fund_text', 'fund_pmc_institute', 'fund_pmc_source',
    'fund_pmc_anysource', 'is_fund_pmc_group', 'is_fund_pmc_title',
    'is_fund_pmc_anysource', 'is_relevant_fund', 'is_explicit_fund', 'support_1',
    'support_3', 'support_4', 'support_5', 'support_6', 'support_7', 'support_8',
    'support_9', 'support_10', 'developed_1', 'received_1', 'received_2',
    'recipient_1', 'authors_1', 'authors_2', 'thank_1', 'thank_2', 'fund_1',
    'fund_2', 'fund_3', 'supported_1', 'financial_1', 'financial_2', 'financial_3',
    'grant_1', 'french_1', 'common_1', 'common_2', 'common_3', 'common_4',
    'common_5', 'acknow_1', 'disclosure_1', 'disclosure_2', 'fund_ack', 'project_ack',

    # Registration columns
    'is_register_pred', 'register_text', 'type', 'is_research', 'is_review',
    'is_reg_pmc_title', 'is_relevant_reg', 'is_method', 'is_NCT', 'is_explicit_reg',
    'prospero_1', 'registered_1', 'registered_2', 'registered_3', 'registered_4',
    'registered_5', 'not_registered_1', 'registration_1', 'registration_2',
    'registration_3', 'registration_4', 'registry_1', 'reg_title_1', 'reg_title_2',
    'reg_title_3', 'reg_title_4', 'funded_ct_1', 'ct_2', 'ct_3', 'protocol_1',

    # Open science columns
    'is_success', 'is_open_data', 'is_open_code', 'is_relevant_code', 'is_relevant_data',

    # New columns
    'file_size', 'chars_in_body'
]


class StreamingXMLMetadataExtractor:
    """Extracts metadata from XML data in memory (streams)."""

    NAMESPACES = {
        'xlink': 'http://www.w3.org/1999/xlink',
        'mml': 'http://www.w3.org/1998/Math/MathML',
        'ali': 'http://www.niso.org/schemas/ali/1.0/'
    }

    def __init__(self, output_path=None, output_format='csv', save_every=250000):
        self.records = []
        self.output_path = output_path
        self.output_format = output_format
        self.save_every = save_every
        self.total_saved = 0
        self.save_count = 0

    def extract_text(self, element: Optional[ET.Element]) -> str:
        """Extract all text content from an element and its children."""
        if element is None:
            return ''

        texts = []
        if element.text:
            texts.append(element.text.strip())

        for child in element:
            child_text = self.extract_text(child)
            if child_text:
                texts.append(child_text)
            if child.tail:
                texts.append(child.tail.strip())

        return ' '.join(filter(None, texts))

    def extract_article_ids(self, root: ET.Element) -> Dict[str, str]:
        """Extract article IDs (PMID, PMCID, DOI)."""
        ids = {'pmid': '', 'pmcid_pmc': '', 'pmcid_uid': '', 'doi': ''}

        article_ids = root.findall(".//article-meta/article-id")

        for aid in article_ids:
            pub_id_type = aid.get('pub-id-type', '')
            text = (aid.text or '').strip()

            if pub_id_type == 'pmid':
                ids['pmid'] = text
            elif pub_id_type == 'pmc':
                ids['pmcid_pmc'] = text
            elif pub_id_type == 'pmcid':
                ids['pmcid_uid'] = text
            elif pub_id_type == 'doi':
                ids['doi'] = text

        return ids

    def extract_journal_info(self, root: ET.Element) -> Dict[str, str]:
        """Extract journal and publisher information."""
        info = {'journal': '', 'publisher': ''}

        journal_title = root.find(".//journal-meta/journal-title-group/journal-title")
        if journal_title is not None:
            info['journal'] = self.extract_text(journal_title)

        publisher_parts = []
        publisher_name = root.find(".//journal-meta/publisher/publisher-name")
        if publisher_name is not None:
            publisher_parts.append(self.extract_text(publisher_name))

        publisher_loc = root.find(".//journal-meta/publisher/publisher-loc")
        if publisher_loc is not None:
            publisher_parts.append(self.extract_text(publisher_loc))

        if publisher_parts:
            info['publisher'] = '; '.join(publisher_parts)

        return info

    def extract_affiliations(self, root: ET.Element) -> Dict[str, str]:
        """Extract author affiliation information."""
        affiliations = {'affiliation_institution': '', 'affiliation_country': ''}

        aff_elements = root.findall(".//contrib-group//aff")

        institutions = []
        countries = []

        for aff in aff_elements:
            inst = aff.find(".//institution")
            if inst is not None:
                inst_text = self.extract_text(inst)
                if inst_text:
                    institutions.append(inst_text)

            country = aff.find(".//country")
            if country is not None:
                country_text = self.extract_text(country)
                if country_text:
                    countries.append(country_text)

        if institutions:
            affiliations['affiliation_institution'] = '; '.join(dict.fromkeys(institutions))
        if countries:
            affiliations['affiliation_country'] = '; '.join(dict.fromkeys(countries))

        return affiliations

    def extract_pub_dates(self, root: ET.Element) -> Dict[str, str]:
        """Extract publication dates."""
        dates = {'year_epub': '', 'year_ppub': ''}

        pub_dates = root.findall(".//article-meta/pub-date")

        for pub_date in pub_dates:
            pub_type = pub_date.get('pub-type', '')
            year_elem = pub_date.find('year')

            if year_elem is not None:
                year = year_elem.text.strip() if year_elem.text else ''

                if pub_type == 'epub':
                    dates['year_epub'] = year
                elif pub_type == 'ppub':
                    dates['year_ppub'] = year

        return dates

    def extract_article_type(self, root: ET.Element) -> str:
        """Extract article type attribute."""
        # The root element IS the article element in JATS XML
        # Check for 'article' with or without namespace
        if root.tag == 'article' or root.tag.endswith('}article'):
            return root.get('article-type', '')
        return ''

    def extract_coi_text(self, root: ET.Element) -> str:
        """Extract conflict of interest statement."""
        fn_elements = root.findall(".//fn[@fn-type]")

        for fn in fn_elements:
            fn_type = fn.get('fn-type', '').lower()
            if 'coi' in fn_type or 'conflict' in fn_type:
                return self.extract_text(fn)

        return ''

    def extract_funding_text(self, root: ET.Element) -> Dict[str, str]:
        """Extract funding information."""
        funding = {
            'fund_text': '',
            'fund_pmc_institute': '',
            'fund_pmc_source': '',
            'fund_pmc_anysource': ''
        }

        fn_elements = root.findall(".//fn[@fn-type]")
        for fn in fn_elements:
            fn_type = fn.get('fn-type', '').lower()
            if 'financial' in fn_type or 'funding' in fn_type:
                funding['fund_text'] = self.extract_text(fn)
                break

        funding_group = root.find(".//funding-group")
        if funding_group is not None:
            institutions = []
            institution_elems = funding_group.findall(".//institution")
            for inst in institution_elems:
                inst_text = self.extract_text(inst)
                if inst_text:
                    institutions.append(inst_text)

            if institutions:
                funding['fund_pmc_institute'] = '; '.join(institutions)

            funding_sources = []
            institution_wraps = funding_group.findall(".//institution-wrap")
            for wrap in institution_wraps:
                parts = []
                inst = wrap.find(".//institution")
                if inst is not None:
                    parts.append(self.extract_text(inst))

                inst_id = wrap.find(".//institution-id")
                if inst_id is not None:
                    parts.append(self.extract_text(inst_id))

                if parts:
                    funding_sources.append(''.join(parts))

            if funding_sources:
                funding['fund_pmc_source'] = '; '.join(funding_sources)
                funding['fund_pmc_anysource'] = funding['fund_pmc_source']

        return funding

    def extract_registration_text(self, root: ET.Element) -> str:
        """Extract trial registration information."""
        custom_metas = root.findall(".//custom-meta")
        for meta in custom_metas:
            meta_name = meta.find("meta-name")
            if meta_name is not None and 'regist' in self.extract_text(meta_name).lower():
                meta_value = meta.find("meta-value")
                if meta_value is not None:
                    return self.extract_text(meta_value)

        ext_links = root.findall(".//ext-link")
        for link in ext_links:
            link_text = self.extract_text(link)
            if 'nct' in link_text.lower() or 'clinical' in link_text.lower() or 'trial' in link_text.lower():
                return link_text

        return ''

    def calculate_body_chars(self, root: ET.Element) -> int:
        """Calculate number of characters in article body."""
        body = root.find(".//body")
        if body is not None:
            body_text = self.extract_text(body)
            return len(body_text)
        return 0

    def process_xml_data(self, xml_data: bytes, source_name: str, file_size: int) -> Dict[str, Union[str, int]]:
        """Process XML data from memory and extract metadata."""
        record = {col: None for col in ALL_COLUMNS}

        try:
            record['file_size'] = file_size
            record['filename'] = source_name

            logger.debug(f"Processing {source_name} ({file_size:,} bytes)")

            # Parse XML from bytes
            root = ET.fromstring(xml_data)

            # Extract all metadata
            record.update(self.extract_article_ids(root))
            record.update(self.extract_journal_info(root))
            record.update(self.extract_affiliations(root))
            record.update(self.extract_pub_dates(root))
            record['type'] = self.extract_article_type(root)
            record['coi_text'] = self.extract_coi_text(root)
            record.update(self.extract_funding_text(root))
            record['register_text'] = self.extract_registration_text(root)
            record['chars_in_body'] = self.calculate_body_chars(root)

            logger.debug(f"Successfully extracted metadata from {source_name}")

        except Exception as e:
            logger.error(f"Error processing {source_name}: {e}")
            logger.debug(traceback.format_exc())
            record['filename'] = source_name

        return record

    def process_tarball(self, tarball_path: Path) -> int:
        """Extract and process all XML files from a tar.gz archive."""
        count = 0
        logger.info(f"Opening tarball: {tarball_path.name}")
        log_resource_usage(f"[{tarball_path.name}] Before processing: ")

        try:
            with tarfile.open(tarball_path, 'r:gz') as tar:
                members = tar.getmembers()
                xml_members = [m for m in members if m.name.endswith('.xml') and m.isfile()]
                logger.info(f"Found {len(xml_members):,} XML files in {tarball_path.name}")

                for i, member in enumerate(xml_members, 1):
                    try:
                        # Log progress every 1000 files
                        if i % 1000 == 0:
                            logger.info(f"  Processing file {i:,}/{len(xml_members):,} ({100*i/len(xml_members):.1f}%)")
                            log_resource_usage(f"  [{tarball_path.name}] Progress: ")

                        # Extract file to memory
                        f = tar.extractfile(member)
                        if f is not None:
                            xml_data = f.read()

                            # Process the XML data
                            source_name = f"{tarball_path.name}:{member.name}"
                            record = self.process_xml_data(xml_data, source_name, member.size)
                            self.records.append(record)
                            count += 1

                            # Check if we need to save incrementally
                            self.check_and_save_incremental()

                    except Exception as e:
                        logger.error(f"Error extracting {member.name} from {tarball_path.name}: {e}")
                        logger.debug(traceback.format_exc())

        except Exception as e:
            logger.error(f"Error opening tarball {tarball_path}: {e}")
            logger.debug(traceback.format_exc())

        log_resource_usage(f"[{tarball_path.name}] After processing: ")
        logger.info(f"Completed {tarball_path.name}: {count:,} files processed")

        return count

    def to_dataframe(self) -> pd.DataFrame:
        """Convert records to pandas DataFrame."""
        df = pd.DataFrame(self.records, columns=ALL_COLUMNS)
        return df

    def save_csv(self, output_path: Path):
        """Save records to CSV file."""
        df = self.to_dataframe()
        df.to_csv(output_path, index=False)
        print(f"\nSaved {len(df)} records to {output_path}")

    def save_parquet(self, output_path: Path):
        """Save records to Parquet file."""
        df = self.to_dataframe()
        df.to_parquet(output_path, index=False)
        print(f"\nSaved {len(df)} records to {output_path}")

    def save_incremental(self, force=False):
        """Save records incrementally to avoid data loss."""
        if not self.records:
            return

        # Check if we should save
        if not force and len(self.records) < self.save_every:
            return

        if not self.output_path:
            return

        save_start = time.time()
        df = self.to_dataframe()
        num_records = len(df)

        logger.info(f"Incremental save #{self.save_count + 1}: saving {num_records:,} records...")
        log_resource_usage("Before save: ")

        try:
            if self.output_format == 'parquet':
                # For parquet, append to existing file if it exists
                if self.save_count == 0:
                    # First save - create new file
                    logger.debug(f"Creating new parquet file: {self.output_path}")
                    df.to_parquet(self.output_path, index=False)
                else:
                    # Append to existing file
                    logger.debug(f"Reading existing parquet file: {self.output_path}")
                    existing_df = pd.read_parquet(self.output_path)
                    logger.debug(f"Merging {len(existing_df):,} existing + {len(df):,} new records")
                    combined_df = pd.concat([existing_df, df], ignore_index=True)
                    df.to_parquet(self.output_path, index=False)
                    logger.debug(f"Wrote {len(combined_df):,} total records")
            else:
                # For CSV, append after first write
                mode = 'w' if self.save_count == 0 else 'a'
                header = self.save_count == 0
                logger.debug(f"Writing to CSV (mode={mode}, header={header})")
                df.to_csv(self.output_path, mode=mode, header=header, index=False)

            self.total_saved += num_records
            self.save_count += 1

            save_elapsed = time.time() - save_start
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
            print(f"  [{timestamp}] Incremental save #{self.save_count}: {num_records:,} records (total: {self.total_saved:,}) in {save_elapsed:.1f}s")
            logger.info(f"Save completed in {save_elapsed:.1f}s")

            # Clear records to free memory
            self.records = []
            log_resource_usage("After save and clear: ")

        except Exception as e:
            logger.error(f"Failed to save incrementally: {e}")
            logger.debug(traceback.format_exc())
            print(f"  ERROR: Failed to save incrementally: {e}", file=sys.stderr)
            # Don't clear records if save failed

    def check_and_save_incremental(self):
        """Check if we need to save incrementally based on record count."""
        if len(self.records) >= self.save_every:
            self.save_incremental(force=True)


def find_tarballs(directory: Path) -> List[Path]:
    """Find all .tar.gz files in directory."""
    tarballs = sorted(directory.glob('*.tar.gz'))
    return tarballs


def main():
    parser = argparse.ArgumentParser(
        description='Extract metadata from XML files in tar.gz archives (streaming mode - no intermediate files).',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s /path/to/tarballs/
  %(prog)s --format parquet -o output.parquet /path/to/tarballs/
  %(prog)s --limit 5 /path/to/tarballs/
  %(prog)s --pattern "oa_comm_xml.incr.2025-07-*" /path/to/tarballs/
  %(prog)s --save-every 100000 /path/to/tarballs/  # Save every 100k records
        """
    )

    parser.add_argument(
        'tar_directory',
        type=str,
        help='Directory containing .tar.gz archives'
    )

    parser.add_argument(
        '-o', '--output',
        type=str,
        default=None,
        help='Output file path (default: streaming_metadata.csv or .parquet based on format)'
    )

    parser.add_argument(
        '-f', '--format',
        choices=['csv', 'parquet'],
        default='csv',
        help='Output format (default: csv)'
    )

    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Limit number of tar.gz files to process (for testing)'
    )

    parser.add_argument(
        '--pattern',
        type=str,
        default='*.tar.gz',
        help='Glob pattern for tar.gz files (default: *.tar.gz)'
    )

    parser.add_argument(
        '--save-every',
        type=int,
        default=250000,
        help='Save output incrementally every N records to prevent data loss (default: 250000)'
    )

    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level (default: INFO)'
    )

    parser.add_argument(
        '--log-file',
        type=str,
        default=None,
        help='Log file path (default: console only)'
    )

    args = parser.parse_args()

    # Setup logging first
    setup_logging(log_level=args.log_level, log_file=args.log_file)
    logger.info("="*70)
    logger.info("XML Metadata Extractor - Streaming Mode")
    logger.info("="*70)

    tar_dir = Path(args.tar_directory)
    logger.info(f"Processing directory: {tar_dir}")

    if not tar_dir.exists() or not tar_dir.is_dir():
        logger.error(f"Directory does not exist: {tar_dir}")
        return 1

    # Determine output path
    if args.output:
        output_path = Path(args.output)
    else:
        extension = 'csv' if args.format == 'csv' else 'parquet'
        output_path = Path(f'streaming_metadata.{extension}')

    logger.info(f"Output file: {output_path}")
    logger.info(f"Output format: {args.format}")

    # Find all tar.gz files
    logger.info(f"Searching for files matching pattern: {args.pattern}")
    tarballs = sorted(tar_dir.glob(args.pattern))

    if not tarballs:
        logger.error(f"No tar.gz files found matching pattern '{args.pattern}' in {tar_dir}")
        return 1

    logger.info(f"Found {len(tarballs)} tar.gz file(s)")

    if args.limit:
        logger.info(f"Limiting to first {args.limit} files")
        tarballs = tarballs[:args.limit]

    logger.info(f"Incremental save: every {args.save_every:,} records")

    print(f"Found {len(tarballs)} tar.gz file(s) to process")
    print(f"Output: {output_path} ({args.format} format)")
    print(f"Incremental save: every {args.save_every:,} records")
    print(f"Log level: {args.log_level}")
    print("=" * 70)

    log_resource_usage("Initial: ")

    # Create extractor with incremental save settings
    extractor = StreamingXMLMetadataExtractor(
        output_path=output_path,
        output_format=args.format,
        save_every=args.save_every
    )

    # Process all tarballs
    start_time = time.time()
    total_files = 0
    logger.info(f"Starting processing of {len(tarballs)} tar.gz files")

    for i, tarball in enumerate(tarballs, 1):
        print(f"\n[{i}/{len(tarballs)}] Processing: {tarball.name}")
        logger.info(f"[{i}/{len(tarballs)}] Starting tarball: {tarball.name}")
        tarball_start = time.time()

        count = extractor.process_tarball(tarball)
        total_files += count

        tarball_elapsed = time.time() - tarball_start
        rate = count / tarball_elapsed if tarball_elapsed > 0 else 0

        if count > 0:
            print(f"  Extracted {count} XML files in {tarball_elapsed:.2f}s ({rate:.1f} files/sec)")
            logger.info(f"Tarball complete: {count:,} files in {tarball_elapsed:.2f}s ({rate:.1f} files/sec)")
        else:
            print(f"  No XML files found")
            logger.warning(f"No XML files found in {tarball.name}")

        # Log progress
        progress_pct = 100 * i / len(tarballs)
        logger.info(f"Overall progress: {i}/{len(tarballs)} ({progress_pct:.1f}%)")

    elapsed = time.time() - start_time
    logger.info(f"Completed all tarballs in {elapsed:.2f}s ({elapsed/60:.2f} minutes)")
    logger.info(f"Total files processed: {total_files:,}")

    # Save any remaining records
    print("\n" + "=" * 70)
    print("FINAL SAVE")
    print("=" * 70)
    logger.info("Performing final save...")

    if extractor.records:
        # Save remaining records
        extractor.save_incremental(force=True)
        print(f"Saved final batch of records")
        logger.info(f"Final save complete")

    if extractor.total_saved == 0:
        logger.error("No records extracted!")
        print("\nNo records extracted. Exiting.", file=sys.stderr)
        return 1

    print(f"Total records saved: {extractor.total_saved:,}")
    print(f"Number of incremental saves: {extractor.save_count}")
    logger.info(f"Total records saved: {extractor.total_saved:,}")
    logger.info(f"Number of incremental saves: {extractor.save_count}")

    # Print summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    output_size_mb = output_path.stat().st_size / 1024 / 1024
    avg_rate = total_files / elapsed if elapsed > 0 else 0

    print(f"  Tar.gz files processed: {len(tarballs)}")
    print(f"  Total XML files extracted: {total_files:,}")
    print(f"  Total processing time: {elapsed:.2f} seconds ({elapsed/60:.2f} minutes)")
    print(f"  Average rate: {avg_rate:.1f} files/second")
    print(f"  Output format: {args.format}")
    print(f"  Output file: {output_path}")
    print(f"  Output size: {output_size_mb:.2f} MB")

    logger.info("="*70)
    logger.info("FINAL SUMMARY")
    logger.info("="*70)
    logger.info(f"Tar.gz files processed: {len(tarballs)}")
    logger.info(f"Total XML files extracted: {total_files:,}")
    logger.info(f"Total records saved: {extractor.total_saved:,}")
    logger.info(f"Processing time: {elapsed:.2f}s ({elapsed/60:.2f} minutes)")
    logger.info(f"Average rate: {avg_rate:.1f} files/second")
    logger.info(f"Output file: {output_path} ({output_size_mb:.2f} MB)")

    # Final resource usage
    final_mem, final_cpu = log_resource_usage("Final: ")
    logger.info(f"Peak memory usage: {final_mem:.1f} MB")
    logger.info("Processing complete!")

    return 0


if __name__ == '__main__':
    sys.exit(main())
