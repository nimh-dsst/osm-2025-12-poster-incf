#!/usr/bin/env python3
"""
XML Metadata Extractor for Research Transparency Analysis

Extracts metadata from JATS XML files into a structured format with 120 columns.
Copies basic metadata from XML structure while leaving sophisticated analysis columns blank.

Usage:
    python extract_xml_metadata.py [options] <paths...>

Examples:
    python extract_xml_metadata.py batch_0001/
    python extract_xml_metadata.py file1.xml file2.xml file3.xml
    python extract_xml_metadata.py --format parquet --output results.parquet batch_*/
"""

import argparse
import os
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Optional, Union
import pandas as pd
from datetime import datetime


# Define all 120 columns in order
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

# Columns that should be copied from XML (not left blank)
COPIED_COLUMNS = {
    'pmid', 'pmcid_pmc', 'pmcid_uid', 'doi', 'filename', 'journal', 'publisher',
    'affiliation_institution', 'affiliation_country', 'year_epub', 'year_ppub',
    'coi_text', 'fund_text', 'fund_pmc_institute', 'fund_pmc_source',
    'fund_pmc_anysource', 'register_text', 'type', 'file_size', 'chars_in_body'
}


class XMLMetadataExtractor:
    """Extracts metadata from JATS XML files."""

    # XML namespaces commonly used in JATS
    NAMESPACES = {
        'xlink': 'http://www.w3.org/1999/xlink',
        'mml': 'http://www.w3.org/1998/Math/MathML',
        'ali': 'http://www.niso.org/schemas/ali/1.0/'
    }

    def __init__(self):
        self.records = []

    def extract_text(self, element: Optional[ET.Element]) -> str:
        """Extract all text content from an element and its children."""
        if element is None:
            return ''

        # Get text from element and all children, joined together
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

    def find_element(self, root: ET.Element, path: str) -> Optional[ET.Element]:
        """Find element by path, handling namespaces."""
        try:
            elem = root.find(path)
            if elem is not None:
                return elem

            # Try with namespace prefixes
            for prefix, uri in self.NAMESPACES.items():
                namespaced_path = path.replace('/', f'/{{{uri}}}')
                elem = root.find(namespaced_path)
                if elem is not None:
                    return elem

            return None
        except:
            return None

    def find_all_elements(self, root: ET.Element, path: str) -> List[ET.Element]:
        """Find all elements matching path."""
        try:
            return root.findall(path)
        except:
            return []

    def extract_article_ids(self, root: ET.Element) -> Dict[str, str]:
        """Extract article IDs (PMID, PMCID, DOI)."""
        ids = {'pmid': '', 'pmcid_pmc': '', 'pmcid_uid': '', 'doi': ''}

        # Look for article-id elements in article-meta
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

        # Journal title
        journal_title = root.find(".//journal-meta/journal-title-group/journal-title")
        if journal_title is not None:
            info['journal'] = self.extract_text(journal_title)

        # Publisher
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

        # Get all affiliation elements
        aff_elements = root.findall(".//contrib-group//aff")

        institutions = []
        countries = []

        for aff in aff_elements:
            # Look for institution
            inst = aff.find(".//institution")
            if inst is not None:
                inst_text = self.extract_text(inst)
                if inst_text:
                    institutions.append(inst_text)

            # Look for country
            country = aff.find(".//country")
            if country is not None:
                country_text = self.extract_text(country)
                if country_text:
                    countries.append(country_text)

        # Remove duplicates while preserving order
        if institutions:
            affiliations['affiliation_institution'] = '; '.join(dict.fromkeys(institutions))
        if countries:
            affiliations['affiliation_country'] = '; '.join(dict.fromkeys(countries))

        return affiliations

    def extract_pub_dates(self, root: ET.Element) -> Dict[str, str]:
        """Extract publication dates."""
        dates = {'year_epub': '', 'year_ppub': ''}

        # Look for pub-date elements
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
        # Look for fn with fn-type="COI-statement" or similar
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

        # Look for funding statement in fn-group
        fn_elements = root.findall(".//fn[@fn-type]")
        for fn in fn_elements:
            fn_type = fn.get('fn-type', '').lower()
            if 'financial' in fn_type or 'funding' in fn_type:
                funding['fund_text'] = self.extract_text(fn)
                break

        # Look for structured funding-group
        funding_group = root.find(".//funding-group")
        if funding_group is not None:
            # Extract institution names
            institutions = []
            institution_elems = funding_group.findall(".//institution")
            for inst in institution_elems:
                inst_text = self.extract_text(inst)
                if inst_text:
                    institutions.append(inst_text)

            if institutions:
                funding['fund_pmc_institute'] = '; '.join(institutions)

            # Extract full funding source (institution + ID)
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
        # Look in multiple places for registration info

        # Check custom-meta for registration
        custom_metas = root.findall(".//custom-meta")
        for meta in custom_metas:
            meta_name = meta.find("meta-name")
            if meta_name is not None and 'regist' in self.extract_text(meta_name).lower():
                meta_value = meta.find("meta-value")
                if meta_value is not None:
                    return self.extract_text(meta_value)

        # Check for ext-link with registration-related text
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

    def process_xml_file(self, filepath: Path) -> Dict[str, Union[str, int]]:
        """Process a single XML file and extract metadata."""
        # Initialize record with all columns as empty strings/None
        record = {col: None for col in ALL_COLUMNS}

        try:
            # Get file size
            record['file_size'] = os.path.getsize(filepath)
            record['filename'] = str(filepath)

            # Parse XML
            tree = ET.parse(filepath)
            root = tree.getroot()

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

        except Exception as e:
            print(f"Error processing {filepath}: {e}", file=sys.stderr)
            record['filename'] = str(filepath)

        return record

    def process_path(self, path: Path):
        """Process a file or directory."""
        if path.is_file() and path.suffix.lower() == '.xml':
            print(f"Processing: {path}")
            record = self.process_xml_file(path)
            self.records.append(record)
        elif path.is_dir():
            # Recursively find all XML files
            xml_files = sorted(path.rglob('*.xml'))
            print(f"Found {len(xml_files)} XML files in {path}")
            for xml_file in xml_files:
                print(f"Processing: {xml_file}")
                record = self.process_xml_file(xml_file)
                self.records.append(record)
        else:
            print(f"Skipping non-XML file: {path}", file=sys.stderr)

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


def main():
    parser = argparse.ArgumentParser(
        description='Extract metadata from JATS XML files for research transparency analysis.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s batch_0001/
  %(prog)s file1.xml file2.xml file3.xml
  %(prog)s --format parquet --output results.parquet batch_*/
  %(prog)s -o output.csv batch_0001/ batch_0002/
        """
    )

    parser.add_argument(
        'paths',
        nargs='+',
        help='XML files or directories to process (directories are searched recursively)'
    )

    parser.add_argument(
        '-o', '--output',
        type=str,
        default=None,
        help='Output file path (default: extracted_metadata.csv or .parquet based on format)'
    )

    parser.add_argument(
        '-f', '--format',
        choices=['csv', 'parquet'],
        default='csv',
        help='Output format (default: csv)'
    )

    args = parser.parse_args()

    # Determine output path
    if args.output:
        output_path = Path(args.output)
    else:
        extension = 'csv' if args.format == 'csv' else 'parquet'
        output_path = Path(f'extracted_metadata.{extension}')

    # Create extractor
    extractor = XMLMetadataExtractor()

    # Process all paths
    for path_str in args.paths:
        path = Path(path_str)
        if not path.exists():
            print(f"Warning: Path does not exist: {path}", file=sys.stderr)
            continue

        extractor.process_path(path)

    # Save results
    if not extractor.records:
        print("No records extracted. Exiting.", file=sys.stderr)
        return 1

    if args.format == 'csv':
        extractor.save_csv(output_path)
    else:
        extractor.save_parquet(output_path)

    # Print summary
    print(f"\nSummary:")
    print(f"  Total files processed: {len(extractor.records)}")
    print(f"  Output format: {args.format}")
    print(f"  Output file: {output_path}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
