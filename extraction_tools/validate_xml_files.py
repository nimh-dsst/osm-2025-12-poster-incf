#!/usr/bin/env python3
"""
Validate PMC XML files before oddpub processing.

Pre-checks to identify potentially problematic files:
1. File exists and is readable
2. File is not empty
3. File size is within reasonable bounds (not too big, not too small)
4. XML is well-formed and parseable
5. Contains expected PMC elements (article-id, body)
6. Body text extraction produces content

Output: CSV file listing all files with validation status and issues found.
"""

import argparse
import csv
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

from lxml import etree

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Validation thresholds
MIN_FILE_SIZE = 500  # Bytes - minimum for valid PMC XML
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100 MB - unusually large
MIN_BODY_CHARS = 100  # Minimum body text characters
MAX_BODY_CHARS = 10 * 1024 * 1024  # 10 MB body text - likely problematic


def validate_xml_file(xml_path: Path) -> Dict:
    """
    Validate a single XML file.

    Returns dict with:
        - path: file path
        - valid: True/False
        - issues: list of issue strings
        - file_size: size in bytes
        - body_chars: character count in body
        - pmcid: extracted PMCID
        - pmid: extracted PMID
    """
    result = {
        'path': str(xml_path),
        'valid': True,
        'issues': [],
        'file_size': 0,
        'body_chars': 0,
        'pmcid': '',
        'pmid': '',
    }

    # Check 1: File exists and is readable
    if not xml_path.exists():
        result['valid'] = False
        result['issues'].append('FILE_NOT_FOUND')
        return result

    if not xml_path.is_file():
        result['valid'] = False
        result['issues'].append('NOT_A_FILE')
        return result

    try:
        # Check 2: File size
        result['file_size'] = xml_path.stat().st_size

        if result['file_size'] == 0:
            result['valid'] = False
            result['issues'].append('EMPTY_FILE')
            return result

        if result['file_size'] < MIN_FILE_SIZE:
            result['issues'].append(f'TOO_SMALL:{result["file_size"]}')

        if result['file_size'] > MAX_FILE_SIZE:
            result['issues'].append(f'TOO_LARGE:{result["file_size"]}')

        # Check 3: XML parsing
        try:
            # Use recovering parser to handle minor issues
            parser = etree.XMLParser(recover=True, encoding='utf-8')
            root = etree.parse(str(xml_path), parser=parser).getroot()
        except etree.XMLSyntaxError as e:
            result['valid'] = False
            result['issues'].append(f'XML_SYNTAX_ERROR:{str(e)[:100]}')
            return result
        except Exception as e:
            result['valid'] = False
            result['issues'].append(f'XML_PARSE_ERROR:{str(e)[:100]}')
            return result

        # Check 4: Expected elements - PMCID
        pmcid_elem = root.find('.//article-id[@pub-id-type="pmc"]')
        if pmcid_elem is not None and pmcid_elem.text:
            pmcid = pmcid_elem.text.strip()
            result['pmcid'] = f"PMC{pmcid}" if not pmcid.startswith('PMC') else pmcid
        else:
            result['issues'].append('NO_PMCID')

        # PMID
        pmid_elem = root.find('.//article-id[@pub-id-type="pmid"]')
        if pmid_elem is not None and pmid_elem.text:
            result['pmid'] = pmid_elem.text.strip()

        # Check 5: Body element exists
        body = root.find('.//body')
        if body is None:
            result['issues'].append('NO_BODY_ELEMENT')

        # Check 6: Extract body text
        body_text_parts = []
        for p in root.findall('.//body//p'):
            text = ' '.join(p.itertext()).strip()
            if text:
                body_text_parts.append(text)

        for caption in root.findall('.//fig//caption//p'):
            text = ' '.join(caption.itertext()).strip()
            if text:
                body_text_parts.append(text)

        body_text = '\n'.join(body_text_parts)
        result['body_chars'] = len(body_text)

        if result['body_chars'] == 0:
            result['issues'].append('NO_BODY_TEXT')
        elif result['body_chars'] < MIN_BODY_CHARS:
            result['issues'].append(f'SHORT_BODY:{result["body_chars"]}')
        elif result['body_chars'] > MAX_BODY_CHARS:
            result['issues'].append(f'HUGE_BODY:{result["body_chars"]}')

        # Mark as invalid only if there are critical issues
        critical_issues = ['FILE_NOT_FOUND', 'NOT_A_FILE', 'EMPTY_FILE',
                          'XML_SYNTAX_ERROR', 'XML_PARSE_ERROR', 'NO_BODY_TEXT']
        for issue in result['issues']:
            if any(issue.startswith(c) for c in critical_issues):
                result['valid'] = False
                break

    except PermissionError:
        result['valid'] = False
        result['issues'].append('PERMISSION_DENIED')
    except Exception as e:
        result['valid'] = False
        result['issues'].append(f'UNEXPECTED_ERROR:{str(e)[:100]}')

    return result


def validate_files_parallel(xml_files: List[Path], num_workers: int = None) -> List[Dict]:
    """Validate multiple files in parallel."""
    if num_workers is None:
        num_workers = min(multiprocessing.cpu_count(), 8)

    results = []
    total = len(xml_files)

    logger.info(f"Validating {total} files with {num_workers} workers")

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = {executor.submit(validate_xml_file, f): f for f in xml_files}

        for i, future in enumerate(as_completed(futures), 1):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                xml_path = futures[future]
                results.append({
                    'path': str(xml_path),
                    'valid': False,
                    'issues': [f'WORKER_ERROR:{str(e)[:100]}'],
                    'file_size': 0,
                    'body_chars': 0,
                    'pmcid': '',
                    'pmid': '',
                })

            if i % 10000 == 0:
                logger.info(f"  Validated {i}/{total} files ({i*100/total:.1f}%)")

    return results


def write_results(results: List[Dict], output_file: Path):
    """Write validation results to CSV."""
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'path', 'valid', 'issues', 'file_size', 'body_chars', 'pmcid', 'pmid'
        ])
        writer.writeheader()

        for r in results:
            row = r.copy()
            row['issues'] = ';'.join(row['issues'])
            writer.writerow(row)

    logger.info(f"Wrote {len(results)} results to {output_file}")


def print_summary(results: List[Dict]):
    """Print summary statistics."""
    total = len(results)
    valid = sum(1 for r in results if r['valid'])
    invalid = total - valid

    # Count issues
    issue_counts = {}
    for r in results:
        for issue in r['issues']:
            # Get issue type (before colon if present)
            issue_type = issue.split(':')[0]
            issue_counts[issue_type] = issue_counts.get(issue_type, 0) + 1

    print("\n" + "=" * 70)
    print("VALIDATION SUMMARY")
    print("=" * 70)
    print(f"\nTotal files:  {total:,}")
    print(f"Valid:        {valid:,} ({valid*100/total:.1f}%)")
    print(f"Invalid:      {invalid:,} ({invalid*100/total:.1f}%)")

    if issue_counts:
        print("\nIssues found:")
        for issue, count in sorted(issue_counts.items(), key=lambda x: -x[1]):
            print(f"  {issue}: {count:,}")

    # Size statistics for valid files
    valid_results = [r for r in results if r['valid']]
    if valid_results:
        sizes = [r['file_size'] for r in valid_results]
        body_chars = [r['body_chars'] for r in valid_results]

        print(f"\nFile size statistics (valid files):")
        print(f"  Min: {min(sizes):,} bytes")
        print(f"  Max: {max(sizes):,} bytes")
        print(f"  Avg: {sum(sizes)/len(sizes):,.0f} bytes")

        print(f"\nBody text statistics (valid files):")
        print(f"  Min: {min(body_chars):,} chars")
        print(f"  Max: {max(body_chars):,} chars")
        print(f"  Avg: {sum(body_chars)/len(body_chars):,.0f} chars")

    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description='Validate PMC XML files before oddpub processing'
    )
    parser.add_argument('input', nargs='?',
                       help='XML file, directory, or file list (use - for stdin)')
    parser.add_argument('--file-list', '-f',
                       help='Path to file containing XML paths (one per line)')
    parser.add_argument('--output', '-o', required=True,
                       help='Output CSV file for validation results')
    parser.add_argument('--pattern', '-p', default='*.xml',
                       help='Glob pattern for XML files (default: *.xml)')
    parser.add_argument('--max-files', '-m', type=int,
                       help='Maximum number of files to validate')
    parser.add_argument('--workers', '-w', type=int, default=8,
                       help='Number of parallel workers (default: 8)')
    parser.add_argument('--quiet', '-q', action='store_true',
                       help='Suppress progress output')

    args = parser.parse_args()

    if args.quiet:
        logger.setLevel(logging.WARNING)

    # Collect input files
    xml_files = []

    if args.file_list:
        file_list_path = Path(args.file_list)
        if not file_list_path.exists():
            logger.error(f"File list not found: {file_list_path}")
            sys.exit(1)
        with open(file_list_path) as f:
            xml_files = [Path(line.strip()) for line in f if line.strip()]
        logger.info(f"Read {len(xml_files)} paths from {file_list_path}")

    elif args.input == '-':
        # Read from stdin
        for line in sys.stdin:
            line = line.strip()
            if line:
                xml_files.append(Path(line))
        logger.info(f"Read {len(xml_files)} paths from stdin")

    elif args.input:
        input_path = Path(args.input)
        if input_path.is_file():
            xml_files = [input_path]
        elif input_path.is_dir():
            xml_files = sorted(input_path.glob(args.pattern))
            logger.info(f"Found {len(xml_files)} files matching {args.pattern}")
        else:
            logger.error(f"Input not found: {input_path}")
            sys.exit(1)

    else:
        parser.print_help()
        sys.exit(1)

    if args.max_files:
        xml_files = xml_files[:args.max_files]

    if not xml_files:
        logger.error("No XML files to validate")
        sys.exit(1)

    # Validate
    results = validate_files_parallel(xml_files, args.workers)

    # Write results
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_results(results, output_path)

    # Print summary
    print_summary(results)

    # Exit with error code if any invalid files
    invalid_count = sum(1 for r in results if not r['valid'])
    if invalid_count > 0:
        sys.exit(1)


if __name__ == '__main__':
    main()
