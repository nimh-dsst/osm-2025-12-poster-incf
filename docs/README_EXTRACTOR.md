# XML Metadata Extractor for Research Transparency Analysis
**Document created:** 2025-11-06

A Python tool to extract metadata from JATS XML files (PubMed Central format) into a structured format suitable for research transparency analysis.

## Overview

This extractor processes XML files containing scientific articles and extracts basic metadata into a 122-column format:
- **18 "copied" columns**: Directly extracted from XML structure (identifiers, journal info, dates, etc.)
- **102 "sophisticated analysis" columns**: Left blank for later NLP/pattern matching analysis
- **2 new metric columns**: `file_size` and `chars_in_body`

## Installation

Requirements:
```bash
pip install pandas pyarrow
```

## Usage

### Basic Usage

Process a single XML file:
```bash
python extract_xml_metadata.py article.xml
```

Process multiple XML files:
```bash
python extract_xml_metadata.py file1.xml file2.xml file3.xml
```

Process a directory (recursively searches for all .xml files):
```bash
python extract_xml_metadata.py batch_0001/
```

Process multiple directories:
```bash
python extract_xml_metadata.py batch_0001/ batch_0002/ batch_0003/
```

### Output Formats

**CSV format (default):**
```bash
python extract_xml_metadata.py batch_0001/
# Creates: extracted_metadata.csv
```

**Parquet format:**
```bash
python extract_xml_metadata.py --format parquet batch_0001/
# Creates: extracted_metadata.parquet
```

**Custom output filename:**
```bash
python extract_xml_metadata.py -o my_results.csv batch_0001/
python extract_xml_metadata.py -f parquet -o results.parquet batch_0001/
```

### Command-Line Options

```
positional arguments:
  paths                 XML files or directories to process (directories are
                        searched recursively)

options:
  -h, --help            show this help message and exit
  -o OUTPUT, --output OUTPUT
                        Output file path (default: extracted_metadata.csv or
                        .parquet based on format)
  -f {csv,parquet}, --format {csv,parquet}
                        Output format (default: csv)
```

## Extracted Columns

### Copied Columns (18)
These are extracted directly from the XML structure:

| Column | Description | XML Source |
|--------|-------------|------------|
| `pmid` | PubMed ID | `<article-id pub-id-type="pmid">` |
| `pmcid_pmc` | PubMed Central ID | `<article-id pub-id-type="pmc">` |
| `pmcid_uid` | PMC UID | `<article-id pub-id-type="pmcid">` |
| `doi` | Digital Object Identifier | `<article-id pub-id-type="doi">` |
| `filename` | Source XML file path | File path |
| `journal` | Journal name | `<journal-title>` |
| `publisher` | Publisher name and location | `<publisher-name>`, `<publisher-loc>` |
| `affiliation_institution` | Author institutions | `<aff><institution>` |
| `affiliation_country` | Author countries | `<aff><country>` |
| `year_epub` | Electronic publication year | `<pub-date pub-type="epub"><year>` |
| `year_ppub` | Print publication year | `<pub-date pub-type="ppub"><year>` |
| `coi_text` | Conflict of interest statement | `<fn fn-type="COI-statement">` |
| `fund_text` | Funding statement | `<fn fn-type="financial-disclosure">` |
| `fund_pmc_institute` | Funding institutions | `<funding-group><institution>` |
| `fund_pmc_source` | Full funding source info | `<institution-wrap>` |
| `fund_pmc_anysource` | Any funding source | Combined funding fields |
| `register_text` | Trial registration info | `<custom-meta>`, `<ext-link>` |
| `type` | Article type | `<article article-type="...">` |
| `file_size` | File size in bytes | OS file stat |
| `chars_in_body` | Character count in body | `<body>` text length |

### Sophisticated Analysis Columns (102)
These columns are left empty (None/null) for later analysis:

**Conflict of Interest (29 columns):**
- Detection: `is_coi_pred`, `is_relevant_coi`, `is_explicit_coi`, etc.
- Pattern matches: `commercial_1`, `consultant_1`, `grants_1`, `fees_1`, `founder_1`, etc.

**Funding (45 columns):**
- Detection: `is_fund_pred`, `is_relevant_fund`, `is_explicit_fund`, etc.
- Pattern matches: `support_1` through `support_10`, `received_1/2`, `fund_1/2/3`, etc.

**Registration (23 columns):**
- Detection: `is_register_pred`, `is_NCT`, `is_method`, etc.
- Pattern matches: `prospero_1`, `registered_1-5`, `registration_1-4`, etc.

**Open Science (5 columns):**
- `is_success`, `is_open_data`, `is_open_code`, `is_relevant_data`, `is_relevant_code`

## Examples

### Example 1: Process all batches
```bash
python extract_xml_metadata.py batch_*/ -o all_batches.csv
```

### Example 2: Generate parquet for large datasets
```bash
python extract_xml_metadata.py --format parquet batch_0001/ batch_0002/
```

### Example 3: Process specific files
```bash
python extract_xml_metadata.py \
    batch_0001/PMC11529060.xml \
    batch_0001/PMC11529089.xml \
    -o sample.csv
```

## Output Schema

The output contains 122 columns in total:
- 18 columns with extracted metadata
- 102 columns with null/empty values (for sophisticated analysis)
- 2 new metric columns (`file_size`, `chars_in_body`)

## Error Handling

- Invalid XML files are skipped with error messages printed to stderr
- Processing continues even if individual files fail
- Missing or inaccessible paths generate warnings

## Performance

Processing time depends on:
- Number of files
- File sizes
- XML complexity

Typical performance: ~50 files in 1-2 seconds

## Notes

- Directories are searched recursively for `.xml` files
- Only files with `.xml` extension are processed
- Output format is inferred from file extension if not specified
- CSV format is more human-readable; Parquet is more efficient for large datasets
- All 120 original columns are preserved in the same order for compatibility

## Testing

Compare with original data:
```bash
# Extract metadata
python extract_xml_metadata.py batch_0001/ -f parquet -o extracted.parquet

# Compare in Python
python -c "
import pandas as pd
original = pd.read_parquet('out.001.parquet')
extracted = pd.read_parquet('extracted.parquet')
print(f'Original: {len(original)} records')
print(f'Extracted: {len(extracted)} records')
print(f'Missing in original: {set(extracted[\"pmcid_pmc\"]) - set(original[\"pmcid_pmc\"])}')
"
```

## See Also

- `data_dictionary.csv` - Complete column definitions
- Original analysis pipeline documentation
