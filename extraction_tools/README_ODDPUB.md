# PMCOA Processing with oddpub

This script processes PMC Open Access XML files with the oddpub R package to detect open data and open code statements, working directly with tar.gz archives without requiring decompression to disk.

## Overview

**Script**: `process_pmcoa_with_oddpub.py`

**What it does**:
1. Streams through PMC tar.gz archives (no decompression to disk)
2. Extracts body text from XML files
3. Processes text in batches with oddpub R package
4. Detects open data and open code availability statements
5. Outputs results to parquet files

**Output location**: `pmcoaXMLs/oddpub_out/` (configurable)

## Requirements

### Python Dependencies
```bash
pip install pandas pyarrow
```

### R Dependencies

The oddpub R package must be installed and available:

```r
# Install oddpub
devtools::install_github("quest-bih/oddpub")

# Install supporting packages
install.packages(c("future", "furrr", "progressr"))
```

## Usage

### Basic Usage

Process all tar.gz files in the raw_download directory:

```bash
cd ~/claude/osm-2025-12-poster-incf/extraction_tools
python process_pmcoa_with_oddpub.py ~/claude/pmcoaXMLs/raw_download/
```

### Testing with Limited Files

Process only the first 2 tar.gz files with smaller batches:

```bash
cd ~/claude/osm-2025-12-poster-incf/extraction_tools
python process_pmcoa_with_oddpub.py \
  --limit 2 \
  --batch-size 100 \
  ~/claude/pmcoaXMLs/raw_download/
```

### Pattern Filtering

Process only baseline files:

```bash
cd ~/claude/osm-2025-12-poster-incf/extraction_tools
python process_pmcoa_with_oddpub.py \
  --pattern "*baseline*" \
  ~/claude/pmcoaXMLs/raw_download/
```

### Custom Output Directory

```bash
python process_pmcoa_with_oddpub.py \
  --output-dir /path/to/output \
  ~/claude/pmcoaXMLs/raw_download/
```

### Debug Logging

```bash
python process_pmcoa_with_oddpub.py \
  --log-level DEBUG \
  ~/claude/pmcoaXMLs/raw_download/
```

## Command-Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `tar_directory` | (required) | Directory containing .tar.gz archives |
| `-o, --output-dir` | `oddpub_out` | Output directory for results |
| `--batch-size` | 500 | Number of files to process per oddpub batch |
| `--limit` | None | Limit number of tar.gz files (for testing) |
| `--pattern` | `*.tar.gz` | Glob pattern for tar.gz files |
| `--oddpub-path` | `../../oddpub` | Path to oddpub R package directory |
| `--log-level` | INFO | Logging level (DEBUG, INFO, WARNING, ERROR) |

## How It Works

### Processing Pipeline

1. **Streaming Extraction**
   - Opens tar.gz archives without extracting to disk
   - Reads XML files directly from archive into memory
   - Extracts body text using XML parsing

2. **Batch Processing**
   - Groups files into batches (default: 500 files)
   - Writes text files to temporary directory
   - Calls oddpub R package on batch
   - Cleans up temporary files after processing

3. **oddpub Analysis**
   - R package detects open data/code statements
   - Uses keyword matching and pattern detection
   - Extracts Data Availability Statements (DAS)
   - Returns detection flags and categories

4. **Result Collection**
   - Combines results from all batches
   - Adds PMID and PMCID identifiers
   - Saves final results to parquet file

### Output Schema

The output parquet file (`oddpub_results_all.parquet`) contains:

**Core Detection Columns**:
- `is_open_data` - Boolean: Open data detected
- `is_open_code` - Boolean: Open code detected
- `is_open_data_das` - Boolean: Open data in Data Availability Statement
- `is_open_code_cas` - Boolean: Open code in Code Availability Statement
- `is_reuse` - Boolean: Data re-use detected

**Category Columns**:
- `open_data_category` - Categories of open data detected
- Various keyword match columns (see oddpub documentation)

**Metadata Columns**:
- `article` - Original filename
- `pmid` - PubMed ID
- `pmcid` - PubMed Central ID

**Sentence Extraction** (if enabled):
- `detected_sentences` - Sentences where statements were detected

See the [oddpub documentation](https://github.com/quest-bih/oddpub) for full output schema details.

## Performance Characteristics

### Processing Speed

- **XML extraction**: ~1,000 files/second (streaming from tar.gz)
- **oddpub analysis**: ~50-100 files/second (depends on text length and R parallelization)
- **Batch size impact**: Larger batches reduce R overhead but increase memory usage

### Memory Requirements

- **Python process**: ~200-500 MB
- **R process**: ~500 MB - 2 GB (depends on batch size and parallelization)
- **Temporary disk**: ~10-50 MB per batch (cleaned up automatically)

### Recommended Settings

For typical workloads:
- Batch size: 500 files (balances speed and memory)
- R workers: 4 (default in script, uses `future::plan(multisession, workers = 4)`)

For limited memory:
- Reduce batch size to 100-200 files
- Reduce R workers to 2

For maximum speed (with sufficient RAM):
- Increase batch size to 1000 files
- Increase R workers to 8

## Output Files

### During Processing

Temporary files are created in system temp directory:
- `oddpub_batch_N_XXXXX/` - Temporary text files for each batch
- Automatically cleaned up after batch processing

### Final Output

- `oddpub_out/oddpub_results_all.parquet` - Combined results from all batches

## Example Output

```
Found 268 tar.gz file(s) to process
Output directory: oddpub_out
Batch size: 500
oddpub path: ../oddpub
======================================================================

[1/268] Processing: oa_comm_xml.PMC000xxxxxx.baseline.2024-12-16.tar.gz
  Processed 562 XML files from oa_comm_xml.PMC000xxxxxx.baseline.2024-12-16.tar.gz

[2/268] Processing: oa_comm_xml.PMC001xxxxxx.baseline.2024-12-16.tar.gz
  Processed 487 XML files from oa_comm_xml.PMC001xxxxxx.baseline.2024-12-16.tar.gz

...

======================================================================
SUMMARY
======================================================================
  Tar.gz files processed: 268
  Total XML files: 150,000
  Total results: 148,532
  Open data detected: 45,231
  Open code detected: 12,456
  Processing time: 3245.67 seconds (54.09 minutes)
  Output file: oddpub_out/oddpub_results_all.parquet
```

## Troubleshooting

### Error: "oddpub path does not exist"

Make sure the oddpub R package is installed and the path is correct:

```bash
python process_pmcoa_with_oddpub.py \
  --oddpub-path /path/to/oddpub \
  ~/claude/pmcoaXMLs/raw_download/
```

### Error: "R script failed"

Check R dependencies are installed:

```r
library(oddpub)   # Should load without errors
library(future)
library(furrr)
library(progressr)
```

### Processing is slow

- Reduce batch size: `--batch-size 200`
- Check R parallelization is working (should use 4 cores)
- Monitor memory usage - if swapping, reduce batch size

### Out of memory

- Reduce batch size: `--batch-size 100`
- Process fewer tar.gz files at once: `--limit 50`

## Comparison to Other Tools

### vs. rtransparent

- **oddpub**: Focuses specifically on open data/code detection with sophisticated DAS extraction
- **rtransparent**: Broader transparency indicators (COI, funding, registration, data/code)

**When to use oddpub**:
- Detailed open data/code analysis
- Need sentence extraction for detected statements
- Focus on Data Availability Statements

**When to use rtransparent**:
- Comprehensive transparency analysis
- COI and funding analysis
- Trial registration detection

### Processing Workflow

For comprehensive analysis, use both:

1. **rtransparent** → Pattern analysis for all transparency indicators
2. **oddpub** → Detailed open data/code analysis with sentence extraction
3. **Merge results** → Combine using PMID/PMCID

## Notes

- The script uses temporary directories that are automatically cleaned up
- No decompression of tar.gz archives to disk (streaming only)
- Results are saved incrementally by batch (safer for long runs)
- PMID and PMCID are extracted from XML and added to results for easy merging

## Related Documentation

- [oddpub GitHub](https://github.com/quest-bih/oddpub)
- [oddpub Publication](https://doi.org/10.5334/dsj-2020-042)
- PMC XML Structure: [JATS DTD](https://jats.nlm.nih.gov/)
