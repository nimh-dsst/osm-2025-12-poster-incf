# create_compact_rtrans.py

**Purpose:** Create compact, analysis-ready datasets from rtrans parquet files by adding metadata fields, funder matching, and filtering to essential columns.

**Location:** `create_compact_rtrans.py` (project root)

## Quick Start

```bash
# First run (builds cache, ~20 minutes)
python create_compact_rtrans.py \
    --input-dir ~/claude/pmcoaXMLs/rtrans_out_full_parquets \
    --metadata-dir ~/claude/pmcoaXMLs/extracted_metadata_parquet \
    --output-dir ~/claude/pmcoaXMLs/compact_rtrans

# Subsequent runs (uses cache, ~8 minutes)
python create_compact_rtrans.py \
    --input-dir ~/claude/pmcoaXMLs/rtrans_out_full_parquets \
    --metadata-dir ~/claude/pmcoaXMLs/extracted_metadata_parquet \
    --output-dir ~/claude/pmcoaXMLs/compact_rtrans

# Resume interrupted processing
python create_compact_rtrans.py ... --resume

# Enable verbose logging for debugging
python create_compact_rtrans.py ... --verbose
```

## Output Schema

**142 columns total:**
- 109 short fields from rtrans (max_length ≤ 30)
- 2 metadata fields: `file_size`, `chars_in_body`
- 31 funder binary columns: `funder_nih`, `funder_ec`, `funder_nsfc`, etc.

## Features

### Metadata Enrichment
- Adds `file_size` and `chars_in_body` from extracted_metadata
- PMCID normalization for consistent matching
- 184 MB cache file for fast subsequent runs

### Funder Matching
- Vectorized pattern matching (43x faster than row-by-row)
- Searches 4 funding columns: `fund_text`, `fund_pmc_institute`, `fund_pmc_source`, `fund_pmc_anysource`
- Case-insensitive name matching + case-sensitive acronym matching
- 31 binary columns for major biomedical research funders

### Field Filtering
- Keeps only fields with max_length ≤ 30 (configurable)
- Excludes long text fields: `coi_text`, `fund_text`, `affiliation_institution`, etc.
- All boolean/sophisticated analysis fields preserved

### Performance
- Processes ~5.5 files/second
- Vectorized operations for speed
- Memory efficient: ~500 MB
- Full dataset (1,647 files): ~8-20 minutes

## Command-Line Options

```
--input-dir PATH          rtrans parquet files directory (required)
--metadata-dir PATH       extracted_metadata parquet files (required)
--output-dir PATH         output directory (required)
--max-field-length N      maximum field length to include (default: 30)
--funder-db PATH          funder database CSV (default: funder_analysis/biomedical_research_funders.csv)
--data-dict PATH          data dictionary CSV (default: docs/data_dictionary.csv)
--limit N                 process only first N files (for testing)
--overwrite               overwrite existing output files
--resume                  skip files that already exist
--cache-file PATH         metadata cache file (default: metadata_lookup_cache.pkl)
--rebuild-cache           force rebuild of metadata cache
--verbose, -v             enable DEBUG level logging
```

## Logging

**INFO level (default):** Clean output with milestones every 100 files, progress percentage, processing rate, and ETA.

**DEBUG level (--verbose):** Detailed per-file logging for troubleshooting.

Example output:
```
======================================================================
CREATE COMPACT RTRANS DATASET
======================================================================

✓ Loaded 6,470,590 PMCIDs from cache
Found 1,647 rtrans files to process

Starting file processing...
✓ First file processed successfully:
  120 columns → 142 columns (31 funders, 2 metadata)
  Metadata matched: 1643/3855 (42.6%)

Progress: 100/1647 (6%) | 5.2 files/sec | ETA: 5m
Progress: 200/1647 (12%) | 5.3 files/sec | ETA: 4m
...
Progress: 1647/1647 (100%) - Complete!

======================================================================
✓ PROCESSING COMPLETE
======================================================================
Runtime: 8m 15s
Files: 1,647 processed, 0 skipped
Records: 6,589,428 total
Metadata: 5,546,231/6,589,428 matched (84.2%)

Output schema:
  142 columns per file
  └─ 109 original short fields
  └─ 31 funder binary columns
  └─ 2 metadata fields (file_size, chars_in_body)

Dataset ready for analysis!
```

## Algorithm Details

### PMCID Normalization
```python
# Strips whitespace, ensures "PMC" prefix, removes non-numeric chars
"PMC1234567" ← "pmc 1234567", "1234567", "PMC1234567"
```

### Funder Matching (Vectorized)
```python
# For each funder:
1. Combine all funding text columns
2. Case-insensitive search for organization name
3. Case-sensitive search for acronym
4. Binary result: 1 if match, 0 otherwise
```

### Field Filtering
```python
# Processing order (critical):
1. Filter to short fields (max_length ≤ 30)
2. Add metadata fields (file_size, chars_in_body)
3. Add funder columns (funder_nih, funder_ec, etc.)
# This ensures new columns aren't filtered out
```

## Performance Metrics

Based on test with 1,647 files:
- **First run:** 11 min (cache build) + 8 min (processing) = 19 min total
- **Cached runs:** 5 sec (cache load) + 8 min (processing) = 8 min total
- **Processing rate:** ~5.5 files/second, ~0.18 sec/file
- **Memory usage:** ~500 MB peak
- **Output size:** ~450 MB total (vs 1.8 GB rtrans source)

## Use Cases

1. **INCF Poster Analysis:** Compact dataset for analyzing transparency indicators and funder distributions
2. **Exploratory Analysis:** Smaller files easier to load in pandas/R
3. **Subset Analysis:** Filter to specific funders or time periods
4. **Reproducibility:** Can regenerate from rtrans + metadata sources

## Notes

- Original rtrans files remain unchanged (read-only)
- Compact files can be regenerated at any time
- Cache file (184 MB) speeds up subsequent runs significantly
- Uses data_dictionary.csv to identify short fields
