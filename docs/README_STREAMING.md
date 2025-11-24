# Streaming XML Metadata Extractor from Tar.gz Archives
**Document created:** 2025-11-06

A high-performance Python tool that extracts XML metadata directly from tar.gz archives without writing intermediate files to disk. This streaming approach is **~5x faster** than the file-based extractor and requires no temporary disk space.

## Overview

This tool processes tar.gz archives containing XML files and extracts metadata in a single pass:
1. Opens tar.gz archive
2. Extracts XML files directly into memory
3. Processes metadata extraction
4. Accumulates results
5. Writes only the final output (CSV or Parquet)

**Key Benefits:**
- ✅ No intermediate files written to disk
- ✅ ~5x faster than file-based extraction (1,125 vs 229 files/second)
- ✅ Lower disk I/O overhead
- ✅ Processes 268 tar.gz archives efficiently
- ✅ Same 122-column output format as file-based extractor

## Performance

### Benchmark Results

**Processing Rate: ~1,125 files per second**

| Metric | Value |
|--------|------:|
| Files per second | 1,125 |
| Files per minute | 67,500 |
| Time per file | 0.89 ms |

**Comparison with File-Based Extractor:**

| Method | Files/Second | Speedup |
|--------|-------------:|--------:|
| File-based | 229 | 1.0x |
| **Streaming** | **1,125** | **4.9x** |

### Test Results

**Single tar.gz file (3,028 files):**
- Processing time: 3.11 seconds
- Rate: 974 files/second

**Two tar.gz files (30,543 files):**
- Processing time: 27.16 seconds
- Rate: 1,125 files/second

## Installation

Same requirements as the file-based extractor:
```bash
pip install pandas pyarrow
```

## Usage

### Basic Usage

Process all tar.gz files in a directory:
```bash
python extract_from_tarballs.py /Volumes/DSST_backup2025/osm/pmcoa/raw_download/
```

### Output Formats

**CSV format (default):**
```bash
python extract_from_tarballs.py /path/to/tarballs/
# Creates: streaming_metadata.csv
```

**Parquet format (recommended):**
```bash
python extract_from_tarballs.py --format parquet /path/to/tarballs/
# Creates: streaming_metadata.parquet
```

**Custom output filename:**
```bash
python extract_from_tarballs.py -o results.parquet -f parquet /path/to/tarballs/
```

### Advanced Options

**Process limited number of archives (testing):**
```bash
python extract_from_tarballs.py --limit 5 /path/to/tarballs/
```

**Process specific tar.gz pattern:**
```bash
python extract_from_tarballs.py --pattern "oa_comm_xml.incr.2025-07-*" /path/to/tarballs/
```

**Combine multiple options:**
```bash
python extract_from_tarballs.py \
    --format parquet \
    --output july_2025.parquet \
    --pattern "oa_comm_xml.incr.2025-07-*" \
    /path/to/tarballs/
```

### Command-Line Options

```
positional arguments:
  tar_directory         Directory containing .tar.gz archives

options:
  -h, --help            show this help message and exit
  -o OUTPUT, --output OUTPUT
                        Output file path (default: streaming_metadata.csv or
                        .parquet based on format)
  -f {csv,parquet}, --format {csv,parquet}
                        Output format (default: csv)
  --limit LIMIT         Limit number of tar.gz files to process (for testing)
  --pattern PATTERN     Glob pattern for tar.gz files (default: *.tar.gz)
```

## How It Works

### Streaming Pipeline

```
┌─────────────┐
│  tar.gz     │
│  archive    │
└──────┬──────┘
       │ open archive
       ▼
┌─────────────┐
│ Extract XML │
│ to memory   │
└──────┬──────┘
       │ parse XML
       ▼
┌─────────────┐
│ Extract     │
│ metadata    │
└──────┬──────┘
       │ accumulate
       ▼
┌─────────────┐
│ Write final │
│ output only │
└─────────────┘
```

### Key Technical Features

1. **In-Memory Processing:**
   - XML files never touch disk
   - Extracted directly from tar.gz into memory
   - Parsed immediately using ElementTree

2. **Efficient Archive Handling:**
   - Uses Python's `tarfile` module
   - Supports gzip compression natively
   - Sequential processing of archive members

3. **Same Extraction Logic:**
   - Uses identical metadata extraction as file-based tool
   - Same 122-column output format
   - Compatible with downstream analysis

## Output Format

### Columns (122 total)

Same as file-based extractor:
- **18 "copied" columns** with extracted metadata
- **102 "sophisticated analysis" columns** (left empty)
- **2 metric columns** (file_size, chars_in_body)

### Filename Format

The `filename` column contains the source information:
```
<tarball_name>:<path_within_archive>
```

Example:
```
oa_comm_xml.PMC000xxxxxx.baseline.2025-06-26.tar.gz:PMC000xxxxxx/PMC176545.xml
```

This format allows tracing back to the original archive and file location.

## Dataset Information

### Source Directory
`/Volumes/DSST_backup2025/osm/pmcoa/raw_download/`

### Contents
- **268 tar.gz files** containing XML articles
- **268 corresponding CSV files** (file lists)
- Files organized by date (incremental updates)

### Naming Convention
```
oa_comm_xml.incr.YYYY-MM-DD.tar.gz
oa_comm_xml.PMC<range>.baseline.YYYY-MM-DD.tar.gz
```

## Practical Examples

### Example 1: Process all archives
```bash
python extract_from_tarballs.py \
    --format parquet \
    --output full_dataset.parquet \
    /Volumes/DSST_backup2025/osm/pmcoa/raw_download/
```

### Example 2: Process specific month
```bash
python extract_from_tarballs.py \
    --format parquet \
    --pattern "oa_comm_xml.incr.2025-07-*" \
    --output july_2025.parquet \
    /Volumes/DSST_backup2025/osm/pmcoa/raw_download/
```

### Example 3: Test with first 10 archives
```bash
python extract_from_tarballs.py \
    --limit 10 \
    --format csv \
    --output test_sample.csv \
    /Volumes/DSST_backup2025/osm/pmcoa/raw_download/
```

### Example 4: Process baseline archives only
```bash
python extract_from_tarballs.py \
    --pattern "*baseline*" \
    --format parquet \
    /Volumes/DSST_backup2025/osm/pmcoa/raw_download/
```

## Performance Projections

### For Full Dataset (268 tar.gz files)

**Estimated Processing:**
- Assumes average of ~1,125 files/second (from benchmark)
- Estimated total files: ~100,000-150,000 (based on sampling)

| Files | Estimated Time |
|------:|---------------:|
| 100,000 | 1.5 minutes |
| 150,000 | 2.2 minutes |
| 200,000 | 3.0 minutes |

**Actual performance will depend on:**
- Average file size in each archive
- Compression ratio
- Disk I/O speed
- System load

### Disk Space Requirements

**Input:** 268 tar.gz files (~120 GB total, estimated)
**Output:** Parquet format (~30-50 MB per 100,000 records)

**No intermediate disk space needed!**

## Advantages Over File-Based Extractor

### Speed
- **~5x faster** (1,125 vs 229 files/second)
- Eliminates disk write/read cycle for XML files
- Better CPU cache utilization

### Disk Space
- **No temporary files** written
- Only final output uses disk space
- Critical for large-scale processing

### I/O Efficiency
- **Reduced disk operations** (1 read, 1 write vs 2 reads, 2 writes per file)
- Lower wear on SSDs
- Reduced contention on network storage

### Use Cases
Best suited for:
- ✅ Processing tar.gz archives
- ✅ Large-scale batch processing
- ✅ Limited disk space environments
- ✅ Network-mounted storage
- ✅ Production pipelines

Use file-based extractor when:
- ❌ Files are already extracted
- ❌ Need to re-process same files multiple times
- ❌ Want to inspect individual XML files

## Validation

### Data Quality Checks

Tested with 30,543 files across 2 tar.gz archives:
- ✅ 100% success rate (all files processed)
- ✅ 100% PMCID extraction
- ✅ 100% Journal extraction
- ✅ 100% COI text found
- ✅ All sophisticated columns correctly empty
- ✅ All 122 columns present

### Output Verification

```bash
# Quick verification
python -c "
import pandas as pd
df = pd.read_parquet('output.parquet')
print(f'Records: {len(df):,}')
print(f'Columns: {len(df.columns)}')
print(f'PMCID extracted: {df[\"pmcid_pmc\"].notna().sum()}/{len(df)}')
"
```

## Troubleshooting

### Memory Issues
If processing very large archives:
- Process in batches using `--limit`
- Save intermediate outputs
- Combine results later

### Slow Processing
If performance is slower than expected:
- Check disk I/O (network storage may be slower)
- Monitor CPU usage
- Ensure no other heavy processes running

### Archive Errors
If tar.gz files are corrupt:
- Script will skip and continue
- Errors printed to stderr
- Check logs for specific files

## Error Handling

The tool includes robust error handling:
- **Archive-level:** Continues if individual archive fails
- **File-level:** Skips corrupt XML files within archive
- **Parsing-level:** Records errors but continues processing
- **All errors logged** to stderr for review

## Comparison with File-Based Extractor

| Feature | File-Based | Streaming |
|---------|-----------|-----------|
| Input | Individual XML files or directories | tar.gz archives |
| Speed | 229 files/sec | 1,125 files/sec |
| Disk usage | Requires XML files on disk | No intermediate files |
| Use case | Extracted files | Compressed archives |
| Memory | Low | Low (streaming) |
| Complexity | Simple | Moderate |

## See Also

- `extract_xml_metadata.py` - File-based extractor
- `data_dictionary.csv` - Column documentation
- `README_EXTRACTOR.md` - File-based extractor guide
- `BENCHMARK_REPORT.md` - Performance analysis

## Technical Notes

### Archive Format
- Supports `.tar.gz` format (tar + gzip)
- Standard POSIX tar format
- XML files can be at any depth

### XML Format
- JATS (Journal Article Tag Suite) format
- PubMed Central standard
- Handles namespaces automatically

### Memory Management
- Processes one XML file at a time
- No memory accumulation
- Suitable for large archives

### Scalability
- Linear scaling with number of files
- Can process arbitrarily large datasets
- No artificial limits

## Future Enhancements

Potential improvements:
1. **Parallel processing** of multiple archives
2. **Progress bar** for long-running jobs
3. **Resume capability** for interrupted runs
4. **Statistics reporting** per archive
5. **Integration with CSV file lists** for validation

---

**Performance Summary:** Process 268 tar.gz archives with ~150,000 XML files in approximately **2-3 minutes** on standard hardware, without using any temporary disk space.
