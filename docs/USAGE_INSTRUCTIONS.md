# Usage Instructions: Iterative Field Population
**Document created:** 2025-11-11

## Overview
This two-step process populates `is_open_code` and `is_open_data` fields in metadata files using rtrans data, without requiring large amounts of memory.

## Prerequisites
```bash
cd /home/ec2-user/rtransparent/extract-XML-metadata
source venv/bin/activate
```

## Step 1: Split rtrans file into chunks

```bash
python split_rtrans.py \
  --input ~/pmcoaXMLs/rtrans_out/4.02M_2025-09-25.parquet \
  --output-dir ~/pmcoaXMLs/rtrans_out_chunks \
  --chunk-size 200000
```

**What this does:**
- Reads the 4M row rtrans file
- Extracts only: `pmid`, `is_open_code`, `is_open_data`
- Splits into ~20 chunk files of 200k rows each
- Each chunk is ~5-10 MB (very memory efficient)

**Output:**
- `~/pmcoaXMLs/rtrans_out_chunks/rtrans_chunk_001.parquet`
- `~/pmcoaXMLs/rtrans_out_chunks/rtrans_chunk_002.parquet`
- ... (about 20 files total)

**Expected time:** ~30-60 seconds

## Step 2: Populate metadata files

```bash
python populate_metadata_iterative.py \
  --metadata-dir ~/pmcoaXMLs/extracted_metadata_parquet \
  --rtrans-chunks ~/pmcoaXMLs/rtrans_out_chunks \
  --output-dir ~/pmcoaXMLs/populated_metadata
```

**What this does:**
- For each of the 25 metadata files:
  - Loads the metadata file
  - Checks all 20 rtrans chunks for matching PMIDs
  - Populates blank `is_open_code` and `is_open_data` fields
  - Saves updated file to output directory
- Original files remain unchanged

**Output:**
- `~/pmcoaXMLs/populated_metadata/oa_comm_PMC000_baseline.parquet`
- `~/pmcoaXMLs/populated_metadata/oa_comm_PMC001_baseline.parquet`
- ... (25 files total, same structure as originals)

**Expected time:** ~5-10 minutes for all files

## Memory Usage

- **Step 1:** ~150-200 MB peak (loading rtrans file once)
- **Step 2:** ~50-100 MB per metadata file
  - Largest file: oa_comm_PMC010_baseline.parquet (610k rows)
  - Each rtrans chunk: ~5-10 MB
  - Total peak: < 400 MB

## Verification

After completion, check the results:

```bash
# Count populated fields
python -c "
import pandas as pd
from glob import glob

files = glob('~/pmcoaXMLs/populated_metadata/*.parquet')
for f in sorted(files)[:3]:  # Check first 3 files
    df = pd.read_parquet(f)
    code_blank = df['is_open_code'].isna().sum()
    data_blank = df['is_open_data'].isna().sum()
    print(f'{f}: is_open_code blank={code_blank}, is_open_data blank={data_blank}')
"
```

## Options

### Custom chunk size
If memory is very limited, use smaller chunks:
```bash
python split_rtrans.py ... --chunk-size 100000
```

### Populate additional fields
To populate other overlapping fields (journal, affiliation_country):
```bash
python populate_metadata_iterative.py ... --fields is_open_code,is_open_data,journal,affiliation_country
```

## Resuming

If interrupted:
- **Step 1:** Just re-run (fast, will overwrite chunks)
- **Step 2:** Delete partially processed files in output directory and re-run

## Cleanup

After verification, you can remove the chunk directory to save space:
```bash
rm -rf ~/pmcoaXMLs/rtrans_out_chunks
```

The populated metadata files are your final output.
