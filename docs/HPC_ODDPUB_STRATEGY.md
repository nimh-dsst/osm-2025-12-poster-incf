# HPC Strategy for oddpub Processing of 6.4M XMLs

**Date**: 2025-11-27
**Target**: Process 6.4M PMC XML files with oddpub on NIH HPC
**HPC System**: SLURM with swarm wrapper

## Strategy Overview

Since the HPC system has no disk space constraints and abundant compute resources, we'll use a **parallel batch processing** approach rather than sequential streaming processing.

### Key Advantages

1. **Maximum Parallelization**: 6,400 jobs vs 268 sequential tar.gz processing
2. **Better Load Balancing**: Uniform batch sizes (~1,000 XMLs each)
3. **Fault Tolerance**: Failed jobs can be rerun independently
4. **Optimal Resource Usage**: Leverages HPC's compute capacity

### Estimated Performance

| Approach | Jobs | Parallelization | Wall Time | CPU Time |
|----------|------|-----------------|-----------|----------|
| **Sequential (streaming)** | 1 | None | 65 hours | 65 hours |
| **Parallel by tar.gz** | 268 | 268x | ~15 min | 65 hours |
| **Parallel by batch (recommended)** | 6,400 | 6400x | ~6 min | 640 hours |

With sufficient nodes, parallel batch processing completes in **~6 minutes wall time**.

## Four-Phase Workflow

### Phase 1: Extract tar.gz Archives (One-Time)

Extract all PMC tar.gz files to a working directory on HPC.

**Approach A: Parallel Extraction with swarm**
```bash
# Create extraction swarm file
ls /data/pmcoa/*.tar.gz | while read tarfile; do
  basename="${tarfile%.tar.gz}"
  echo "tar -xzf $tarfile -C /scratch/pmcoa_extracted/"
done > extract_swarm.txt

# Run extraction swarm (268 jobs, ~5 min wall time)
swarm -f extract_swarm.txt -g 4 -t 4 --time 00:30:00 --module none
```

**Approach B: Single Job Extraction**
```bash
#!/bin/bash
#SBATCH --time=02:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G

cd /data/pmcoa
for tarfile in *.tar.gz; do
  tar -xzf "$tarfile" -C /scratch/pmcoa_extracted/
done
```

**Storage Requirements**: ~100-150 GB for 6.4M XMLs (uncompressed from ~140 GB compressed)

---

### Phase 2: Create Batch Manifest

Generate a manifest file listing all XMLs and split into batches.

**Script**: `create_batch_manifest.py`

```python
#!/usr/bin/env python3
"""Create batch manifest for oddpub processing."""

import os
import sys
from pathlib import Path

# Configuration
XML_DIR = "/scratch/pmcoa_extracted"
BATCH_SIZE = 1000
OUTPUT_DIR = "/data/oddpub_batches"

def main():
    # Find all XML files
    xml_files = sorted(Path(XML_DIR).rglob("*.xml"))
    total_files = len(xml_files)
    num_batches = (total_files + BATCH_SIZE - 1) // BATCH_SIZE

    print(f"Found {total_files:,} XML files")
    print(f"Creating {num_batches:,} batches of {BATCH_SIZE} files")

    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Write batch manifest files
    for batch_idx in range(num_batches):
        start_idx = batch_idx * BATCH_SIZE
        end_idx = min((batch_idx + 1) * BATCH_SIZE, total_files)
        batch_files = xml_files[start_idx:end_idx]

        manifest_file = f"{OUTPUT_DIR}/batch_{batch_idx:05d}.txt"
        with open(manifest_file, 'w') as f:
            for xml_file in batch_files:
                f.write(f"{xml_file}\n")

        if (batch_idx + 1) % 100 == 0:
            print(f"Created {batch_idx + 1:,} batch manifests...")

    print(f"\nDone! Created {num_batches:,} batch manifest files in {OUTPUT_DIR}")
    print(f"Each batch file lists {BATCH_SIZE} XML files to process")

if __name__ == "__main__":
    main()
```

**Run**:
```bash
python create_batch_manifest.py
# Creates: /data/oddpub_batches/batch_00000.txt through batch_06399.txt
```

---

### Phase 3: Process Batches with oddpub (Parallel)

Process each batch in parallel using swarm.

**Script**: `process_oddpub_batch.py`

```python
#!/usr/bin/env python3
"""Process a single batch of XMLs with oddpub."""

import sys
import os
import tempfile
import subprocess
from pathlib import Path
import pandas as pd
import xml.etree.ElementTree as ET

def extract_article_id(root, id_type):
    """Extract article ID from XML."""
    for article_id in root.findall(".//article-meta/article-id"):
        if article_id.get("pub-id-type") == id_type:
            return article_id.text
    return ""

def extract_body_text(xml_file):
    """Extract body text and IDs from XML file."""
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()

        pmid = extract_article_id(root, 'pmid')
        pmcid = extract_article_id(root, 'pmc')

        body = root.find(".//body")
        if body is not None:
            body_text = ' '.join(body.itertext())
        else:
            body_text = ''

        return pmid, pmcid, body_text
    except Exception as e:
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
        result = subprocess.run(
            ['/usr/bin/Rscript', r_script_path],
            capture_output=True,
            text=True,
            timeout=600
        )

        if result.returncode != 0:
            raise RuntimeError(f"R script failed: {result.stderr}")

        return True
    finally:
        os.unlink(r_script_path)

def main(batch_file, output_file):
    """Process a batch of XML files with oddpub."""

    # Read batch manifest
    with open(batch_file) as f:
        xml_files = [line.strip() for line in f if line.strip()]

    print(f"Processing {len(xml_files)} XML files from {batch_file}")

    # Create temporary directory for text files
    with tempfile.TemporaryDirectory() as temp_dir:
        # Extract body text from XMLs
        records = []
        for xml_file in xml_files:
            pmid, pmcid, body_text = extract_body_text(xml_file)

            # Write text file for oddpub
            filename = Path(xml_file).stem
            text_file = Path(temp_dir) / f"{filename}.txt"
            with open(text_file, 'w') as f:
                f.write(body_text)

            records.append({
                'filename': filename,
                'pmid': pmid,
                'pmcid': pmcid
            })

        # Run oddpub
        oddpub_csv = Path(temp_dir) / 'oddpub_results.csv'
        run_oddpub(temp_dir, oddpub_csv)

        # Load oddpub results
        oddpub_df = pd.read_csv(oddpub_csv)

        # Merge with PMIDs
        records_df = pd.DataFrame(records)

        # Clean article column for matching
        oddpub_df['filename'] = oddpub_df['article'].str.replace('.txt$', '', regex=True)

        # Merge
        results = oddpub_df.merge(records_df, on='filename', how='left')

        # Drop duplicate filename column
        results = results.drop(columns=['filename'])

        # Save to parquet
        results.to_parquet(output_file, index=False)

    print(f"Saved {len(results)} results to {output_file}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: process_oddpub_batch.py <batch_file> <output_file>")
        sys.exit(1)

    batch_file = sys.argv[1]
    output_file = sys.argv[2]

    main(batch_file, output_file)
```

**Create Swarm File**:
```bash
#!/bin/bash
# create_swarm.sh

BATCH_DIR="/data/oddpub_batches"
OUTPUT_DIR="/data/oddpub_output"

mkdir -p "$OUTPUT_DIR"

# Create swarm file
for batch_file in "$BATCH_DIR"/batch_*.txt; do
  batch_name=$(basename "$batch_file" .txt)
  output_file="$OUTPUT_DIR/${batch_name}_results.parquet"
  echo "python process_oddpub_batch.py $batch_file $output_file"
done > oddpub_swarm.txt

echo "Created oddpub_swarm.txt with $(wc -l < oddpub_swarm.txt) jobs"
```

**Submit Swarm**:
```bash
# Create swarm file
bash create_swarm.sh

# Submit swarm job
swarm -f oddpub_swarm.txt \
  -g 16 \
  -t 4 \
  --time 00:30:00 \
  --module python/3.9 R/4.2 \
  --logdir /data/oddpub_logs

# Monitor progress
jobload -u $USER
```

**Resource Requirements per Job**:
- Memory: 16 GB (`-g 16`)
- CPUs: 4 threads (`-t 4`)
- Time: 30 minutes per batch
- 6,400 jobs × 4 threads = 25,600 CPU-hours total

---

### Phase 4: Merge Results

Combine all batch results into a single file.

**Script**: `merge_oddpub_results.py`

```python
#!/usr/bin/env python3
"""Merge oddpub batch results into single parquet file."""

import pandas as pd
from pathlib import Path
import sys

def main(input_dir, output_file):
    """Merge all parquet files in directory."""

    parquet_files = sorted(Path(input_dir).glob("batch_*_results.parquet"))

    print(f"Found {len(parquet_files)} result files")

    # Read and concatenate
    dfs = []
    for i, pf in enumerate(parquet_files):
        df = pd.read_parquet(pf)
        dfs.append(df)

        if (i + 1) % 100 == 0:
            print(f"Loaded {i + 1:,} files...")

    # Concatenate all
    print("Concatenating all results...")
    combined = pd.concat(dfs, ignore_index=True)

    print(f"Total records: {len(combined):,}")
    print(f"Columns: {list(combined.columns)}")

    # Save
    print(f"Saving to {output_file}...")
    combined.to_parquet(output_file, index=False)

    # Summary stats
    print("\nSummary:")
    print(f"  Total articles: {len(combined):,}")
    print(f"  Open data detected: {combined['is_open_data'].sum():,} ({100*combined['is_open_data'].mean():.2f}%)")
    print(f"  Open code detected: {combined['is_open_code'].sum():,} ({100*combined['is_open_code'].mean():.2f}%)")
    print(f"  Output file: {output_file}")
    print(f"  File size: {Path(output_file).stat().st_size / 1024**2:.1f} MB")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: merge_oddpub_results.py <input_dir> <output_file>")
        sys.exit(1)

    input_dir = sys.argv[1]
    output_file = sys.argv[2]

    main(input_dir, output_file)
```

**Run**:
```bash
python merge_oddpub_results.py /data/oddpub_output /data/oddpub_results_final.parquet
```

---

## Complete Workflow Summary

```bash
# Phase 1: Extract tar.gz files (one-time, ~5 minutes wall time)
ls /data/pmcoa/*.tar.gz | \
  sed 's|.*|tar -xzf & -C /scratch/pmcoa_extracted/|' > extract_swarm.txt
swarm -f extract_swarm.txt -g 4 -t 4 --time 00:30:00

# Phase 2: Create batch manifests (~1 minute)
python create_batch_manifest.py

# Phase 3: Process batches with oddpub (~6 minutes wall time with full parallelization)
bash create_swarm.sh
swarm -f oddpub_swarm.txt -g 16 -t 4 --time 00:30:00 \
  --module python/3.9 R/4.2 --logdir /data/oddpub_logs

# Phase 4: Merge results (~5 minutes)
python merge_oddpub_results.py /data/oddpub_output /data/oddpub_results_final.parquet
```

**Total Wall Time**: ~15-20 minutes (with sufficient compute nodes)

---

## Fault Tolerance & Monitoring

### Check for Failed Jobs

```bash
# Check swarm job status
jobload -u $USER

# Find failed batches
find /data/oddpub_output -name "batch_*_results.parquet" | wc -l
# Should be 6400 if all succeeded

# Identify missing batches
comm -23 \
  <(seq -f "batch_%05g_results.parquet" 0 6399 | sort) \
  <(ls /data/oddpub_output/*.parquet | xargs -n1 basename | sort) \
  > missing_batches.txt
```

### Rerun Failed Jobs

```bash
# Create swarm file for only failed jobs
while read batch; do
  batch_num="${batch#batch_}"
  batch_num="${batch_num%_results.parquet}"
  batch_file="/data/oddpub_batches/batch_${batch_num}.txt"
  output_file="/data/oddpub_output/batch_${batch_num}_results.parquet"
  echo "python process_oddpub_batch.py $batch_file $output_file"
done < missing_batches.txt > oddpub_retry_swarm.txt

# Resubmit failed jobs
swarm -f oddpub_retry_swarm.txt -g 16 -t 4 --time 00:30:00 \
  --module python/3.9 R/4.2 --logdir /data/oddpub_logs
```

---

## Resource Estimation

### Total Resource Requirements

| Resource | Per Job | Total (6,400 jobs) |
|----------|---------|-------------------|
| CPU threads | 4 | 25,600 CPU-hours |
| Memory | 16 GB | 102,400 GB-hours |
| Disk (temp) | 50 MB | 320 GB peak |
| Wall time | 6 min | With 1,000 nodes |

### Cost Estimation (NIH HPC)

NIH HPC Biowulf is free for NIH users. No cost concerns.

---

## Optimization Options

### Option 1: Smaller Batches for Faster Turnaround
- Batch size: 500 XMLs
- Jobs: 12,800
- Wall time: ~3 minutes (with sufficient nodes)
- Better fault tolerance

### Option 2: Larger Batches for Fewer Jobs
- Batch size: 2,000 XMLs
- Jobs: 3,200
- Wall time: ~12 minutes per job
- Less scheduler overhead

### Option 3: Adaptive Batch Sizing
- Small batches for large tar.gz files
- Large batches for small tar.gz files
- Balanced job completion times

**Recommendation**: Use 1,000 XML batch size (6,400 jobs) for good balance.

---

## Alternative: Process by tar.gz File

If extraction is undesirable, process each tar.gz file in parallel:

**Swarm File**:
```bash
ls /data/pmcoa/*.tar.gz | while read tarfile; do
  output_file="/data/oddpub_output/$(basename $tarfile .tar.gz)_results.parquet"
  echo "python process_pmcoa_with_oddpub.py --batch-size 500 --output-file $output_file $tarfile"
done > oddpub_tarfile_swarm.txt

swarm -f oddpub_tarfile_swarm.txt -g 32 -t 8 --time 01:00:00 \
  --module python/3.9 R/4.2 --logdir /data/oddpub_logs
```

**Pros**: No extraction phase
**Cons**: Less parallelization (268 vs 6,400 jobs), uneven job sizes

---

## Files to Transfer to HPC

1. `process_oddpub_batch.py` - Batch processing script
2. `create_batch_manifest.py` - Manifest creation script
3. `merge_oddpub_results.py` - Results merging script
4. `create_swarm.sh` - Swarm file generator

**Transfer**:
```bash
scp process_oddpub_batch.py create_batch_manifest.py merge_oddpub_results.py \
    create_swarm.sh user@biowulf.nih.gov:/data/oddpub_scripts/
```

---

## Expected Output

**Final File**: `/data/oddpub_results_final.parquet`

**Schema**: 15 columns (see `data_dictionary_oddpub.csv`)
- article, is_open_data, open_data_category, is_reuse
- is_open_code, is_code_supplement, is_code_reuse
- is_open_data_das, is_open_code_cas
- das, open_data_statements, cas, open_code_statements
- pmid, pmcid

**Size**: ~50-100 MB (compressed parquet)
**Records**: 6,400,000

---

## Next Steps

1. Transfer scripts to HPC
2. Load required modules: `module load python/3.9 R/4.2`
3. Install R packages: `R -e "install.packages(c('oddpub', 'future', 'furrr', 'progressr'))"`
4. Run Phase 1: Extract tar.gz files
5. Run Phase 2: Create batch manifests
6. Run Phase 3: Submit swarm job
7. Monitor progress with `jobload -u $USER`
8. Run Phase 4: Merge results
9. Transfer final parquet back to local system
