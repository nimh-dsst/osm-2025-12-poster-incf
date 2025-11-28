# HPC Strategy for oddpub Processing of PMC XML Files (REVISED v2)

**Date**: 2025-11-28 (Revised - chunked processing for realistic timing)
**Target**: Process ~7M PMC baseline XML files with oddpub on NIH HPC
**HPC System**: SLURM with swarm wrapper
**Status**: ⚠️ **NOT YET TESTED ON HPC** - Timing estimates based on EC2 instance testing

## Executive Summary

**Recommended Approach**: Process baseline tar.gz files with chunking (2,346 jobs)
**Scope**: Baseline files only (~7M XMLs from 39 tar.gz files)
**Estimated Wall Time**: ~13-14 hours with 2,346 parallel nodes
**Estimated CPU Time**: ~31,500 CPU-hours total (~1,311 CPU-days)

⚠️ **Important**: These timing estimates are based on testing on an EC2 instance and may differ on NIH HPC infrastructure.

## Critical Finding: oddpub is SLOW

**Processing Speed**: ~16 seconds per XML file
**Test Results**: 20 files processed in 322 seconds

This means processing large tar.gz files (610k XMLs) would take **79 days** per file without chunking!

**Solution**: Split large files into chunks of 3,000 XMLs each (~13 hours processing time per chunk)

## Strategy Overview

Process baseline tar.gz files with intelligent chunking to keep each job under 14 hours:
- Files with >3,000 XMLs are split into multiple chunks
- Each chunk processes 3,000 XMLs independently
- Files with ≤3,000 XMLs are processed in a single job
- Uses streaming extraction (no disk writes)
- Avoids filesystem issues from flat directory extraction

### Why This Approach?

**Problems Solved**:
1. **Flat extraction bottleneck**: Avoided by processing tar.gz files directly
2. **Long-running jobs**: Chunking keeps all jobs under 14 hours
3. **Processing time**: Massive parallelization (2,346 jobs vs 39 sequential jobs)

**Chunking Details**:
- Chunk size: 3,000 XMLs per chunk
- Processing time per chunk: ~13.4 hours
- 35 out of 39 files need chunking
- Largest file (610k XMLs) split into 204 chunks

### Performance Comparison

| Approach | Jobs | Wall Time | Feasibility |
|----------|------|-----------|-------------|
| Sequential (no chunking) | 39 | **79 days** | ❌ Not practical |
| **Chunked parallel (recommended)** | **2,346** | **13.4 hours** | ✅ Practical |

## Two-Phase Workflow

### Phase 1: Process Chunked tar.gz Files (Parallel)

Process baseline tar.gz files with automatic chunking using SLURM swarm.

**Create Swarm File**:
```bash
cd /data/oddpub_scripts
bash create_oddpub_swarm.sh /data/pmcoa /data/oddpub_output /data/oddpub_scripts
```

This generates `oddpub_swarm.txt` with 2,346 jobs:
- Small files (<3k XMLs): Single job per file
- Large files (>3k XMLs): Multiple chunked jobs
- Example output:
  ```
  oa_comm_xml.PMC010xxxxxx.baseline.2025-06-26: 610,792 XMLs -> 204 chunks
  oa_other_xml.PMC011xxxxxx.baseline.2025-06-26: 2,156 XMLs -> 1 job
  ```

**Submit Swarm Job**:
```bash
swarm -f oddpub_swarm.txt \
  -g 32 \
  -t 8 \
  --time 14:00:00 \
  --module python/3.9 R/4.2 \
  --logdir /data/oddpub_logs

# Monitor progress
jobload -u $USER
watch -n 30 'ls -1 /data/oddpub_output/*.parquet | wc -l'
```

**Resource Requirements per Job**:
- Memory: 32 GB (`-g 32`)
- CPUs: 8 threads (`-t 8`)
- Time: 14 hours max (typical: 9-13 hours depending on chunk size)
- Total: 2,346 parallel jobs

### Phase 2: Merge Results

Combine all chunk results into a single parquet file.

**Merge Script**:
```bash
python /data/oddpub_scripts/merge_oddpub_results.py \
  /data/oddpub_output \
  /data/oddpub_results_final.parquet
```

Expected output: 2,346 parquet files → 1 combined file

---

## Complete Workflow

### 1. Setup on HPC

```bash
# SSH to HPC
ssh user@biowulf.nih.gov

# Load modules
module load python/3.9 R/4.2

# Install R packages (one-time)
R -e "install.packages(c('oddpub', 'future', 'furrr', 'progressr'), repos='https://cloud.r-project.org')"

# Install Python packages
pip install --user pandas pyarrow
```

### 2. Transfer Scripts

```bash
# From local machine
cd osm-2025-12-poster-incf

scp extraction_tools/process_pmcoa_with_oddpub.py \
    hpc_scripts/merge_oddpub_results.py \
    hpc_scripts/create_oddpub_swarm.sh \
    user@biowulf.nih.gov:/data/oddpub_scripts/
```

### 3. Create Swarm File

```bash
cd /data/oddpub_scripts
chmod +x create_oddpub_swarm.sh
bash create_oddpub_swarm.sh /data/pmcoa /data/oddpub_output /data/oddpub_scripts
```

Output:
```
Found 39 baseline tar.gz files
  oa_comm_xml.PMC010xxxxxx.baseline.2025-06-26: 610,792 XMLs -> 204 chunks
  ...

Created oddpub_swarm.txt with 2346 jobs
  Files split into chunks: 35
  Single-job files: 4
```

### 4. Submit Processing Jobs

```bash
swarm -f oddpub_swarm.txt \
  -g 32 \
  -t 8 \
  --time 14:00:00 \
  --module python/3.9 R/4.2 \
  --logdir /data/oddpub_logs
```

### 5. Monitor Progress

```bash
# Check job status
jobload -u $USER

# Count completed files
ls -1 /data/oddpub_output/*.parquet | wc -l
# Should be 2,346 when complete

# Watch progress
watch -n 30 'echo "Completed: $(ls -1 /data/oddpub_output/*.parquet 2>/dev/null | wc -l) / 2346"'
```

### 6. Merge Results

```bash
python merge_oddpub_results.py \
  /data/oddpub_output \
  /data/oddpub_results_final.parquet
```

### 7. Transfer Results

```bash
# From local machine
scp user@biowulf.nih.gov:/data/oddpub_results_final.parquet ~/
```

---

## Resource Estimation

### Total Resource Requirements

| Resource | Per Job | Total (2,346 jobs) |
|----------|---------|---------------------|
| CPU threads | 8 | ~18,768 CPU-cores |
| Memory | 32 GB | ~75 TB-hours |
| Wall time | ~13 hours | With 2,346 nodes |
| CPU time | ~13 hours | **31,500 CPU-hours** |

### Disk Space

**Temporary** (during processing):
- Individual chunk files: ~5-10 MB each × 2,346 = ~12-23 GB

**Final Output**:
- Individual parquet files: ~5-10 MB each
- Merged parquet file: ~100-150 MB (compressed)

---

## Fault Tolerance

### Check for Failed Jobs

```bash
# Expected: 2,346 files
expected=2346
completed=$(ls -1 /data/oddpub_output/*.parquet 2>/dev/null | wc -l)

echo "Progress: $completed / $expected"

if [ "$completed" -lt "$expected" ]; then
  echo "Missing $((expected - completed)) files"
fi
```

### Identify Missing tar.gz Chunks

```bash
# Create list of expected output files
cd /data/pmcoa
python3 << 'EOF'
import glob
import os

for tarfile in sorted(glob.glob("*baseline*.tar.gz")):
    csv_file = tarfile.replace('.tar.gz', '.filelist.csv')
    if not os.path.exists(csv_file):
        continue

    xml_count = sum(1 for line in open(csv_file)) - 1
    basename = tarfile.replace('.tar.gz', '')

    if xml_count > 3000:
        # Chunked file
        num_chunks = (xml_count + 2999) // 3000
        for chunk in range(num_chunks):
            print(f"{basename}_chunk{chunk}_results.parquet")
    else:
        # Single file
        print(f"{basename}_results.parquet")
EOF
```

### Rerun Failed Jobs

Extract failed job commands from swarm file and create retry file:
```bash
# Find missing files
python3 find_missing_chunks.py > missing_files.txt

# Create retry swarm file
grep -f missing_files.txt oddpub_swarm.txt > oddpub_retry_swarm.txt

# Submit retry jobs
swarm -f oddpub_retry_swarm.txt \
  -g 32 -t 8 --time 14:00:00 \
  --module python/3.9 R/4.2 \
  --logdir /data/oddpub_logs
```

---

## Troubleshooting

### Check Logs for Errors

```bash
cd /data/oddpub_logs

# Find error logs
grep -l "Error\\|Failed\\|Exception" swarm_*.e | head -20

# View specific error log
less swarm_12345.e
```

### Common Issues

**Issue**: R package not found
```
Error in library(oddpub) : there is no package called 'oddpub'
```
**Solution**:
```bash
module load R/4.2
R -e "install.packages('oddpub', repos='https://cloud.r-project.org')"
```

**Issue**: Out of memory
```
Error: cannot allocate vector of size X GB
```
**Solution**: Increase memory allocation:
```bash
swarm -f oddpub_swarm.txt -g 64 -t 8 ...  # 64 GB instead of 32 GB
```

**Issue**: Timeout (job exceeds 14 hours)
```
TIMEOUT: job killed after 14 hours
```
**Solution**: These should be rare with 3k chunk size. If needed:
```bash
# Reduce chunk size in create_oddpub_swarm.sh
CHUNK_SIZE=2000  # Instead of 3000

# Or increase time limit
swarm -f oddpub_swarm.txt -g 32 -t 8 --time 18:00:00 ...
```

---

## Expected Output

**Final File**: `/data/oddpub_results_final.parquet`

**Size**: ~100-150 MB (compressed parquet)

**Records**: ~6,980,244 (baseline files only)

**Schema**: 15 columns (see `data_dictionary_oddpub.csv`)
- `article`, `pmid`, `pmcid`
- `is_open_data`, `is_open_code`
- `open_data_category`, `open_data_statements`
- `is_open_data_das`, `is_open_code_cas`
- `das`, `cas`
- And more

---

## Files to Transfer to HPC

From `osm-2025-12-poster-incf/`:

1. `extraction_tools/process_pmcoa_with_oddpub.py` - Main processing script (supports chunking)
2. `hpc_scripts/merge_oddpub_results.py` - Merge results
3. `hpc_scripts/create_oddpub_swarm.sh` - Generate swarm file with automatic chunking

**Transfer Command**:
```bash
scp extraction_tools/process_pmcoa_with_oddpub.py \
    hpc_scripts/merge_oddpub_results.py \
    hpc_scripts/create_oddpub_swarm.sh \
    user@biowulf.nih.gov:/data/oddpub_scripts/
```

---

## Processing Speed Analysis

**Test Environment**: EC2 instance (non-HPC), 2025-11-27-28

**Test Results** (20 XML files):
- Total time: 322 seconds
- oddpub R processing: 223 seconds for 20 files
- Rate: 11.15 seconds per file (oddpub only)
- Overall rate: 16.09 seconds per file (including overhead)

**Functionality Tests**:
- ✅ Single tar.gz file processing (1 XML, 22.5 seconds)
- ✅ Chunked processing (tested with --start-index and --chunk-size)
- ✅ Swarm file generation (2,346 jobs from 39 baseline files)
- ⚠️ **NOT YET TESTED**: Actual HPC execution, merge script on 2,346 files

**Chunk Size Selection**:
- Target: <10 hours per chunk
- Calculation: 36,000 seconds / 16.09 sec/file = 2,237 files
- Selected: 3,000 files per chunk (conservative, ~13 hours)

**Why 3,000?**:
- Keeps most jobs well under 14 hours
- Balances number of jobs vs job length
- Provides buffer for slower processing on some files

---

## Summary

**Workflow**: Baseline tar.gz → chunked processing with oddpub → merge
**Scope**: 39 baseline files, ~7M XMLs
**Time**: ~13-14 hours wall time (with 2,346 parallel jobs)
**Jobs**: 2,346 parallel jobs (35 files split into chunks)
**Simplicity**: Automated chunking, uses validated script
**Filesystem**: No extraction bottleneck (streaming only)

This approach is **practical and realistic** given oddpub's actual processing speed.

---

## Related Documentation

- **oddpub Usage**: `../extraction_tools/README_ODDPUB.md`
- **Data Dictionary**: `../docs/data_dictionary_oddpub.csv`
- **HPC Scripts README**: `../hpc_scripts/README.md`
- **NIH HPC Swarm**: https://hpc.nih.gov/apps/swarm.html
