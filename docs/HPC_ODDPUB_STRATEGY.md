# HPC Strategy for oddpub Processing of 6.4M XMLs (REVISED)

**Date**: 2025-11-27 (Revised - avoids flat directory extraction)
**Target**: Process 6.4M PMC XML files with oddpub on NIH HPC
**HPC System**: SLURM with swarm wrapper

## Executive Summary

**Recommended Approach**: Process each tar.gz file in parallel (268 jobs)
**Wall Time**: ~15-20 minutes with 268 nodes
**CPU Time**: ~65 CPU-hours total

## Strategy Overview

Process each tar.gz archive file directly using the existing `process_pmcoa_with_oddpub.py` script, avoiding the filesystem performance issues that would result from extracting 6.4M files to a flat directory structure.

### Why This Approach?

**Problem with flat extraction**:
- Extracting 6.4M files to a single directory causes severe filesystem degradation
- Directory operations (ls, find, glob) become extremely slow or timeout
- File creation overhead increases dramatically with directory size
- Inode table fragmentation and metadata overhead

**Solution - Process tar.gz files directly**:
-Uses streaming extraction (memory only, no disk writes)
- Leverages validated `process_pmcoa_with_oddpub.py` script
- 268-way parallelization is excellent for HPC
- Simple two-phase workflow

### Performance Comparison

| Approach | Jobs | Wall Time | Filesystem Impact |
|----------|------|-----------|-------------------|
| Sequential | 1 | 65 hours | None |
| **Parallel tar.gz (recommended)** | **268** | **15-20 min** | **None** |
| ~~Extract + batch~~ | ~~6,400~~ | ~~Unknown~~ | ~~Severe~~ |

## Two-Phase Workflow

### Phase 1: Process tar.gz Files (Parallel)

Process each tar.gz archive in parallel using SLURM swarm.

**Create Swarm File**:
```bash
#!/bin/bash
# create_oddpub_swarm.sh

TAR_DIR="/data/pmcoa"
OUTPUT_DIR="/data/oddpub_output"

mkdir -p "$OUTPUT_DIR"

# Create swarm file - one job per tar.gz
for tarfile in "$TAR_DIR"/*.tar.gz; do
  basename=$(basename "$tarfile" .tar.gz)
  output_file="$OUTPUT_DIR/${basename}_results.parquet"

  # Use existing process_pmcoa_with_oddpub.py script
  echo "python /data/oddpub_scripts/process_pmcoa_with_oddpub.py --batch-size 500 --output-file $output_file $tarfile"
done > oddpub_swarm.txt

echo "Created oddpub_swarm.txt with $(wc -l < oddpub_swarm.txt) jobs"
```

**Submit Swarm Job**:
```bash
# Transfer script to HPC first
scp ../extraction_tools/process_pmcoa_with_oddpub.py user@biowulf.nih.gov:/data/oddpub_scripts/

# Create swarm file
bash create_oddpub_swarm.sh

# Submit to SLURM
swarm -f oddpub_swarm.txt \
  -g 32 \
  -t 8 \
  --time 01:00:00 \
  --module python/3.9 R/4.2 \
  --logdir /data/oddpub_logs

# Monitor progress
jobload -u $USER
```

**Resource Requirements per Job**:
- Memory: 32 GB (`-g 32`)
- CPUs: 8 threads (`-t 8`)
- Time: 1 hour max (typically 15-20 min)
- Total: 268 jobs

### Phase 2: Merge Results

Combine all tar.gz results into a single parquet file.

**Merge Script**:
```bash
python /data/oddpub_scripts/merge_oddpub_results.py \
  /data/oddpub_output \
  /data/oddpub_results_final.parquet \
  --check-missing
```

This uses the existing `hpc_scripts/merge_oddpub_results.py` script.

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
scp extraction_tools/process_pmcoa_with_oddpub.py user@biowulf.nih.gov:/data/oddpub_scripts/
scp hpc_scripts/merge_oddpub_results.py user@biowulf.nih.gov:/data/oddpub_scripts/
scp hpc_scripts/create_oddpub_swarm.sh user@biowulf.nih.gov:/data/oddpub_scripts/
```

### 3. Create Swarm File

```bash
cd /data/oddpub_scripts
chmod +x create_oddpub_swarm.sh
bash create_oddpub_swarm.sh
```

### 4. Submit Processing Jobs

```bash
swarm -f oddpub_swarm.txt \
  -g 32 \
  -t 8 \
  --time 01:00:00 \
  --module python/3.9 R/4.2 \
  --logdir /data/oddpub_logs
```

### 5. Monitor Progress

```bash
# Check job status
jobload -u $USER

# Count completed files
ls -1 /data/oddpub_output/*.parquet | wc -l
# Should be 268 when complete

# Watch progress
watch -n 30 'ls -1 /data/oddpub_output/*.parquet | wc -l'
```

### 6. Merge Results

```bash
python merge_oddpub_results.py \
  /data/oddpub_output \
  /data/oddpub_results_final.parquet \
  --check-missing
```

### 7. Transfer Results

```bash
# From local machine
scp user@biowulf.nih.gov:/data/oddpub_results_final.parquet ~/
```

---

## Fault Tolerance

### Check for Failed Jobs

```bash
# Expected: 268 files
expected=268
completed=$(ls -1 /data/oddpub_output/*.parquet 2>/dev/null | wc -l)

echo "Progress: $completed / $expected"

if [ "$completed" -lt "$expected" ]; then
  echo "Missing $((expected - completed)) files"
fi
```

### Identify Missing tar.gz Files

```bash
# Create list of expected output files
cd /data/pmcoa
for f in *.tar.gz; do
  basename="${f%.tar.gz}_results.parquet"
  echo "$basename"
done | sort > expected_files.txt

# Create list of actual output files
cd /data/oddpub_output
ls -1 *.parquet | sort > actual_files.txt

# Find missing
comm -23 expected_files.txt actual_files.txt > missing_files.txt

cat missing_files.txt
```

### Rerun Failed Jobs

```bash
# Create retry swarm file
> oddpub_retry_swarm.txt

while read missing_file; do
  tarfile="${missing_file%_results.parquet}.tar.gz"
  output_file="/data/oddpub_output/$missing_file"
  echo "python /data/oddpub_scripts/process_pmcoa_with_oddpub.py --batch-size 500 --output-file $output_file /data/pmcoa/$tarfile"
done < missing_files.txt > oddpub_retry_swarm.txt

# Submit retry jobs
swarm -f oddpub_retry_swarm.txt \
  -g 32 -t 8 --time 01:00:00 \
  --module python/3.9 R/4.2 \
  --logdir /data/oddpub_logs
```

---

## Resource Estimation

### Total Resource Requirements

| Resource | Per Job | Total (268 jobs) |
|----------|---------|------------------|
| CPU threads | 8 | 2,144 CPU-hours |
| Memory | 32 GB | 8,576 GB-hours |
| Disk (temp) | 100 MB | 26.8 GB peak |
| Wall time | 15-20 min | With 268 nodes |

### Disk Space

**Temporary** (during processing):
- oddpub temp files: ~26 GB peak (auto-cleanup)

**Final Output**:
- Individual parquet files: ~10-20 MB each × 268 = ~3-5 GB
- Merged parquet file: ~50-100 MB (compressed)

---

## Optimization Options

### Faster Individual Jobs

Use larger batch size for oddpub:
```bash
--batch-size 1000  # Reduces number of oddpub R restarts
```

**Tradeoff**: Slightly higher memory usage, but faster processing

### More Fault Tolerance

Use shorter time limit to get faster restarts on failures:
```bash
--time 00:30:00  # 30 minutes instead of 1 hour
```

**Tradeoff**: May timeout on larger tar.gz files

### Recommended Settings

```bash
swarm -f oddpub_swarm.txt \
  -g 32 \
  -t 8 \
  --time 01:00:00 \
  --module python/3.9 R/4.2 \
  --logdir /data/oddpub_logs
```

Good balance of resources, fault tolerance, and speed.

---

## Expected Output

**Final File**: `/data/oddpub_results_final.parquet`

**Size**: ~50-100 MB (compressed parquet)

**Records**: ~6,400,000

**Schema**: 15 columns (see `data_dictionary_oddpub.csv`)
- `article`, `pmid`, `pmcid`
- `is_open_data`, `is_open_code`
- `open_data_category`, `open_data_statements`
- `is_open_data_das`, `is_open_code_cas`
- `das`, `cas`
- And more

---

## Troubleshooting

### Check Logs for Errors

```bash
cd /data/oddpub_logs

# Find error logs
grep -l "Error\|Failed\|Exception" swarm_*.e | head -20

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

**Issue**: Timeout
```
TIMEOUT: job killed after X seconds
```
**Solution**: Increase time limit or reduce batch size:
```bash
swarm -f oddpub_swarm.txt -g 32 -t 8 --time 02:00:00 ...  # 2 hours
# Or use --batch-size 250 in the python script
```

---

## Files to Transfer to HPC

From `osm-2025-12-poster-incf/`:

1. `extraction_tools/process_pmcoa_with_oddpub.py` - Main processing script
2. `hpc_scripts/merge_oddpub_results.py` - Merge results
3. `hpc_scripts/create_oddpub_swarm.sh` - Generate swarm file (optional, can create manually)

**Transfer Command**:
```bash
scp extraction_tools/process_pmcoa_with_oddpub.py \
    hpc_scripts/merge_oddpub_results.py \
    user@biowulf.nih.gov:/data/oddpub_scripts/
```

---

## Alternative: Split Large tar.gz Files

If some tar.gz files are very large and cause timeouts, you can split them:

```bash
# Process large files with multiple jobs
large_tarfile="/data/pmcoa/oa_comm_xml.PMC000xxxxxx.baseline.tar.gz"

# Create multiple output files with different batch ranges
python process_pmcoa_with_oddpub.py \
  --batch-size 500 \
  --limit-batches 0-10 \
  --output-file output_part1.parquet \
  $large_tarfile &

python process_pmcoa_with_oddpub.py \
  --batch-size 500 \
  --limit-batches 10-20 \
  --output-file output_part2.parquet \
  $large_tarfile &
```

**Note**: This requires modifying `process_pmcoa_with_oddpub.py` to add `--limit-batches` parameter.

---

## Summary

**Workflow**: tar.gz → process with oddpub → merge
**Time**: ~15-20 minutes wall time
**Jobs**: 268 parallel jobs
**Simplicity**: Uses existing validated script
**Filesystem**: No extraction bottleneck

This approach provides excellent parallelization while avoiding the severe filesystem performance issues that would result from extracting millions of files to a flat directory structure.

---

## Related Documentation

- **oddpub Usage**: `../extraction_tools/README_ODDPUB.md`
- **Data Dictionary**: `../docs/data_dictionary_oddpub.csv`
- **Implementation Summary**: `../docs/ODDPUB_IMPLEMENTATION_SUMMARY.md`
- **NIH HPC Swarm**: https://hpc.nih.gov/apps/swarm.html
