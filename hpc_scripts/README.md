# HPC Scripts for oddpub Processing with Apptainer Container

This directory contains scripts for processing ~7M PMC XML files with oddpub on NIH HPC (Biowulf) using Apptainer containers and SLURM swarm jobs.

## Overview

**Strategy**: Process baseline tar.gz files with intelligent chunking (~7,000 jobs with 1,000 XMLs per chunk)

**Scope**: Baseline files only (~7M XMLs from 39 tar.gz files)

**Performance**: 6.7 seconds per XML file (measured on HPC)

**Wall Time**: ~13 hours with 1,000 parallel nodes

**Approach**: Uses Apptainer container with all dependencies pre-installed, avoiding system library issues and ensuring reproducibility.

---

## ⚠️ IMPORTANT: Container Rebuild Required

**If you are retrying failed jobs from the initial run (2025-11-29):**

A critical timeout bug was discovered (98.4% job failure rate) and fixed. The container **must be rebuilt** before retrying jobs.

**See detailed instructions**: [`../docs/REBUILD_CONTAINER_GUIDE.md`](../docs/REBUILD_CONTAINER_GUIDE.md)

**Quick summary:**
1. Cancel running jobs: `scancel -u $USER --name=swarm`
2. Rebuild container on curium with timeout fix (600s → 3600s)
3. Deploy to HPC via helix
4. Run `verify_and_retry_oddpub.sh` to generate retry swarm
5. Submit ~7,026 retry jobs

---

## Files

| File | Description |
|------|-------------|
| `create_oddpub_swarm_container.sh` | Generate swarm file with automatic chunking (1,000 XMLs per chunk) |
| `verify_and_retry_oddpub.sh` | Verify output completeness and generate retry swarm for failed jobs |
| `verify_and_retry_oddpub_packed.sh` | Same as above but packs 8 jobs per swarm line for efficiency |
| `merge_oddpub_results.py` | Merge all chunk results into single parquet file |

## Prerequisites

### On Build Host (curium)

**Build the Apptainer container** (requires root/sudo, takes ~5 minutes):

```bash
# SSH to curium
ssh curium

# Navigate to repository
cd /data/adamt/osm-2025-12-poster-incf

# Update repository
gh repo sync --branch develop

# Build container
cd container
sudo apptainer build --force oddpub.sif oddpub.def

# Transfer to HPC (helix is the data transfer node)
scp oddpub.sif helix.nih.gov:/data/$USER/containers/
```

See `../container/README.md` for detailed build instructions and testing.

## Quick Start on HPC

### 1. Setup Repository on HPC

```bash
# SSH to HPC
ssh biowulf.nih.gov

# Navigate to repository
cd /data/adamt/osm-2025-12-poster-incf

# Update to latest version
gh repo sync --branch develop
```

### 2. Create Swarm File

```bash
cd hpc_scripts

# Generate swarm commands
bash create_oddpub_swarm_container.sh \
    /data/NIMH_scratch/licc/pmcoa/files \
    /data/NIMH_scratch/adamt/osm/osm-2025-12-poster-incf/output/ \
    /data/adamt/containers/oddpub.sif
```

**Output**: `oddpub_swarm.txt` with ~7,000 jobs

**Example output**:
```
Found 39 baseline tar.gz files
  oa_comm_xml.PMC010xxxxxx.baseline.2025-06-26: 610,792 XMLs -> 611 chunks
  oa_comm_xml.PMC000xxxxxx.baseline.2025-06-26: 3,028 XMLs -> 4 chunks
  ...

Created oddpub_swarm.txt with 7038 jobs
  Files split into chunks: 35
  Single-job files: 4
```

### 3. Submit Processing Jobs

```bash
swarm -f oddpub_swarm.txt \
    -g 32 \
    -t 8 \
    --time 03:00:00 \
    --gres=lscratch:10 \
    --module apptainer \
    --logdir /data/NIMH_scratch/adamt/osm/logs/oddpub
```

**Resource Requirements per Job**:
- Memory: 32 GB (`-g 32`)
- CPUs: 8 threads (`-t 8`)
- Time: 3 hours max (typical: 1.9 hours for 1,000 XMLs)
- Local scratch: 10 GB (`--gres=lscratch:10`) - Fast local SSD for temp files
- Module: apptainer (loads Apptainer runtime)

**Total Resources**:
- Jobs: ~7,000
- CPU-hours: ~13,300 total
- Wall time: ~13 hours with 1,000 nodes, ~26 hours with 500 nodes

**Performance Optimization**:
- Uses `/lscratch/$SLURM_JOB_ID` for temporary text files (local SSD on compute node)
- Significantly faster I/O than network storage
- Reduces load on shared filesystem
- Script automatically detects and uses lscratch when available

### 4. Monitor Progress

```bash
# Check job status
jobload -u $USER

# Count completed files
ls -1 /data/NIMH_scratch/adamt/osm/osm-2025-12-poster-incf/output/*.parquet | wc -l
# Should be ~7,000 when complete

# Watch progress
watch -n 30 'echo "Completed: $(ls -1 /data/NIMH_scratch/adamt/osm/osm-2025-12-poster-incf/output/*.parquet 2>/dev/null | wc -l) / 7000"'
```

### 4a. Verify Completeness and Generate Retry Swarm

After jobs complete (or if monitoring shows failures), use the verify scripts to check for missing outputs and generate retry commands:

```bash
# Basic verification and retry generation
bash verify_and_retry_oddpub.sh \
    /data/NIMH_scratch/licc/pmcoa/files \
    /data/NIMH_scratch/adamt/osm/osm-2025-12-poster-incf/output \
    /data/adamt/containers/oddpub.sif

# Or use packed version for better CPU efficiency (8 jobs per node)
bash verify_and_retry_oddpub_packed.sh \
    /data/NIMH_scratch/licc/pmcoa/files \
    /data/NIMH_scratch/adamt/osm/osm-2025-12-poster-incf/output \
    /data/adamt/containers/oddpub.sif
```

**What these scripts do**:
1. Check all tar.gz files for corresponding `.filelist.csv` files
2. Count expected chunks based on XML counts from CSV
3. Verify each expected output parquet file exists
4. Generate retry commands for missing outputs
5. Create swarm file with retry commands

**CSV Filename Handling**:
- The scripts correctly handle CSV filenames by stripping `.tar.gz` and adding `.filelist.csv`
- Example: `oa_comm_xml.PMC000xxxxxx.baseline.2025-06-26.tar.gz` → `oa_comm_xml.PMC000xxxxxx.baseline.2025-06-26.filelist.csv`
- This matches the optimized oddpub processor's expectations

**Output**:
- Verification summary showing success/failure rates
- `oddpub_retry_swarm.txt` or `oddpub_retry_packed_swarm.txt` with retry commands
- If all jobs succeeded, instructions for merging results

### 5. Merge Results

```bash
# Source bind paths for container
. /usr/local/current/apptainer/app_conf/sing_binds

# Merge using container
apptainer exec /data/adamt/containers/oddpub.sif \
    python3 /scripts/merge_oddpub_results.py \
    /data/NIMH_scratch/adamt/osm/osm-2025-12-poster-incf/output \
    /data/NIMH_scratch/adamt/osm/oddpub_results_final.parquet
```

### 6. Transfer Results

```bash
# From local machine (use helix for data transfer)
scp helix.nih.gov:/data/NIMH_scratch/adamt/osm/oddpub_results_final.parquet ~/
```

## Performance Estimates

**Measured Performance** (from HPC test with 10 files):
- Time per file: 6.7 seconds
- Time per 1,000-file chunk: 1.9 hours

**Parallelization Scenarios**:

| Parallel Nodes | Wall Time | Total CPU-Hours |
|----------------|-----------|-----------------|
| 1,000 nodes | 13.3 hours | 13,300 |
| 750 nodes | 17.7 hours | 13,300 |
| 500 nodes | 26.6 hours | 13,300 |
| 300 nodes | 44.3 hours | 13,300 |

**Recommendation**: Request 750-1,000 nodes for completion in <18 hours.

## Troubleshooting

### Check for Failed Jobs

```bash
# Expected: ~7,000 files
expected=7000
completed=$(ls -1 /data/NIMH_scratch/adamt/osm/osm-2025-12-poster-incf/output/*.parquet 2>/dev/null | wc -l)

echo "Progress: $completed / $expected"

if [ $completed -lt $expected ]; then
    echo "WARNING: $(($expected - $completed)) jobs may have failed"
fi
```

### Check Logs for Errors

```bash
cd /data/NIMH_scratch/adamt/osm/logs/oddpub

# Find error logs
grep -l "Error\|Failed\|Exception" swarm_*.e | head -10

# View specific log
less swarm_12345.e
```

### Common Issues

**Container not found**:
```bash
ls -lh /data/adamt/containers/oddpub.sif
# If missing, rebuild and transfer from curium
```

**Permission denied on /data**:
```bash
# Always source bind paths before running Apptainer
. /usr/local/current/apptainer/app_conf/sing_binds
```

**Out of memory**:
```bash
# Increase memory allocation
swarm -f oddpub_swarm.txt -g 64 -t 8 ...  # 64 GB instead of 32 GB
```

**Timeout** (job exceeds 3 hours):
```bash
# Increase time limit (shouldn't be necessary with 1,000 XMLs per chunk)
swarm -f oddpub_swarm.txt --time 04:00:00 ...
```

### Rerun Failed Jobs

```bash
# Find expected but missing output files
cd /data/NIMH_scratch/licc/pmcoa/files
output_dir="/data/NIMH_scratch/adamt/osm/osm-2025-12-poster-incf/output"

> /tmp/oddpub_retry_swarm.txt

for tarfile in *baseline*.tar.gz; do
    basename="${tarfile%.tar.gz}"

    # Check for chunk outputs
    if ! ls "$output_dir/${basename}_chunk"*".parquet" &>/dev/null; then
        echo "# Missing outputs for $basename"
        echo ". /usr/local/current/apptainer/app_conf/sing_binds && apptainer exec /data/adamt/containers/oddpub.sif python3 /scripts/process_pmcoa_with_oddpub.py --batch-size 500 --output-file $output_dir/${basename}_results.parquet $tarfile" >> /tmp/oddpub_retry_swarm.txt
    fi
done

# Submit retry jobs
if [ -s /tmp/oddpub_retry_swarm.txt ]; then
    swarm -f /tmp/oddpub_retry_swarm.txt \
        -g 32 -t 8 --time 03:00:00 \
        --gres=lscratch:10 \
        --module apptainer \
        --logdir /data/NIMH_scratch/adamt/osm/logs/oddpub_retry
fi
```

## Test Container Before Large Run

```bash
# Test with small file (10 XMLs, ~1 minute)
. /usr/local/current/apptainer/app_conf/sing_binds

apptainer exec /data/adamt/containers/oddpub.sif \
    python3 /scripts/process_pmcoa_with_oddpub.py \
    --batch-size 50 --max-files 10 \
    --output-file /tmp/test_oddpub.parquet \
    /data/NIMH_scratch/licc/pmcoa/files/oa_comm_xml.incr.2025-08-25.tar.gz

# Check results
python3 -c "import pandas as pd; df = pd.read_parquet('/tmp/test_oddpub.parquet'); print(f'{len(df)} results, {df.columns.tolist()}')"
```

## Expected Output

**Final File**: `/data/NIMH_scratch/adamt/osm/oddpub_results_final.parquet`

**Size**: ~100-200 MB (compressed)

**Records**: ~7,000,000 (one per XML file processed)

**Schema**: 15 columns
- `article` - Article filename
- `is_open_data` - Boolean: Open data detected
- `is_open_code` - Boolean: Open code detected
- `open_data_category` - Type of repository
- `das` - Data availability statement
- `cas` - Code availability statement
- Plus 9 more columns (see `../docs/data_dictionary_oddpub.csv`)

## Additional Resources

- **Container Documentation**: `../container/README.md`
- **Container Test Script**: `../container/test_container.sh`
- **HPC Strategy (Detailed)**: `../docs/HPC_ODDPUB_STRATEGY_CONTAINER.md`
- **oddpub Documentation**: `../docs/README_ODDPUB.md`
- **Data Dictionary**: `../docs/data_dictionary_oddpub.csv`
- **NIH HPC Swarm Guide**: https://hpc.nih.gov/apps/swarm.html
- **NIH HPC Apptainer Guide**: https://hpc.nih.gov/apps/apptainer.html
