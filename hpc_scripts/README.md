# HPC Scripts for oddpub Processing

This directory contains scripts for processing 6.4M PMC XML files with oddpub on NIH HPC (Biowulf) using SLURM swarm jobs.

## Overview

**Strategy**: Parallel batch processing with 6,400 jobs

**Wall Time**: ~6-15 minutes (with sufficient compute nodes)

**CPU Time**: ~640 CPU-hours total

## Files

| File | Description |
|------|-------------|
| `create_batch_manifest.py` | Create batch manifest files from extracted XMLs |
| `process_oddpub_batch.py` | Process one batch with oddpub (run via swarm) |
| `merge_oddpub_results.py` | Merge all batch results into single file |
| `create_swarm.sh` | Generate swarm file for batch processing |
| `check_progress.sh` | Check processing progress |

## Quick Start

### 1. Transfer Scripts to HPC

```bash
# From local machine
scp -r hpc_scripts user@biowulf.nih.gov:/data/oddpub_scripts/
```

### 2. Setup on HPC

```bash
# SSH to HPC
ssh user@biowulf.nih.gov

# Load modules
module load python/3.9 R/4.2

# Install R packages (one-time)
R -e "install.packages(c('oddpub', 'future', 'furrr', 'progressr'), repos='https://cloud.r-project.org')"

# Make scripts executable
cd /data/oddpub_scripts
chmod +x *.sh *.py
```

### 3. Extract tar.gz Archives (Phase 1)

```bash
# Option A: Parallel extraction with swarm (fastest, ~5 min)
ls /data/pmcoa/*.tar.gz | \
  sed 's|.*|tar -xzf & -C /scratch/pmcoa_extracted/|' > extract_swarm.txt

swarm -f extract_swarm.txt -g 4 -t 4 --time 00:30:00

# Option B: Single job extraction (~2 hours)
sbatch --mem=16G --time=02:00:00 --wrap="
  mkdir -p /scratch/pmcoa_extracted
  cd /data/pmcoa
  for f in *.tar.gz; do tar -xzf \$f -C /scratch/pmcoa_extracted/; done
"
```

### 4. Create Batch Manifests (Phase 2)

```bash
# Create batches of 1,000 XMLs each
python create_batch_manifest.py \
  /scratch/pmcoa_extracted \
  -o /data/oddpub_batches \
  -b 1000

# Output: /data/oddpub_batches/batch_00000.txt through batch_06399.txt
```

### 5. Process with oddpub (Phase 3)

```bash
# Generate swarm file
bash create_swarm.sh /data/oddpub_batches /data/oddpub_output

# Submit swarm job (6,400 jobs)
swarm -f oddpub_swarm.txt \
  -g 16 \
  -t 4 \
  --time 00:30:00 \
  --module python/3.9 R/4.2 \
  --logdir /data/oddpub_logs

# Monitor progress
jobload -u $USER
watch -n 10 "bash check_progress.sh"
```

### 6. Merge Results (Phase 4)

```bash
# Check all batches completed
bash check_progress.sh

# Merge into single file
python merge_oddpub_results.py \
  /data/oddpub_output \
  /data/oddpub_results_final.parquet \
  --check-missing

# Transfer results back to local machine
scp user@biowulf.nih.gov:/data/oddpub_results_final.parquet ~/
```

## Resource Requirements

**Per Job (1,000 XMLs)**:
- Memory: 16 GB (`-g 16`)
- CPUs: 4 threads (`-t 4`)
- Time: 30 minutes max
- Typical: 6-10 minutes

**Total (6,400 jobs)**:
- CPU-hours: ~640
- Memory-hours: ~6,400 GB-hours
- Wall time: 6-15 minutes (with sufficient nodes)

## Troubleshooting

### Check for Failed Jobs

```bash
# Expected batch count
expected=$(ls -1 /data/oddpub_batches/batch_*.txt | wc -l)

# Completed batch count
completed=$(ls -1 /data/oddpub_output/batch_*_results.parquet | wc -l)

echo "Completed: $completed / $expected"

# Find missing batches
comm -23 \
  <(seq -f "batch_%05g_results.parquet" 0 $((expected-1)) | sort) \
  <(ls /data/oddpub_output/*.parquet | xargs -n1 basename | sort) \
  > missing_batches.txt

cat missing_batches.txt
```

### Rerun Failed Batches

```bash
# Create retry swarm file
> oddpub_retry_swarm.txt

while read batch; do
  batch_num="${batch#batch_}"
  batch_num="${batch_num%_results.parquet}"
  batch_file="/data/oddpub_batches/batch_${batch_num}.txt"
  output_file="/data/oddpub_output/batch_${batch_num}_results.parquet"
  echo "python process_oddpub_batch.py $batch_file $output_file" >> oddpub_retry_swarm.txt
done < missing_batches.txt

# Submit retry jobs
swarm -f oddpub_retry_swarm.txt \
  -g 16 -t 4 --time 00:30:00 \
  --module python/3.9 R/4.2 \
  --logdir /data/oddpub_logs
```

### Check Logs for Errors

```bash
# View logs for failed jobs
cd /data/oddpub_logs

# Find error logs
grep -l "Error\|Failed" swarm_*.e

# View specific log
less swarm_12345.e
```

## Alternative: Process tar.gz Files Directly

If you prefer not to extract tar.gz files:

```bash
# Use existing streaming script
ls /data/pmcoa/*.tar.gz | while read tarfile; do
  output="/data/oddpub_output/$(basename $tarfile .tar.gz)_results.parquet"
  echo "python /path/to/process_pmcoa_with_oddpub.py --batch-size 500 $tarfile > $output"
done > oddpub_tarfile_swarm.txt

swarm -f oddpub_tarfile_swarm.txt -g 32 -t 8 --time 01:00:00 \
  --module python/3.9 R/4.2 --logdir /data/oddpub_logs
```

**Note**: This is slower (268 jobs vs 6,400) and has uneven job sizes.

## Optimization Tips

### Faster Turnaround
- Use smaller batches (500 XMLs): 12,800 jobs, ~3 min wall time
- Trade-off: More scheduler overhead

### Fewer Jobs
- Use larger batches (2,000 XMLs): 3,200 jobs, ~12 min per job
- Trade-off: Longer individual job time

### Recommended Settings
- Batch size: 1,000 (6,400 jobs)
- Good balance of parallelization and job overhead

## Expected Output

**Final File**: `/data/oddpub_results_final.parquet`

**Size**: ~50-100 MB (compressed)

**Records**: ~6,400,000

**Schema**: 15 columns
- `article`, `pmid`, `pmcid`
- `is_open_data`, `is_open_code`
- `open_data_category`, `open_data_statements`
- `is_open_data_das`, `is_open_code_cas`
- `das`, `cas`
- And more (see `../docs/data_dictionary_oddpub.csv`)

## Additional Resources

- **Strategy Document**: `../docs/HPC_ODDPUB_STRATEGY.md`
- **oddpub Documentation**: `../extraction_tools/README_ODDPUB.md`
- **Data Dictionary**: `../docs/data_dictionary_oddpub.csv`
- **NIH HPC Documentation**: https://hpc.nih.gov/apps/swarm.html
