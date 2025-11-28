# HPC Scripts for oddpub Processing

This directory contains scripts for processing 6.4M PMC XML files with oddpub on NIH HPC (Biowulf) using SLURM swarm jobs.

## Overview

**Strategy**: Process baseline tar.gz files with intelligent chunking (2,346 jobs)

**Scope**: Baseline files only (~7M XMLs from 39 tar.gz files)

**Wall Time**: ~13-14 hours (with 2,346 parallel nodes)

**Approach**: Uses existing `process_pmcoa_with_oddpub.py` script with automatic chunking to keep each job under 14 hours, avoiding filesystem bottlenecks from extracting millions of files.

## Files

| File | Description |
|------|-------------|
| `create_oddpub_swarm.sh` | Generate swarm file with automatic chunking (baseline files only) |
| `merge_oddpub_results.py` | Merge all chunk results into single parquet file |

**Critical Finding**: oddpub processes at ~16 seconds per XML file. Without chunking, the largest file (610k XMLs) would take 79 days to process! The swarm script automatically splits large files into 3,000-XML chunks (~13 hours each) to make processing practical.

## Quick Start

### 1. Transfer Scripts to HPC

```bash
# From local machine
cd osm-2025-12-poster-incf

scp extraction_tools/process_pmcoa_with_oddpub.py \
    extraction_tools/renv.lock \
    extraction_tools/.Rprofile \
    hpc_scripts/merge_oddpub_results.py \
    hpc_scripts/create_oddpub_swarm.sh \
    user@biowulf.nih.gov:/data/oddpub_scripts/
```

### 2. Setup on HPC

**Using renv (recommended for reproducibility)**:

```bash
# SSH to HPC
ssh user@biowulf.nih.gov

# Load modules (poppler needed for pdftools)
module load python/3.9 R/4.2 poppler

# Create user library directory (required on NIH HPC)
mkdir -p /data/$USER/R/rhel8/4.2

# Install Python packages
pip install --user pandas pyarrow

# Navigate to project directory
cd /data/oddpub_scripts

# Initialize renv and install R packages from renv.lock
R -e "install.packages('renv', repos='https://cloud.r-project.org'); renv::init(bare=TRUE); renv::restore()"

# Verify R packages
R -e "library(oddpub); library(future); library(furrr); library(progressr); cat('All packages loaded successfully\n')"

# Make scripts executable
chmod +x *.sh *.py
```

**Alternative: Manual installation** (if renv fails):

```bash
# Load modules
module load python/3.9 R/4.2 poppler

# Create user library directory
mkdir -p /data/$USER/R/rhel8/4.2

# Install R packages manually
R -e "install.packages(c('devtools', 'future', 'furrr', 'progressr'), repos='https://cloud.r-project.org'); devtools::install_github('quest-bih/oddpub')"

# Install Python packages
pip install --user pandas pyarrow

# Make scripts executable
cd /data/oddpub_scripts
chmod +x *.sh *.py
```

### 3. Create Swarm File

```bash
bash create_oddpub_swarm.sh /data/pmcoa /data/oddpub_output /data/oddpub_scripts
```

This creates `oddpub_swarm.txt` with 268 jobs (one per tar.gz file).

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
watch -n 30 'echo "Completed: $(ls -1 /data/oddpub_output/*.parquet 2>/dev/null | wc -l) / 268"'
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

## Resource Requirements

**Per Job** (processing one tar.gz file):
- Memory: 32 GB (`-g 32`)
- CPUs: 8 threads (`-t 8`)
- Time: 1 hour max (typically 15-20 min)

**Total** (268 jobs):
- CPU-hours: ~65
- Wall time: 15-20 minutes (with 268 nodes)

## Troubleshooting

### Check for Failed Jobs

```bash
# Expected: 268 files
expected=268
completed=$(ls -1 /data/oddpub_output/*.parquet 2>/dev/null | wc -l)

echo "Progress: $completed / $expected"
```

### Identify Missing Files

```bash
# Create list of expected outputs
cd /data/pmcoa
for f in *.tar.gz; do echo "${f%.tar.gz}_results.parquet"; done | sort > /tmp/expected.txt

# Create list of actual outputs
cd /data/oddpub_output
ls -1 *.parquet | sort > /tmp/actual.txt

# Find missing
comm -23 /tmp/expected.txt /tmp/actual.txt > /tmp/missing.txt

echo "Missing $(wc -l < /tmp/missing.txt) files:"
head -20 /tmp/missing.txt
```

### Rerun Failed Jobs

```bash
# Create retry swarm file
> oddpub_retry_swarm.txt

while read missing_file; do
  tarfile="/data/pmcoa/${missing_file%_results.parquet}.tar.gz"
  output_file="/data/oddpub_output/$missing_file"
  echo "python /data/oddpub_scripts/process_pmcoa_with_oddpub.py --batch-size 500 --output-file $output_file $tarfile"
done < /tmp/missing.txt > oddpub_retry_swarm.txt

# Submit retry jobs
swarm -f oddpub_retry_swarm.txt \
  -g 32 -t 8 --time 01:00:00 \
  --module python/3.9 R/4.2 \
  --logdir /data/oddpub_logs
```

### Check Logs for Errors

```bash
cd /data/oddpub_logs

# Find error logs
grep -l "Error\|Failed\|Exception" swarm_*.e | head -10

# View specific log
less swarm_12345.e
```

## Common Issues

**R package not found**:
```bash
module load R/4.2
R -e "install.packages('oddpub', repos='https://cloud.r-project.org')"
```

**Out of memory**:
```bash
# Increase memory allocation
swarm -f oddpub_swarm.txt -g 64 -t 8 ...  # 64 GB instead of 32 GB
```

**Timeout**:
```bash
# Increase time limit
swarm -f oddpub_swarm.txt -g 32 -t 8 --time 02:00:00 ...  # 2 hours
```

## Expected Output

**Final File**: `/data/oddpub_results_final.parquet`

**Size**: ~50-100 MB (compressed)

**Records**: ~6,400,000

**Schema**: 15 columns (see `../docs/data_dictionary_oddpub.csv`)

## Additional Resources

- **Strategy Document**: `../docs/HPC_ODDPUB_STRATEGY.md`
- **oddpub Documentation**: `../extraction_tools/README_ODDPUB.md`
- **Data Dictionary**: `../docs/data_dictionary_oddpub.csv`
- **NIH HPC Documentation**: https://hpc.nih.gov/apps/swarm.html
