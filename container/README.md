# Apptainer Container for oddpub HPC Processing

This directory contains the Apptainer definition file for creating a reproducible environment with all dependencies for oddpub processing on NIH HPC.

## Why Use a Container?

- **Solves system library issues**: Bundles ICU, poppler, and all other system dependencies
- **Complete reproducibility**: Identical environment across all systems
- **No installation hassles**: Pre-built container has everything configured
- **HPC-compatible**: Apptainer is designed for HPC and supported on NIH Biowulf

## Container Contents

- **Base**: rocker/r-ver:4.2.0 (Debian-based with R 4.2.0)
- **System libraries**: ICU, libxml2, poppler-cpp, SSL, curl
- **Python**: Python 3.9+ with pandas and pyarrow
- **R packages**: oddpub 7.2.3, future, furrr, progressr
- **Scripts**: process_pmcoa_with_oddpub.py and merge_oddpub_results.py

## Building the Container

**Option 1: Build Locally** (requires root/sudo on Linux system)

```bash
# On your local Linux machine or VM
cd osm-2025-12-poster-incf/container

# Build container
sudo apptainer build oddpub.sif oddpub.def

# Transfer to HPC
scp oddpub.sif user@biowulf.nih.gov:/data/$USER/containers/
```

**Option 2: Build on Cloud VM** (AWS, GCP, Azure)

```bash
# Launch Ubuntu VM with root access
# SSH to VM
sudo apt-get update && sudo apt-get install -y apptainer

# Clone repo
git clone https://github.com/YOUR_ORG/osm-2025-12-poster-incf.git
cd osm-2025-12-poster-incf/container

# Build
sudo apptainer build oddpub.sif oddpub.def

# Download to local machine, then scp to HPC
```

**Option 3: Use Apptainer Remote Build** (requires Sylabs account)

```bash
# Get token from https://cloud.sylabs.io/
apptainer remote login
apptainer build --remote oddpub.sif oddpub.def
```

## Testing the Container Locally

```bash
# Test container works
apptainer exec oddpub.sif python3 --version
apptainer exec oddpub.sif R --version

# Test Python packages
apptainer exec oddpub.sif python3 -c "import pandas; import pyarrow; print('OK')"

# Test R packages
apptainer exec oddpub.sif R -e "library(oddpub); library(future); cat('OK\n')"
```

## Using on NIH HPC

### Setup (One-Time)

```bash
# SSH to HPC
ssh user@biowulf.nih.gov

# Create container directory
mkdir -p /data/$USER/containers

# Transfer container (from local machine)
# scp oddpub.sif user@biowulf.nih.gov:/data/$USER/containers/

# Clone repository
cd /data
git clone https://github.com/YOUR_ORG/osm-2025-12-poster-incf.git oddpub_scripts
```

### Create Swarm File

```bash
cd /data/oddpub_scripts/hpc_scripts

# Generate swarm file with container commands
bash create_oddpub_swarm_container.sh \
    /data/pmcoa \
    /data/oddpub_output \
    /data/$USER/containers/oddpub.sif
```

### Submit Processing Jobs

```bash
# Source HPC bind paths
. /usr/local/current/apptainer/app_conf/sing_binds

# Submit swarm
swarm -f oddpub_swarm.txt \
    -g 32 \
    -t 8 \
    --time 14:00:00 \
    --module apptainer \
    --logdir /data/oddpub_logs
```

### Monitor and Merge

```bash
# Monitor progress
watch -n 30 'ls -1 /data/oddpub_output/*.parquet | wc -l'

# Merge results when complete
. /usr/local/current/apptainer/app_conf/sing_binds
apptainer exec /data/$USER/containers/oddpub.sif \
    python3 /scripts/merge_oddpub_results.py \
    /data/oddpub_output \
    /data/oddpub_results_final.parquet
```

## Updating the Container

When scripts or package versions change:

```bash
# On local machine: rebuild container
cd osm-2025-12-poster-incf
git pull
cd container
sudo apptainer build oddpub_v2.sif oddpub.def

# Transfer new version to HPC
scp oddpub_v2.sif user@biowulf.nih.gov:/data/$USER/containers/
```

## Troubleshooting

**Python type hint error "type object is not subscriptable"**:
- Fixed in latest version (2025-11-28)
- Container uses Python 3.9 which requires `Tuple[...]` instead of `tuple[...]`
- Rebuild container to get the fix

**Build fails with "permission denied"**:
- You need root/sudo access to build
- Use Option 2 (cloud VM) or Option 3 (remote build)

**Container can't access /data on HPC**:
```bash
# Always source bind paths before running
. /usr/local/current/apptainer/app_conf/sing_binds
```

**Out of space during build**:
```bash
# Set cache directory
export APPTAINER_CACHEDIR=/path/to/large/disk
```

**Need to inspect container**:
```bash
# Enter container interactively
apptainer shell oddpub.sif
```

## Container Size

Expected size: ~1.5-2 GB (includes R, Python, and all dependencies)

## Related Documentation

- **NIH HPC Apptainer Guide**: https://hpc.nih.gov/apps/apptainer.html
- **HPC Processing Strategy**: `../docs/HPC_ODDPUB_STRATEGY.md`
- **Apptainer Documentation**: https://apptainer.org/docs/
