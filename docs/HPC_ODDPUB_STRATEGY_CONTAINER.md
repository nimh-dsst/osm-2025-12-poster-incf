# HPC Strategy for oddpub Processing with Apptainer Container

**Date**: 2025-11-28 (Revised - Container approach)
**Target**: Process ~7M PMC baseline XML files with oddpub on NIH HPC
**HPC System**: SLURM with swarm wrapper, using Apptainer containers
**Status**: ⚠️ **Container approach - resolves system library dependency issues**

## Executive Summary

**Recommended Approach**: Process baseline tar.gz files with chunking using Apptainer container (2,346 jobs)
**Container Benefits**: Bundles all system dependencies (ICU, poppler, libxml2), eliminating installation issues
**Scope**: Baseline files only (~7M XMLs from 39 tar.gz files)
**Estimated Wall Time**: ~13-14 hours with 2,346 parallel nodes
**Estimated CPU Time**: ~31,500 CPU-hours total (~1,311 CPU-days)

## Why Use a Container?

The standard approach (renv for R, pip/uv for Python) fails on NIH HPC due to system library issues:
- `stringi` requires ICU (libicui18n.so.73) which may not be in library path
- `pdftools` requires poppler-cpp system libraries
- Other R packages have similar C++ library dependencies

**Container Solution**:
- ✅ Bundles ALL system dependencies
- ✅ Build once, run anywhere
- ✅ Complete reproducibility
- ✅ No installation hassles on HPC
- ✅ Apptainer is HPC-native (no Docker daemon needed)

## Two-Phase Workflow

### Phase 1: Build Container (Local Machine)

**Prerequisites**: Linux machine with root/sudo access (your laptop, VM, or cloud instance)

```bash
# On local Linux machine
cd osm-2025-12-poster-incf/container

# Build container (requires root)
sudo apptainer build oddpub.sif oddpub.def

# This takes 10-20 minutes and creates a ~1.5-2 GB .sif file
```

**Alternative: Cloud VM Build**:
```bash
# Launch AWS EC2 Ubuntu instance
# SSH to instance
sudo apt-get update && sudo apt-get install -y apptainer

git clone https://github.com/YOUR_ORG/osm-2025-12-poster-incf.git
cd osm-2025-12-poster-incf/container
sudo apptainer build oddpub.sif oddpub.def

# Download to local machine, then transfer to HPC
```

**Alternative: Remote Build** (if you have Sylabs account):
```bash
apptainer remote login  # Get token from https://cloud.sylabs.io/
apptainer build --remote oddpub.sif oddpub.def
```

### Phase 2: Process on HPC

**Step 1: Setup on HPC (One-Time)**

```bash
# SSH to HPC
ssh user@biowulf.nih.gov

# Create container directory
mkdir -p /data/$USER/containers

# Transfer container from local machine
# (from your local terminal)
scp oddpub.sif user@biowulf.nih.gov:/data/$USER/containers/

# Clone repository on HPC
cd /data
git clone https://github.com/YOUR_ORG/osm-2025-12-poster-incf.git oddpub_scripts
```

**Step 2: Test Container on HPC**

```bash
# Source bind paths (required to access /data filesystem)
. /usr/local/current/apptainer/app_conf/sing_binds

# Test container
apptainer exec /data/$USER/containers/oddpub.sif python3 --version
apptainer exec /data/$USER/containers/oddpub.sif R --version

# Test packages
apptainer exec /data/$USER/containers/oddpub.sif \
    python3 -c "import pandas; import pyarrow; print('Python OK')"

apptainer exec /data/$USER/containers/oddpub.sif \
    R -e "library(oddpub); library(future); cat('R OK\n')"
```

**Step 3: Create Swarm File**

```bash
cd /data/oddpub_scripts/hpc_scripts
chmod +x create_oddpub_swarm_container.sh

bash create_oddpub_swarm_container.sh \
    /data/pmcoa \
    /data/oddpub_output \
    /data/$USER/containers/oddpub.sif
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

**Step 4: Submit Processing Jobs**

```bash
swarm -f oddpub_swarm.txt \
    -g 32 \
    -t 8 \
    --time 14:00:00 \
    --module apptainer \
    --logdir /data/oddpub_logs
```

**Resource Requirements per Job**:
- Memory: 32 GB (`-g 32`)
- CPUs: 8 threads (`-t 8`)
- Time: 14 hours max (typical: 9-13 hours)
- Module: apptainer (loads Apptainer runtime)

**Step 5: Monitor Progress**

```bash
# Check job status
jobload -u $USER

# Count completed files
ls -1 /data/oddpub_output/*.parquet | wc -l
# Should be 2,346 when complete

# Watch progress
watch -n 30 'echo "Completed: $(ls -1 /data/oddpub_output/*.parquet 2>/dev/null | wc -l) / 2346"'
```

**Step 6: Merge Results**

```bash
# Source bind paths
. /usr/local/current/apptainer/app_conf/sing_binds

# Merge using container
apptainer exec /data/$USER/containers/oddpub.sif \
    python3 /scripts/merge_oddpub_results.py \
    /data/oddpub_output \
    /data/oddpub_results_final.parquet
```

**Step 7: Transfer Results**

```bash
# From local machine
scp user@biowulf.nih.gov:/data/oddpub_results_final.parquet ~/
```

---

## Container Contents

The `oddpub.sif` container includes:

**Base Image**: rocker/r-ver:4.2.0 (Debian-based)

**System Libraries**:
- ICU (International Components for Unicode)
- libxml2 (XML parsing)
- libpoppler-cpp (PDF processing)
- libssl, libcurl (networking)
- All C++ dependencies

**Python Environment**:
- Python 3.9+
- pandas 1.3.0+
- pyarrow 6.0.0+

**R Environment**:
- R 4.2.0
- oddpub 7.2.3
- future, furrr, progressr

**Scripts** (embedded at build time):
- `/scripts/process_pmcoa_with_oddpub.py`
- `/scripts/merge_oddpub_results.py`

---

## Updating the Container

When scripts or packages change:

```bash
# On local machine
cd osm-2025-12-poster-incf
git pull

# Rebuild container
cd container
sudo apptainer build oddpub_v2.sif oddpub.def

# Transfer to HPC
scp oddpub_v2.sif user@biowulf.nih.gov:/data/$USER/containers/
```

---

## Troubleshooting

### Container Build Issues

**Error: "permission denied"**
- Building requires root/sudo access
- Use cloud VM or remote build option

**Error: "no space left on device"**
- Set cache directory: `export APPTAINER_CACHEDIR=/path/to/large/disk`

### HPC Runtime Issues

**Error: "cannot access /data"**
```bash
# Always source bind paths before running
. /usr/local/current/apptainer/app_conf/sing_binds
```

**Error: "container not found"**
- Check container path: `ls -lh /data/$USER/containers/oddpub.sif`
- Verify transfer completed

**Error: "Python package not found"**
- Rebuild container (packages are baked in at build time)
- Cannot install packages at runtime in read-only container

### Job Failures

Check logs for errors:
```bash
cd /data/oddpub_logs
grep -l "Error\\|Failed" swarm_*.e | head -10
less swarm_12345.e  # View specific error
```

Common issues:
- Out of memory: Increase `-g 64` in swarm command
- Timeout: Increase `--time 16:00:00`
- Bind paths not set: Source `sing_binds` file

---

## Performance Characteristics

Same as non-container approach:

**Processing Speed**: ~16 seconds per XML file
**Chunk Size**: 3,000 XMLs = ~13 hours processing time
**Total Jobs**: 2,346 (35 files chunked, 4 single-job)
**Wall Time**: ~13-14 hours with full parallelization
**CPU Time**: ~31,500 CPU-hours total

---

## Comparison: Container vs Native Installation

| Aspect | Native (renv/uv) | **Container (Apptainer)** |
|--------|------------------|---------------------------|
| Setup complexity | High (many steps) | **Low (build once, run anywhere)** |
| System dependencies | ❌ Manual troubleshooting | **✅ Pre-bundled** |
| Reproducibility | Partial (system libs vary) | **✅ Complete** |
| HPC compatibility | ❌ Library issues common | **✅ Designed for HPC** |
| Updates | Reinstall packages | **Rebuild container** |
| Debugging | Complex | **Simpler (isolated)** |

**Recommendation**: Use containers for production HPC workflows.

---

## Related Documentation

- **Container README**: `../container/README.md`
- **Apptainer Definition**: `../container/oddpub.def`
- **NIH HPC Apptainer Guide**: https://hpc.nih.gov/apps/apptainer.html
- **Data Dictionary**: `data_dictionary_oddpub.csv`
- **oddpub Documentation**: `../extraction_tools/README_ODDPUB.md`
