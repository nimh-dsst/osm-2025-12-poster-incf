# Container Rebuild and Redeployment Guide

**Context**: After fixing the R script timeout (600s → 3600s), the container needs to be rebuilt and all failed jobs need to be retried.

**Status**: Timeout fix committed (2025-11-29), ready for rebuild.

---

## Step 1: Cancel Running HPC Jobs

**On HPC (biowulf.nih.gov):**

```bash
# SSH to HPC
ssh biowulf.nih.gov

# Cancel all running oddpub swarm jobs
scancel -u $USER --name=swarm

# Verify cancellation
squeue -u $USER
```

**Why cancel**: 98.4% of jobs are failing due to timeout. Letting them run wastes ~20,850 node-hours of compute resources.

---

## Step 2: Rebuild Container with Timeout Fix

**On build host (curium):**

```bash
# SSH to curium
ssh curium

# Navigate to repository
cd $HPC_CONTAINER_BASE_DIR/osm-2025-12-poster-incf

# Pull latest changes with timeout fix
gh repo sync --branch develop

# Verify timeout fix is present
grep -A 2 "timeout=" extraction_tools/process_pmcoa_with_oddpub.py | head -5
# Should show: timeout=3600  # 60 minute timeout

# Rebuild container (requires sudo, takes ~5 minutes)
cd container
sudo apptainer build --force oddpub.sif oddpub.def

# Verify build succeeded
ls -lh oddpub.sif
# Should be ~1.5-2 GB

# Test container quickly
apptainer exec oddpub.sif python3 --version
apptainer exec oddpub.sif R --version
```

**Build time**: ~5 minutes
**Output**: `oddpub.sif` (~1.5-2 GB)

---

## Step 3: Deploy Container to HPC

**Still on curium:**

```bash
# Transfer to HPC via helix (data transfer node)
scp oddpub.sif helix.nih.gov:$HPC_CONTAINER_BASE_DIR/containers/

# Verify transfer
ssh helix.nih.gov "ls -lh $HPC_CONTAINER_BASE_DIR/containers/oddpub.sif"
```

**Transfer time**: ~30 seconds (1.5 GB over 10 Gbps link)

---

## Step 4: Update Repository on HPC

**On HPC (biowulf.nih.gov):**

```bash
# Navigate to repository
cd $HPC_CONTAINER_BASE_DIR/osm-2025-12-poster-incf

# Pull latest changes
gh repo sync --branch develop

# Verify verification script is present
ls -lh hpc_scripts/verify_and_retry_oddpub.sh
```

---

## Step 5: Run Verification and Generate Retry Swarm

**On HPC:**

```bash
cd $HPC_CONTAINER_BASE_DIR/osm-2025-12-poster-incf/hpc_scripts

# Run verification script
bash verify_and_retry_oddpub.sh \
    $HPC_PMCOA_BASE_DIR/pmcoa/files \
    $HPC_BASE_DIR/osm/osm-2025-12-poster-incf/output \
    $HPC_CONTAINER_BASE_DIR/containers/oddpub.sif
```

**Expected output:**
```
Found 12 existing output files in $HPC_BASE_DIR/osm/osm-2025-12-poster-incf/output

Checking for missing outputs...
  [List of files with missing chunks]

==================== VERIFICATION SUMMARY ====================
Expected outputs:  7038
Actual outputs:    12
Missing outputs:   7026

Success rate:  0.2% (12/7038)
Failure rate:  99.8% (7026/7038)

⚠️  RETRY NEEDED

Created oddpub_retry_swarm.txt with 7026 retry jobs
```

The script automatically:
- Skips the ~12 successful outputs
- Generates retry commands for only the 7,026 missing outputs
- Uses the new container with 60-minute timeout

---

## Step 6: Submit Retry Jobs

**On HPC:**

```bash
swarm -f oddpub_retry_swarm.txt \
    -g 32 \
    -t 8 \
    --time 03:00:00 \
    --gres=lscratch:10 \
    --module apptainer \
    --logdir $HPC_BASE_DIR/osm/logs/oddpub_retry
```

**Resource allocation:**
- Memory: 32 GB per job
- CPUs: 8 threads per job
- Time: 3 hours (enough for 60-min timeout + overhead)
- Local scratch: 10 GB SSD
- Jobs: ~7,026

**Estimated completion:**
- With 1,000 parallel nodes: ~13 hours
- With 750 parallel nodes: ~18 hours
- With 500 parallel nodes: ~27 hours

---

## Step 7: Monitor Progress

**On HPC:**

```bash
# Check job status
jobload -u $USER

# Count completed files (real-time)
watch -n 30 'echo "Completed: $(ls -1 $HPC_BASE_DIR/osm/osm-2025-12-poster-incf/output/*.parquet 2>/dev/null | wc -l) / 7038"'

# Check for errors in logs
cd $HPC_BASE_DIR/osm/logs/oddpub_retry
tail -f swarm_*.e | grep -i error
```

---

## Step 8: Verify Completion

**After all jobs finish:**

```bash
cd $HPC_CONTAINER_BASE_DIR/osm-2025-12-poster-incf/hpc_scripts

# Run verification again
bash verify_and_retry_oddpub.sh \
    $HPC_PMCOA_BASE_DIR/pmcoa/files \
    $HPC_BASE_DIR/osm/osm-2025-12-poster-incf/output \
    $HPC_CONTAINER_BASE_DIR/containers/oddpub.sif
```

**Expected output if all succeeded:**
```
==================== VERIFICATION SUMMARY ====================
Expected outputs:  7038
Actual outputs:    7038
Missing outputs:   0

✅ ALL JOBS COMPLETED SUCCESSFULLY!

No retry needed. You can proceed to merge results:
  [merge commands shown]
```

---

## Step 9: Merge Results

**On HPC:**

```bash
# Source bind paths for container
. /usr/local/current/apptainer/app_conf/sing_binds

# Merge all chunk results into final parquet file
apptainer exec $HPC_CONTAINER_BASE_DIR/containers/oddpub.sif \
    python3 /scripts/merge_oddpub_results.py \
    $HPC_BASE_DIR/osm/osm-2025-12-poster-incf/output \
    $HPC_BASE_DIR/osm/oddpub_results_final.parquet
```

**Expected output:**
- File: `oddpub_results_final.parquet`
- Size: ~100-200 MB (compressed)
- Records: ~7,000,000 (one per XML)
- Columns: 15 (see `docs/data_dictionary_oddpub.csv`)

---

## Step 10: Transfer Results

**From local machine:**

```bash
# Download final results via helix
scp helix.nih.gov:$HPC_BASE_DIR/osm/oddpub_results_final.parquet ~/

# Verify download
python3 -c "import pandas as pd; df = pd.read_parquet('~/oddpub_results_final.parquet'); print(f'{len(df)} records, {len(df.columns)} columns')"
```

---

## Troubleshooting

### Container build fails

**Error**: "no space left on device"
```bash
# Set cache directory to larger disk
export APPTAINER_CACHEDIR=$HPC_CONTAINER_BASE_DIR/tmp
sudo apptainer build --force oddpub.sif oddpub.def
```

### Transfer to HPC fails

**Error**: "permission denied"
```bash
# Use helix (data transfer node) instead of biowulf
scp oddpub.sif helix.nih.gov:$HPC_CONTAINER_BASE_DIR/containers/
```

### Jobs still timing out

**Symptom**: Timeout errors in logs after rebuild

**Diagnosis**: Check container has new timeout
```bash
# On HPC
. /usr/local/current/apptainer/app_conf/sing_binds
apptainer exec $HPC_CONTAINER_BASE_DIR/containers/oddpub.sif \
    grep "timeout=" /scripts/process_pmcoa_with_oddpub.py | head -5
```

Should show `timeout=3600`, not `timeout=600`.

If still 600: Container wasn't rebuilt with latest code. Go back to Step 2.

### Verification script reports unexpected counts

**Symptom**: Expected ≠ actual by large margin

**Diagnosis**: Check CSV file counts
```bash
# Count XMLs in first tar.gz
head -20 $HPC_PMCOA_BASE_DIR/pmcoa/files/oa_comm_xml.PMC000xxxxxx.baseline.2025-06-26.filelist.csv

# Verify CSV line count matches expected
wc -l $HPC_PMCOA_BASE_DIR/pmcoa/files/*.filelist.csv
```

---

## Quick Command Reference

```bash
# On curium (build)
ssh curium
cd $HPC_CONTAINER_BASE_DIR/osm-2025-12-poster-incf && gh repo sync --branch develop
cd container && sudo apptainer build --force oddpub.sif oddpub.def
scp oddpub.sif helix.nih.gov:$HPC_CONTAINER_BASE_DIR/containers/

# On HPC (deploy and run)
ssh biowulf.nih.gov
scancel -u $USER --name=swarm
cd $HPC_CONTAINER_BASE_DIR/osm-2025-12-poster-incf && gh repo sync --branch develop
cd hpc_scripts
bash verify_and_retry_oddpub.sh $HPC_PMCOA_BASE_DIR/pmcoa/files $HPC_BASE_DIR/osm/osm-2025-12-poster-incf/output $HPC_CONTAINER_BASE_DIR/containers/oddpub.sif
swarm -f oddpub_retry_swarm.txt -g 32 -t 8 --time 03:00:00 --gres=lscratch:10 --module apptainer --logdir $HPC_BASE_DIR/osm/logs/oddpub_retry
```

---

## Timeline

**Estimated total time**: 14-20 hours

| Step | Duration |
|------|----------|
| Cancel jobs | 1 minute |
| Rebuild container | 5 minutes |
| Deploy to HPC | 1 minute |
| Verification | 2 minutes |
| Submit retry | 1 minute |
| **Processing (750 nodes)** | **~18 hours** |
| Merge results | 5 minutes |
| Transfer results | 1 minute |

**Recommendation**: Start rebuild in morning, submit jobs by 10 AM, complete by next morning.

---

## Related Documentation

- Container Definition: `../container/oddpub.def`
- Container README: `../container/README.md`
- HPC Strategy: `HPC_ODDPUB_STRATEGY_CONTAINER.md`
- HPC Scripts README: `../hpc_scripts/README.md`
- Data Dictionary: `data_dictionary_oddpub.csv`
