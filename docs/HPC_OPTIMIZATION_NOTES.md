# HPC Optimization Notes

## CPU Utilization Issue (2025-11-29)

### Problem Discovered

During the oddpub processing run, `jobload` showed poor CPU utilization:
- Allocated: 8 CPUs per job (`-t 8`)
- Used: 1-4 CPUs (12-50% utilization)
- Root cause: oddpub R script is single-threaded

Example from jobload output:
```
JOBID            NODES  CPUS  THREADS   LOAD       MEMORY
5577070_999      cn3536     8        1    12%     0.4 /   32.0 GB
5577070_0        cn4286     8        4    50%     0.9 /   32.0 GB
```

### Solution: Job Packing

NIH HPC supports running multiple commands per subjob to better utilize allocated CPUs. Since oddpub is single-threaded, we can pack 8 jobs per node.

**Created packed swarm scripts:**
- `hpc_scripts/create_oddpub_swarm_packed.sh`
- `hpc_scripts/verify_and_retry_oddpub_packed.sh`

These scripts generate swarm files with 8 commands per line:
```bash
cmd1 & cmd2 & cmd3 & cmd4 & cmd5 & cmd6 & cmd7 & cmd8 & wait
```

### Benefits of Packing

1. **CPU Efficiency**: 100% utilization vs 12-50%
2. **Throughput**: Process 8x more files with same allocation
3. **Queue Time**: Fewer subjobs = faster queue movement
4. **Cost**: Better fairshare usage

### When to Use Each Approach

**Standard swarm (1 job per line):**
- Multi-threaded applications
- Jobs with variable runtime
- Debugging/testing
- When CPU efficiency isn't critical

**Packed swarm (8 jobs per line):**
- Single-threaded applications
- Uniform job runtime
- Production runs
- Large-scale processing

### Current Run Performance

Despite suboptimal CPU usage, the current run is performing well:
- Processing rate: ~1,274 jobs/hour
- Completion ETA: ~4.2 hours (vs 18 hours estimated)
- Zero errors with timeout fix

Decision: Let current run complete, use packing for future runs.

### Other HPC Optimizations

1. **Use /lscratch for I/O** (already implemented)
   - Local SSD faster than network filesystem
   - Reduces network congestion
   - `--gres=lscratch:10` allocates 10 GB

2. **Batch size tuning**
   - Current: 500 files per oddpub batch
   - Balances memory usage vs R startup overhead
   - Fits within 60-minute timeout

3. **Chunk size optimization**
   - Current: 1,000 XMLs per chunk
   - ~1.9 hours processing time
   - Good fit for 3-hour walltime limit

### Future Improvements

1. **Profiling**: Identify what limits oddpub to 1-4 threads
2. **Container optimization**: Pre-compile R packages
3. **Parallel R**: Investigate furrr/future usage in oddpub
4. **Memory tuning**: Reduce from 32 GB if not needed

### References

- NIH HPC swarm documentation: https://hpc.nih.gov/apps/swarm.html
- Job packing examples: https://hpc.nih.gov/apps/swarm.html#pack
- CPU architecture: https://hpc.nih.gov/systems/hardware.html