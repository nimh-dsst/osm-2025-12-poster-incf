# Deprecated HPC Scripts

These scripts have been superseded by `pmcid_registry.py` which provides:
- DuckDB-based tracking of all 7M PMCIDs
- Automatic detection of processed/missing PMCIDs from parquet outputs
- Efficient batch generation for retry swarms
- Queue-aware retry generation (skips already-queued jobs)

## Deprecated Scripts

| Script | Replaced By |
|--------|-------------|
| `create_oddpub_swarm.sh` | `pmcid_registry.py generate-retry` |
| `create_oddpub_swarm_container.sh` | `pmcid_registry.py generate-retry` |
| `create_oddpub_swarm_packed.sh` | `pmcid_registry.py generate-retry` |
| `generate_oddpub_swarm_extracted.sh` | `pmcid_registry.py generate-retry` |
| `generate_oddpub_swarm_extracted_packed.sh` | `pmcid_registry.py generate-retry` |
| `verify_and_retry_oddpub.sh` | `pmcid_registry.py status` + `generate-retry` |
| `verify_and_retry_oddpub_packed.sh` | `pmcid_registry.py status` + `generate-retry` |
| `verify_and_retry_oddpub_extracted.sh` | `pmcid_registry.py status` + `generate-retry` |
| `analyze_oddpub_progress.sh` | `pmcid_registry.py status` |
| `analyze_oddpub_progress_improved.sh` | `pmcid_registry.py status` |
| `generate_pmcid_chunk_mapping.py` | Built into `pmcid_registry.py` |

## New Workflow

```bash
# Check processing status
python pmcid_registry.py status

# Generate retry swarm for missing PMCIDs
python pmcid_registry.py generate-retry oddpub_v7 \
    --xml-base-dir /data/NIMH_scratch/adamt/pmcoa \
    --output-dir /data/NIMH_scratch/adamt/osm/oddpub_output \
    --container /data/adamt/containers/oddpub_optimized.sif \
    --batch-size 1000

# Submit swarm
swarm -f oddpub_retry_*.swarm -g 16 -t 2 --time 02:00:00 ...
```

## Why These Scripts Are Deprecated

1. **Inefficient**: Old scripts scanned filesystem to find missing files
2. **Error-prone**: Relied on filename parsing that broke with different naming conventions
3. **No state tracking**: Couldn't track which PMCIDs were queued vs completed
4. **Redundant work**: Would regenerate work for already-completed PMCIDs

The new `pmcid_registry.py` maintains a DuckDB database with all 7M PMCIDs and their processing status, making it trivial to generate targeted retry swarms.
