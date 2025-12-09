# Fresh Conversation Start: HPC Container-Based XML Metadata Extractor

## Context for New Conversation

You need to design and implement a container-based XML metadata extractor for HPC processing of ~7M PMC XML files. This extractor will enhance the existing metadata with journal and publisher information.

## Current Infrastructure

### Hosts and Access Patterns

**osm2025** (current EC2 server):
- Where Claude is running
- Can push to GitHub
- Can be accessed by other hosts for pulling files
- Has test data in `$EC2_PROJ_BASE_DIR/pmcoaXMLs/raw_download/`

**Behind firewall (can only pull, not be pushed to):**

**curium**:
- Apptainer build host (has sudo)
- Can pull from: osm2025, GitHub
- Repository location: `$HPC_CONTAINER_BASE_DIR/osm-2025-12-poster-incf/`
- Update command: `gh repo sync --branch develop`

**helix**:
- HPC file transfer server
- Shares filesystem with biowulf
- Used for: `scp` transfers to/from HPC

**biowulf head node**:
- Job submission
- No heavy computation
- Can pull from: osm2025, GitHub

**biowulf compute nodes** (e.g., cn2882):
- Where jobs actually run
- Interactive testing via `sinteractive`
- Can pull from: osm2025 (via helix)

## Requirements

### Functional Requirements

1. **Extract additional XML fields**:
   - `journal-id` (multiple types: nlm-ta, iso-abbrev, publisher-id)
   - `publisher-name`
   - `publisher-loc`
   - Article type/subtype
   - Any other fields useful for analysis

2. **Performance requirements**:
   - Process ~7M files efficiently on HPC
   - Match or exceed current Python extractor speed (1,125 files/sec streaming)
   - Handle tar.gz archives directly (no extraction needed)

3. **Integration**:
   - Output must merge seamlessly with existing 122-column schema
   - Maintain PMCID as join key
   - Add new columns without breaking existing pipeline

### Technical Requirements

1. **Container-based**:
   - Use Apptainer (Singularity) for HPC compatibility
   - All dependencies bundled
   - Python-based for consistency with existing tools

2. **Testing at each stage**:
   - Local testing on osm2025
   - Container build testing on curium
   - Small-scale testing on interactive node
   - Full swarm testing on HPC

3. **Robust error handling**:
   - Handle malformed XML gracefully
   - Log errors without failing entire batch
   - Validate output format

## Existing Code to Reference

### Current Extractor
Location: `rtransparent/extract-XML-metadata/extract_from_tarballs.py`

Key features:
- Streams tar.gz files without extraction
- Extracts 18 populated fields + 102 placeholder columns
- Outputs parquet format
- 1,125 files/sec performance

### Data Dictionary
Location: `osm-2025-12-poster-incf/docs/data_dictionary_rtrans.csv`

Current schema (122 columns) - new extractor must be compatible.

### Test Data

Small test file for quick validation:
```
$EC2_PROJ_BASE_DIR/pmcoaXMLs/raw_download/oa_other_xml.incr.2025-07-03.tar.gz
```

Contains ~10 XML files, perfect for testing.

## Testing Strategy

### 1. Local Development Testing (osm2025)

```bash
# Test extraction logic
python new_xml_extractor.py --test-mode test.tar.gz

# Validate output schema
python -c "import pandas as pd; df = pd.read_parquet('test_output.parquet'); print(df.columns)"

# Compare with existing extractor
python extract_from_tarballs.py --limit 10 test.tar.gz -o baseline.parquet
```

### 2. Container Build Testing (curium)

```bash
# Create test script that curium can pull
cat > test_container_build.sh << 'EOF'
#!/bin/bash
# This script will be pulled and run on curium

# Pull latest code
cd $HPC_CONTAINER_BASE_DIR/osm-2025-12-poster-incf
gh repo sync --branch develop

# Build container
cd container
sudo apptainer build --force xml_extractor.sif xml_extractor.def

# Test container
apptainer exec xml_extractor.sif python3 --version
apptainer exec xml_extractor.sif python3 -c "import pandas, pyarrow, lxml"

# Run extraction test
wget https://osm2025/test_data/small.tar.gz -O /tmp/test.tar.gz
apptainer exec xml_extractor.sif python3 /scripts/new_xml_extractor.py /tmp/test.tar.gz
EOF

# Make available for pulling
scp test_container_build.sh osm2025:~/public/
```

### 3. HPC Testing (interactive node)

```bash
# Get interactive node
sinteractive --gres=lscratch:10

# Pull and test container
cd /lscratch/$SLURM_JOB_ID
scp helix:$HPC_CONTAINER_BASE_DIR/containers/xml_extractor.sif .

# Test with real data
. /usr/local/current/apptainer/app_conf/sing_binds
apptainer exec xml_extractor.sif python3 /scripts/new_xml_extractor.py \
  $HPC_PMCOA_BASE_DIR/pmcoa/files/oa_other_xml.incr.2025-07-03.tar.gz

# Validate output
python3 -c "import pandas as pd; df = pd.read_parquet('output.parquet'); print(f'{len(df)} records, {len(df.columns)} columns')"
```

### 4. Swarm Testing

Create minimal swarm for testing:
```bash
# Create 10-job test swarm
for i in {0..9}; do
  echo ". /usr/local/current/apptainer/app_conf/sing_binds && apptainer exec $HPC_CONTAINER_BASE_DIR/containers/xml_extractor.sif python3 /scripts/new_xml_extractor.py --chunk $i $HPC_PMCOA_BASE_DIR/pmcoa/files/test.tar.gz"
done > test_swarm.txt

# Submit small test
swarm -f test_swarm.txt -g 16 -t 2 --time 00:30:00
```

## Deliverables

1. **XML extractor script** (`new_xml_extractor.py`):
   - Extracts journal-id, publisher-name, etc.
   - Compatible with existing 122-column schema
   - Streaming tar.gz support

2. **Apptainer definition** (`xml_extractor.def`):
   - Based on tested Python image
   - Includes all dependencies
   - Optimized for HPC

3. **Testing documentation** showing:
   - Local test results
   - Container build success
   - Interactive node validation
   - Swarm test completion

4. **Integration plan**:
   - How to merge with existing pipeline
   - Schema changes needed
   - Performance benchmarks

## Key Lessons from Previous Work

1. **Always test locally first** before moving to HPC
2. **Use streaming for tar.gz files** - don't extract to disk
3. **Build containers on curium** (has sudo), not locally
4. **Test pull access** from each host before assuming it works
5. **Use timestamp-based log directories** for better tracking
6. **Verify output at each stage** before scaling up

## Starting the Conversation

Begin with: "I need to design an HPC container-based XML metadata extractor that extracts journal and publisher fields from PMC XML files. I have test data at $EC2_PROJ_BASE_DIR/pmcoaXMLs/raw_download/oa_other_xml.incr.2025-07-03.tar.gz and existing extractor code to reference. The solution must be tested progressively across multiple hosts with specific access restrictions."

Include this document as context.