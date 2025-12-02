# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This repository contains analysis code for the INCF 2025 conference poster on research transparency metrics. It processes ~6.5M biomedical research articles from PubMed Central Open Access to analyze open data/code sharing, funding sources, COI declarations, and trial registration.

## Project Structure

```
osm-2025-12-poster-incf/
├── extraction_tools/           # XML metadata extraction (Python)
├── funder_analysis/           # Funder mapping and analysis (Python)
├── analysis/                  # Poster analysis scripts
├── notebooks/                 # Jupyter notebooks
├── docs/                      # Documentation and data dictionary
├── results/                   # Small summary files only
└── create_compact_rtrans.py   # Main data processing script
```

## Quick Start

### Setup Python Environment

```bash
# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# For Jupyter notebooks and interactive analysis
pip install -r requirements-dev.txt
```

**Important:** The compact dataset generation and analysis scripts have already been run on the full dataset (1,647 files). Results are in:
- `~/claude/pmcoaXMLs/compact_rtrans/` - Compact parquet files (142 columns, 1.89M funder matches)
- `results/funder_data_sharing_full_*.{csv,png}` - Full analysis outputs

### Extract Metadata from PMC Archives

```bash
cd extraction_tools

# Fast streaming extraction from tar.gz archives (recommended)
python extract_from_tarballs.py \
  -f parquet \
  -o baseline_metadata.parquet \
  /path/to/pmc/archives/

# File-based extraction (slower, 229 files/sec vs 1,125 files/sec)
python extract_xml_metadata.py /path/to/xmls/ -o output.parquet -f parquet
```

### Process PMC XMLs with oddpub

oddpub R package detects open data and open code availability statements:

```bash
cd extraction_tools

# Test run (limited files)
python process_pmcoa_with_oddpub.py \
  --pattern "oa_comm_xml.PMC012*.tar.gz" \
  --batch-size 50 \
  --max-files 20 \
  ~/claude/pmcoaXMLs/raw_download/

# Full production run (for HPC)
python process_pmcoa_with_oddpub.py \
  --batch-size 500 \
  ~/claude/pmcoaXMLs/raw_download/
```

See `extraction_tools/README_ODDPUB.md` for complete documentation.

### Create Compact Analysis Dataset

The main processing script combines rtransparent R package output with metadata and funder matching:

```bash
# Full run (completed 2025-11-26)
# Runtime: 21m 11s
# Output: 1,647 files, 6.55M records, 1.89M funder matches
python create_compact_rtrans.py \
  --input-dir ~/claude/pmcoaXMLs/rtrans_out_full_parquets \
  --metadata-dir ~/claude/pmcoaXMLs/extracted_metadata_parquet \
  --output-dir ~/claude/pmcoaXMLs/compact_rtrans \
  --overwrite

# Test with limited files
python create_compact_rtrans.py \
  --input-dir /path/to/rtrans \
  --metadata-dir /path/to/metadata \
  --output-dir /path/to/output \
  --limit 10
```

**Critical Bug Fix (2025-11-26):** Funder matching must occur BEFORE field filtering, otherwise long funding text columns are removed before pattern search can execute.

### Analyze Data Sharing Trends by Funder

```bash
# Full dataset analysis (memory-efficient batch processing)
# Runtime: ~5 minutes
# Output: 3 CSVs + 3 PNGs (counts, totals, percentages)
python analysis/funder_data_sharing_trends.py \
  --input-dir ~/claude/pmcoaXMLs/compact_rtrans \
  --output-prefix results/funder_data_sharing_full

# Analyze code sharing instead of data sharing
python analysis/funder_data_sharing_trends.py \
  --input-dir ~/claude/pmcoaXMLs/compact_rtrans \
  --output-prefix results/funder_code_sharing_full \
  --metric is_open_code

# Test with limited files
python analysis/funder_data_sharing_trends.py \
  --input-dir ~/claude/pmcoaXMLs/compact_rtrans \
  --output-prefix results/test \
  --limit 100
```

### Map Funding Sources

```bash
cd funder_analysis

python funder-mapping-parquet.py \
  --input /path/to/rtrans_out_full_parquets/ \
  --output funder_results/
```

## Data Architecture

### Data Pipeline Flow

1. **PMC XML Archives** → `extract_from_tarballs.py` → **Baseline Metadata** (122 columns, 18 populated)
2. **PMC XMLs** → **rtransparent R package** → **Pattern Analysis** (120 columns, all sophisticated fields)
3. **Baseline Metadata + Pattern Analysis** → `create_compact_rtrans.py` → **Compact Dataset** (142 columns)

### Key Data Characteristics

**Input Data Sources:**
- PMC XML archives: tar.gz files from https://www.ncbi.nlm.nih.gov/pmc/tools/openftlist/
- rtransparent output: 1,647 parquet files, 1.8 GB total
- Metadata extracts: 25 parquet files with file_size and chars_in_body

**Output Schema (142 columns):**
- 109 short fields from rtrans (max_length ≤ 30)
- 2 metadata fields: file_size, chars_in_body
- 31 funder binary columns: funder_nih, funder_ec, funder_nsfc, etc.

**Excluded Long Text Fields (>1000 chars max_length):**
- coi_text, fund_text, affiliation_institution
- fund_pmc_source, fund_pmc_institute, register_text

### Data Processing Notes

- **PMCID Matching:** PMCIDs are normalized (strip whitespace, ensure "PMC" prefix) before lookups
- **Funder Matching:** Case-insensitive name search + case-sensitive acronym search across 4 funding columns
- **Vectorized Operations:** create_compact_rtrans.py uses pandas vectorized operations for 43x speedup
- **Memory Efficient:** Processes files individually, ~500 MB peak memory usage
- **Caching:** 184 MB metadata cache file speeds up subsequent runs significantly

## Common Workflows

### Full Pipeline: Archives to Analysis-Ready Dataset

```bash
# 1. Extract baseline metadata (fast, for file_size/chars_in_body)
cd extraction_tools
python extract_from_tarballs.py \
  -f parquet -o baseline_metadata.parquet \
  /path/to/pmc/archives/

# 2. Process XMLs with rtransparent R package (done on HPC)
# See rtransparent repo for R package usage

# 3. Create compact dataset with funder matching
cd ..
python create_compact_rtrans.py \
  --input-dir /path/to/rtrans_out_full_parquets \
  --metadata-dir /path/to/extracted_metadata_parquet \
  --output-dir /path/to/compact_rtrans

# 4. Run analysis
cd analysis
jupyter notebook
```

### Data Quality Validation

```bash
# Verify column count
python -c "
import pandas as pd
df = pd.read_parquet('output.parquet')
print(f'{len(df.columns)} columns, {len(df)} records')
"

# Check metadata coverage
python -c "
import pandas as pd
df = pd.read_parquet('output.parquet')
print(f'PMCID coverage: {df['pmcid_pmc'].notna().sum()}/{len(df)}')
print(f'file_size: {df['file_size'].notna().sum()}/{len(df)}')
"

# Check funder matches
python -c "
import pandas as pd
df = pd.read_parquet('output.parquet')
funder_cols = [c for c in df.columns if c.startswith('funder_')]
print(f'{len(funder_cols)} funder columns')
print(f'{df[funder_cols].sum().sum()} total matches')
"
```

## Performance Characteristics

**Extraction Tools:**
- Streaming extraction: ~1,125 files/sec, no disk footprint
- File-based extraction: ~229 files/sec

**create_compact_rtrans.py:**
- Processing rate: ~5.5 files/second
- First run: ~11 min (cache build) + ~8 min (processing) = 19 min total
- Cached runs: ~5 sec (cache load) + ~8 min (processing) = 8 min total
- Output: ~450 MB (vs 1.8 GB rtrans source)

**Memory Requirements:**
- Batch processing: 4-8 GB RAM
- Single file processing: ~500 MB peak

## Current Processing Status (2025-12-02)

### PMCID Registry

A DuckDB-based tracking system (`hpc_scripts/pmcid_registry.py`) tracks processing status:

| Metric | Value |
|--------|-------|
| Total PMCIDs in registry | 6,980,244 |
| oddpub v7.2.3 processed | 6,490,266 (93.0%) |
| Missing/retry needed | ~490,000 |

**Registry Commands:**
```bash
# Show status
python hpc_scripts/pmcid_registry.py status

# Update from oddpub output
python hpc_scripts/pmcid_registry.py update-oddpub-v7 ~/claude/osm-oddpub-out/

# Generate retry swarm for missing PMCIDs
python hpc_scripts/pmcid_registry.py generate-retry oddpub_v7 \
    --xml-base-dir /data/NIMH_scratch/adamt/pmcoa \
    --output-dir /data/NIMH_scratch/adamt/osm/oddpub_output \
    --container /data/adamt/containers/oddpub_optimized.sif \
    --batch-size 1000
```

### oddpub HPC Processing

Processing ~7M PMC articles with oddpub R package v7.2.3 on NIH Biowulf HPC:

**Status (2025-12-02):** HPC processing largely complete. Merged results contain 6,994,457 articles with 374,906 (5.36%) having open data detected.

**Output Locations:**
- Primary: `/data/NIMH_scratch/adamt/osm/oddpub_output/` (PMC*_chunk*_results.parquet)
- Legacy: `/data/NIMH_scratch/adamt/osm/osm-2025-12-poster-incf/output/` (oa_*_xml.PMC*.baseline.*_results.parquet)
- Local sync: `~/claude/osm-oddpub-out/`

**HPC Scripts:**
- `hpc_scripts/pmcid_registry.py` - DuckDB registry for tracking processing status
- `hpc_scripts/generate_oddpub_swarm_extracted_packed.sh` - Generate swarm file for extracted XMLs
- `hpc_scripts/verify_and_retry_oddpub_extracted.sh` - Verify completion, generate retry swarm (legacy)

**Container:** `/data/adamt/containers/oddpub_optimized.sif`
- R 4.3.2 with oddpub 7.2.3
- Python script for processing extracted XMLs
- 4 parallel jobs per swarm line

### oddpub v5 vs v7.2.3 Comparison

Analysis completed comparing rtransparent's embedded oddpub v5 with standalone v7.2.3:

| Metric | v5 | v7.2.3 | Difference |
|--------|-----|--------|------------|
| Detection rate | 11.25% | 5.37% | -5.89% |
| Agreement | 91.95% | - | - |

Key finding: v7.2.3 is significantly stricter, detecting ~50% fewer open data statements. See `docs/ODDPUB_V5_VS_V7_COMPARISON.md` for details.

### OpenSS Analysis (Open Data Subset)

Analysis of publications with `is_open_data=true` from oddpub v7.2.3:

**Merged Dataset (2025-12-02):**
- Total articles: 6,994,457 (93% of PMC Open Access)
- Open data detected: 374,906 (5.36%)
- Open code detected: 141,909 (2.03%)
- Merged file: `~/claude/pmcoaXMLs/oddpub_merged/oddpub_v7.2.3_all.parquet` (409 MB)

**Initial OpenSS Exploration (206,382 records):**
- 346,209 funding texts extracted
- 92,653 unique potential funders discovered

**Scripts:**
- `analysis/openss_explore_funders.py` - NLP-based funder discovery
- `analysis/openss_journals_institutions.py` - Journals/institutions analysis

**Output:**
- `results/openss_explore/` - Funder analysis (all_potential_funders.csv, OPENSS_FINDINGS_SUMMARY.md)
- `results/openss_journals/` - Journals/institutions (top_journals.csv, top_countries.csv, etc.)

**Key Findings:**
- **Top journals:** PLoS ONE (16,891), Scientific Reports (9,874), Frontiers in Microbiology (7,951)
- **Top countries:** China (22,831), USA (17,785), UK (7,636)
- **Missing funders discovered:** National Science Foundation (18,666!), ANR (8,046), JSPS (12,567), NRF (12,394)

See `results/openss_explore/OPENSS_FINDINGS_SUMMARY.md` for complete analysis.

### Next Steps

1. Update oddpub results with completed HPC retry jobs
2. Re-merge oddpub results with new data
3. Update funder database with newly discovered funders (NSF, ANR, JSPS, etc.)
4. Create final poster figures

## Latest Results (2025-11-26)

### Data Sharing Rates by Major Funder (Full Dataset)

| Funder | Total Funded | With Open Data | % |
|--------|--------------|----------------|---|
| **HHMI (USA)** | 11,385 | 5,193 | **45.61%** |
| **Wellcome Trust (UK)** | 74,506 | 19,935 | 26.76% |
| **DFG (Germany)** | 78,165 | 19,700 | 25.20% |
| **AMED (Japan)** | 19,479 | 4,713 | 24.20% |
| **BMGF (USA)** | 13,338 | 3,145 | 23.58% |
| **NIH (USA)** | 405,016 | 93,839 | 23.17% |
| **EC (Europe)** | 136,313 | 31,057 | 22.78% |
| **MRC (UK)** | 111,426 | 22,443 | 20.14% |
| **CIHR (Canada)** | 44,401 | 7,426 | 16.72% |
| **NSFC (China)** | 481,466 | 77,785 | 16.16% |

**Key Finding:** HHMI shows significantly higher data sharing rate than other funders - requires validation to rule out artifacts.

## Important Notes

### Git Workflow After Context Compaction

**CRITICAL:** After a conversation context compaction, always check for uncommitted changes before continuing work:

```bash
git status
git diff
```

If there are pending changes from the previous session, commit them before starting new work. This ensures no work is lost due to compaction boundaries.

### Data Files
- Never commit large data files (XMLs, parquets >10 MB, tar.gz)
- `.gitignore` prevents accidental commits of data directories
- Small summary CSVs/markdown files (<10 MB) are acceptable in results/
- Analysis PNG files in results/ are gitignored (regenerate as needed)

### Branches
- **main**: Stable, production-ready code for poster
- **develop**: Active development branch (current work)

### Data Dictionaries

Complete schema documentation for all data outputs:

- **rtransparent outputs**: `docs/data_dictionary_rtrans.csv`
  - 122/142 columns for rtransparent/compact_rtrans datasets
  - Includes COI, funding, registration, and open science fields

- **oddpub outputs**: `docs/data_dictionary_oddpub.csv`
  - 15 columns for oddpub open data/code detection
  - Includes detection flags, categories, and extracted statements

- **pmcoaXMLs directory structure**: `~/claude/pmcoaXMLs/README.md`
  - Documentation for all input and output directories

### Funder Database
- 31 major biomedical funders tracked: `funder_analysis/biomedical_research_funders.csv`
- 10 international funders (NIH, NSFC, EC, Wellcome, MRC, NHMRC, NSF, HHMI, DFG, CRUK)
- 22 NIH institutes (NCI, NHLBI, NIMH, etc.)
- Columns: Name, Acronym

## Migration Context

This repository consolidates analysis code from:
- rtransparent repo (branch: feature/extract-XML-metadata)
- osm repo (branch: agt-funder-matrix)

Migration completed: 2025-11-24

The related rtransparent R package (for XML pattern analysis) is maintained separately at:
https://github.com/nimh-dsst/rtransparent

## Documentation

Key documentation files:
- `README.md` - Project overview and quick start
- `docs/data_dictionary.csv` - Complete data schema
- `docs/NEXT_STEPS.md` - Remaining work and timeline
- `docs/create_compact_rtrans.md` - Detailed script documentation
- `docs/BENCHMARK_REPORT.md` - Performance analysis
- `docs/TOOLS_COMPARISON.md` - Tool selection guide
- `extraction_tools/README*.md` - Extractor documentation
