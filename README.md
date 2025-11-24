# Open Science Metrics - INCF 2025 Conference Poster

**Conference:** EBRAINS/INCF Conference
**Location:** Brussels, Belgium
**Date:** December 11, 2025
**Project:** Research transparency analysis from PubMed Central Open Access corpus

## Overview

This repository contains analysis code and tools for generating research transparency metrics presented at the INCF 2025 conference. The analysis examines ~6.5M biomedical research articles from PubMed Central Open Access to assess:

- Open data and code sharing practices
- Funding source attribution
- Conflict of interest declarations
- Trial registration compliance

## Repository Structure

```
osm-2025-12-poster-incf/
├── extraction_tools/      # XML metadata extraction scripts
│   ├── extract_xml_metadata.py
│   ├── extract_from_tarballs.py
│   ├── populate_metadata_iterative.py
│   └── merge_parquet_*.py
├── funder_analysis/       # Funder mapping and analysis
│   ├── funder-mapping-parquet.py
│   └── funder_mapping.py
├── analysis/              # Poster analysis scripts
│   └── (analysis notebooks and scripts)
├── notebooks/             # Jupyter notebooks
├── docs/                  # Documentation
│   ├── data_dictionary.csv
│   └── *.md
└── results/               # Small summary files only
```

## Data Sources

**Input Data:**
- PubMed Central Open Access bulk XML files
- rtransparent R package output (120+ columns per article)
- Full parquet dataset: `rtrans_out_full_parquets/` (1,647 files, 1.8 GB)

**NOT Included in Repository:**
- Raw XML files (~150 GB)
- Large parquet files (see `.gitignore`)
- Intermediate processing files

Data available from:
- PMC OA: https://www.ncbi.nlm.nih.gov/pmc/tools/openftlist/
- rtransparent: https://github.com/nimh-dsst/rtransparent

## Requirements

### Python
```bash
cd extraction_tools  # or funder_analysis
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Key dependencies:
- pandas >= 1.3.0
- pyarrow >= 6.0.0
- psutil >= 5.8.0

### R (optional)
For running rtransparent package:
- R >= 4.0
- xml2, dplyr, stringr, tidyverse
- rtransparent package

## Quick Start

### 1. Extract Metadata from PMC Archives

```bash
cd extraction_tools
source venv/bin/activate

# Stream from tar.gz archives (fast, recommended)
python extract_from_tarballs.py \
  -f parquet \
  -o baseline_metadata.parquet \
  /path/to/pmc/archives/
```

### 2. Process with rtransparent (HPC)

Use the rtransparent R package to analyze XMLs and generate full parquet outputs.

### 3. Map Funding Sources

```bash
cd funder_analysis
source venv/bin/activate

python funder-mapping-parquet.py \
  --input /path/to/rtrans_out_full_parquets/ \
  --output funder_results/
```

### 4. Generate Poster Analyses

```bash
cd analysis
# Run analysis notebooks or scripts
jupyter notebook
```

## Data Schema

**Complete dataset:** 122 columns
- **Identifiers:** pmid, pmcid, doi
- **Metadata:** journal, publisher, year, country
- **COI Detection:** 29 pattern columns
- **Funding:** 45 pattern columns + funder array
- **Registration:** 23 pattern columns
- **Open Science:** is_open_data, is_open_code

See `docs/data_dictionary.csv` for complete documentation.

## Branches

- **main**: Stable, production-ready code for poster
- **develop**: Active development branch

## Migration History

This repository consolidates analysis code from:
- `rtransparent` repo (branch: `feature/extract-XML-metadata`)
- `osm` repo (branch: `agt-funder-matrix`)

Migration completed: 2025-11-24

## Important Notes

⚠️ **Data Files:**
- Never commit large data files (XMLs, parquets, tar.gz)
- Use `.gitignore` to prevent accidental commits
- Small summary CSVs/markdown files (<10 MB) are acceptable

📊 **Performance:**
- Streaming extraction: ~1,125 files/sec
- Full dataset processing: ~5-10 minutes (metadata extraction)
- Memory requirements: ~4-8 GB RAM for batch processing

## Documentation

- `docs/REORGANIZATION_PLAN.md` - Project restructuring documentation
- `docs/is_open_data_missing_analysis.md` - Data quality analysis
- `docs/BENCHMARK_REPORT.md` - Performance benchmarks
- `docs/TOOLS_COMPARISON.md` - Tool selection guide

## Contact

For questions about this analysis, contact the NIMH Data Science and Sharing Team.

## License

TBD
