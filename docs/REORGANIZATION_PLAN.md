# Project Reorganization Plan
**Date:** 2025-11-24
**Target:** EBRAINS/INCF Conference Poster - Brussels, Dec 11, 2025

## Executive Summary

Analysis revealed that derived data in `rtrans_out` and `rtrans_out_chunks` is incomplete compared to the full parquet outputs in `rtrans_out_full_parquets`. This reorganization consolidates analysis code into a new conference-specific repository and updates all scripts to use the complete dataset.

## Current State Analysis

### Data Completeness Issues

**rtrans_out_full_parquets (Authoritative Source)**
- Location: `$EC2_PROJ_BASE_DIR/pmcoaXMLs/rtrans_out_full_parquets/`
- Size: 1.8 GB
- Files: 1,647 parquet files
- Columns: 120+ columns (complete rtransparent output)
- Records: ~6.5M total
- Status: ✅ Complete, authoritative

**rtrans_out (Obsolete Merged)**
- Location: `$EC2_PROJ_BASE_DIR/pmcoaXMLs/rtrans_out/`
- Size: 35 MB
- Files: 1 merged parquet file
- Columns: **9 columns only** (pmid, journal, affiliation_country, is_open_code, is_open_data, year, funder, created_at, data_tags)
- Records: 4,026,571
- Status: ❌ Incomplete - missing 102+ columns, missing 2.4M records
- **RECOMMENDATION: Mark for deletion**

**rtrans_out_chunks (Obsolete Split)**
- Location: `$EC2_PROJ_BASE_DIR/pmcoaXMLs/rtrans_out_chunks/`
- Size: 24 MB
- Files: 21 chunk files
- Columns: **9 columns only** (same as rtrans_out)
- Records: 4,026,571 total (split into chunks)
- Status: ❌ Incomplete - missing 102+ columns, missing 2.4M records
- Created by: `split_rtrans.py` from the incomplete rtrans_out
- **RECOMMENDATION: Mark for deletion**

### Missing Data Impact

The incomplete derived datasets are missing:
- **102 sophisticated analysis columns**: COI patterns (29), funding patterns (45), registration patterns (23), open science indicators (5)
- **2.4M records**: Records with null/missing is_open_data values
- **Critical fields**: Full funder arrays, all pattern detection columns, metadata fields

Scripts using these incomplete datasets will produce inaccurate results for:
- Comprehensive funding analysis beyond basic yes/no
- Conflict of interest detection
- Trial registration analysis
- Complete open data/code statistics

## Repository Structure Analysis

### rtransparent Repository
- **Primary Purpose**: R package for HPC cluster analysis
- **Branch**: `feature/extract-XML-metadata`
- **Code to Migrate**: Python scripts in `rtransparent/extract-XML-metadata/`
  - `extract_xml_metadata.py` - File-based XML metadata extractor
  - `extract_from_tarballs.py` - Streaming XML metadata extractor
  - `populate_metadata_iterative.py` - Metadata population script
  - `merge_parquet_*.py` - Parquet merging utilities (3 variants)
  - `split_rtrans.py` - Chunking utility (uses obsolete data)
  - `download_pmcoa.py` - PMC archive downloader
- **Documentation**: 11 markdown files (many undated)
- **Status**: Code committed but needs migration to analysis repo

### osm Repository
- **Primary Purpose**: Web dashboard and CLI tool
- **Branch**: `agt-funder-matrix`
- **Code to Migrate**: Funder labeling scripts in `osm/scripts/`
  - `funder-mapping-stdin.py` - Original stdin-based funder mapper
  - `funder-mapping-chunks.py` - Chunk-based funder mapper
  - `funder_mapping.py` - Main funder mapping logic
  - `funder-line-graph_v15.py` - Funder visualization
- **Status**: Funder analysis code needs migration to analysis repo

### New Repository: osm-2025-12-poster-incf
- **Purpose**: Analysis code for INCF conference poster
- **Structure**:
  - `main` branch: Stable, ready-to-present analysis
  - `develop` branch: Active development
- **Contents** (to be created):
  - Migrated XML metadata extraction tools
  - Migrated funder labeling tools
  - Updated scripts using rtrans_out_full_parquets
  - Analysis notebooks/scripts for poster
  - Documentation
  - `.gitignore` for large data files

## Scripts Requiring Updates

### High Priority - Use Incomplete Data

**populate_metadata_iterative.py**
- Current: Reads from `rtrans_out_chunks/` (9 columns, 4M records)
- Update to: Read from `rtrans_out_full_parquets/` (120+ columns, 6.5M records)
- Impact: Will recover 2.4M missing records and 102 missing columns
- Changes needed:
  - Update default `--rtrans-dir` path
  - Handle 1,647 files instead of 21 chunks
  - May need batch processing for memory efficiency

**split_rtrans.py**
- Current: Splits rtrans_out merged file
- Status: **Likely obsolete** - was created to work around incomplete data
- Recommendation: Remove or update to work with full parquets if chunking still needed

### Medium Priority - Could Benefit from Full Data

**merge_parquet_*.py** (3 variants)
- Current: Generic merging utilities
- Update: Add support for handling 1,647 full parquet files efficiently
- Variants:
  - `merge_parquet_files.py` - Standard merge
  - `merge_parquet_chunked.py` - Memory-efficient chunked merge
  - `merge_parquet_ultra_efficient.py` - Ultra-low memory merge

### Low Priority - Still Valid

**extract_xml_metadata.py**
- Status: ✅ No changes needed - processes raw XML files

**extract_from_tarballs.py**
- Status: ✅ No changes needed - processes raw tar.gz archives

**download_pmcoa.py**
- Status: ✅ No changes needed - downloads archives

### New Scripts Created

**funder-mapping-parquet.py**
- Location: `$EC2_PROJ_BASE_DIR/pmcoaXMLs/funder-mapping-parquet.py`
- Created: 2025-11-24
- Purpose: Maps funding to 31 funder organizations from parquet files
- Status: ✅ Already uses rtrans_out_full_parquets correctly
- Action: Migrate to new repo as-is

## Documentation Updates Needed

### Undated Markdown Files in extract-XML-metadata/

These files need dates added from git history:
1. `BENCHMARK_REPORT.md`
2. `COMPLETION_SUMMARY.md`
3. `GDRIVE_UPLOAD_GUIDE.md`
4. `INCREMENTAL_SAVE_FEATURE.md`
5. `POPULATION_STRATEGY.md`
6. `README.md`
7. `README_EXTRACTOR.md`
8. `README_STREAMING.md`
9. `SESSION_SUMMARY.md`
10. `SUMMARY.md`
11. `TOOLS_COMPARISON.md`
12. `USAGE_INSTRUCTIONS.md`

Action: Use `git log` to find creation/modification dates and add to document headers

## Migration Plan

### Phase 1: Preparation
- [x] Audit current state
- [x] Identify obsolete data
- [x] Document reorganization plan
- [ ] Add dates to markdown documentation
- [ ] Create backup tags in source repositories

### Phase 2: Repository Setup
- [ ] Create `osm-2025-12-poster-incf` repository on GitHub
- [ ] Initialize with `main` and `develop` branches
- [ ] Create comprehensive `.gitignore`:
  - `*.tar.gz`
  - `*.xml` (PMC XMLs)
  - `*.parquet` (data outputs)
  - `*.pkl` (pickle files)
  - `raw_download/`
  - `extracted_metadata_parquet/`
  - `rtrans_out*/`
  - `populated_metadata/`
- [ ] Set up repository structure:
  ```
  osm-2025-12-poster-incf/
  ├── README.md
  ├── extraction_tools/     # From rtransparent
  ├── funder_analysis/      # From osm
  ├── analysis/             # New poster analysis code
  ├── notebooks/            # Jupyter notebooks for poster
  ├── docs/                 # Documentation
  └── .gitignore
  ```

### Phase 3: Code Migration

**From rtransparent/extract-XML-metadata/**
- [ ] `extract_xml_metadata.py`
- [ ] `extract_from_tarballs.py`
- [ ] `populate_metadata_iterative.py` (update paths)
- [ ] `merge_parquet_*.py` (3 files)
- [ ] `download_pmcoa.py`
- [ ] `data_dictionary.csv`
- [ ] `requirements.txt`
- [ ] Updated markdown documentation (with dates)

**From osm/scripts/**
- [ ] `funder-mapping-stdin.py`
- [ ] `funder-mapping-chunks.py`
- [ ] `funder_mapping.py`
- [ ] `funder-line-graph_v15.py`

**From pmcoaXMLs/**
- [ ] `funder-mapping-parquet.py` (already correct)
- [ ] `is_open_data_missing_analysis.md`
- [ ] This reorganization plan

### Phase 4: Script Updates
- [ ] Update `populate_metadata_iterative.py`:
  - Change default `--rtrans-dir` to rtrans_out_full_parquets
  - Add batch processing for 1,647 files
  - Test with full dataset
- [ ] Update README files with new paths
- [ ] Create analysis scripts for poster using full parquets
- [ ] Validate all scripts work with complete dataset

### Phase 5: Cleanup
- [ ] Mark `rtrans_out/` for deletion (add .DELETE suffix)
- [ ] Mark `rtrans_out_chunks/` for deletion (add .DELETE suffix)
- [ ] Update CLAUDE.md to reflect new structure
- [ ] Archive old analysis files

## Timeline

**Target: Dec 11, 2025 (INCF Conference)**

- **Week of Nov 24**: Repository setup and code migration (Phase 1-3)
- **Week of Dec 1**: Script updates and analysis (Phase 4)
- **Week of Dec 8**: Poster preparation and final validation
- **Dec 11**: Conference presentation

## Data Size Considerations

### Will NOT Commit to GitHub:
- PMC XMLs: ~150 GB compressed
- rtrans_out_full_parquets: 1.8 GB
- extracted_metadata: varies
- Any `.pkl` analysis caches

### Will Commit to GitHub:
- Python scripts: <1 MB
- Documentation: <1 MB
- Analysis notebooks: <10 MB
- `data_dictionary.csv`: <100 KB
- Small result summaries: <10 MB

## Risk Assessment

**Low Risk:**
- Code migration - straightforward copy
- Documentation updates - mechanical process
- `.gitignore` setup - well-defined

**Medium Risk:**
- Script path updates - need thorough testing
- Batch processing 1,647 files - may need performance tuning

**High Risk:**
- Timeline pressure - 17 days until conference
- Incomplete data discovery late in project
- Potential for additional data quality issues

## Success Criteria

1. ✅ New repository created with proper branch structure
2. ✅ All relevant code migrated and functional
3. ✅ Scripts updated to use complete rtrans_out_full_parquets dataset
4. ✅ No large data files committed to GitHub
5. ✅ Documentation updated with dates and new paths
6. ✅ Analysis ready for Dec 11 poster presentation
7. ✅ Obsolete data directories marked for deletion

## Notes

- Keep original repositories intact until migration validated
- Use git tags to mark migration points
- Consider creating migration verification scripts
- Document any data quality issues discovered during migration
