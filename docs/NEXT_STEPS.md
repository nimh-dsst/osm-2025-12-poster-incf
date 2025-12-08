# Next Steps for INCF Poster Analysis

**Last Updated:** 2025-12-08
**Status:** V3 funder aliases with parent-child aggregation running on HPC

## Completed (2025-12-08)

1. ✅ Created funder_aliases_v3.csv with parent_funder column
   - Schema: canonical_name, variant, variant_type, country, **parent_funder**, variant_count, merged_count, selection_method
   - 57 canonical funders, 81 variant mappings
   - 10 parent-child relationships defined

2. ✅ Parent-child funder relationships:
   - **NIH children:** NCI, NHLBI, NIDDK, NIAID, NIMH
   - **UKRI children:** MRC, BBSRC, EPSRC
   - **EC children:** ERC, Horizon 2020

3. ✅ Updated normalize_funders.py with get_parent() method
   - `FunderNormalizer.get_parent(canonical)` returns parent funder if exists
   - Returns None for funders without parents

4. ✅ Updated openss_funder_trends.py for v3:
   - Added `--aggregate-children` flag for parent-child aggregation
   - Added `aggregate_children_to_parents()` function
   - Aggregates child counts into parent totals before graphing
   - Reduces 57 funders → 47 after aggregation

5. ✅ V3 test successful on HPC (50 file limit)
   - Detected 6 child→parent mappings (5 NIH institutes + MRC)
   - Aggregation working correctly

6. ⏳ Full v3 analysis running on HPC

## Completed (2025-12-07)

1. ✅ Added article_type column to pmcid_registry DuckDB
   - Schema updated with `article_type VARCHAR` and `license VARCHAR` columns
   - Created `update-article-type` CLI command
   - Populated from rtransparent parquet files (uses `type` column)
   - 6,376,680 / 6,980,244 PMCIDs (91.4%) now have article type

2. ✅ Article type distribution in registry:
   | Type | Count | % |
   |------|-------|---|
   | research-article | 4,597,910 | 72.1% |
   | review-article | 568,421 | 8.9% |
   | case-report | 311,618 | 4.9% |
   | abstract | 154,886 | 2.4% |
   | other | 154,716 | 2.4% |
   | brief-report | 114,896 | 1.8% |
   | editorial | 110,814 | 1.7% |
   | letter | 91,832 | 1.4% |
   | correction | 85,359 | 1.3% |
   | book-review | 36,208 | 0.6% |
   | article-commentary | 22,578 | 0.4% |
   | systematic-review | 18,139 | 0.3% |
   | discussion | 16,294 | 0.3% |
   | data-paper | 15,263 | 0.2% |
   | meeting-report | 14,750 | 0.2% |

## Completed (2025-12-03)

1. ✅ Populated pmcid column in oddpub_v7.2.3_all.parquet (6,994,105 PMCIDs)
   - Used pyarrow for memory-efficient processing
   - Normalized format: `PMCPMC544856.txt` → `PMC544856`

2. ✅ Created registry validation script (`hpc_scripts/validate_pmcid_registry.py`)
   - DuckDB-based comparison against PMC filelist CSVs
   - Validates PMCID coverage and source_tarball correctness

3. ✅ Validated registry: 100% PMCID coverage (6,980,244 PMCIDs)

4. ✅ Identified registry source_tarball bug: all records have `oa_comm_xml` prefix
   - Created repair script: `hpc_scripts/repair_pmcid_registry.py`
   - Fixes source_tarball and adds license column (comm/noncomm/other)

5. ✅ Created DuckDB-based dashboard builder (`analysis/build_dashboard_data_duckdb.py`)
   - 10-100x faster than pandas version (8 seconds for 1000 PMCIDs test)
   - Uses DuckDB glob pattern to read all 1647 parquet files at once
   - SQL JOINs instead of slow pandas .isin() operations
   - Output: 9 columns (pmid, journal, affiliation_country, is_open_data, is_open_code, year, funder[], data_tags[], created_at)

6. ✅ Dashboard data build running on Biowulf HPC (6.57M PMCIDs, ~15-20 min expected)

## Completed (2025-12-02)

1. ✅ Merged oddpub results (6,994,457 articles, 374,906 open data)
2. ✅ Created canonical funder alias system (43 funders, 65+ variants)
3. ✅ Built corpus totals for canonical funders
4. ✅ Generated funder trends graphs (counts and percentages)
5. ✅ OpenSS funder discovery (58,896 potential funders)

## Completed (2025-12-01)

1. ✅ Created DuckDB-based PMCID registry (`hpc_scripts/pmcid_registry.py`)
   - Tracks 6,980,244 PMCIDs across 5 processing pipelines
   - Boolean flags: oddpub_v7, oddpub_v5, rtransparent, metadata, compact
   - Commands: init, update-*, status, generate-retry, export-missing

2. ✅ Initialized registry and updated oddpub v7.2.3 status
   - 4,186,201 PMCIDs processed (60.0%)
   - 2,794,043 PMCIDs need retry

3. ✅ Discovered missing XMLs from `oa_other_*.tar.gz` archives
   - These had not been extracted, causing many failures
   - Now extracted and included in retry jobs

4. ✅ Generated and submitted retry swarm for missing PMCIDs
   - 27,941 batches of 100 PMCIDs each
   - 6,986 swarm lines submitted to Biowulf HPC

5. ✅ Completed oddpub v5 vs v7.2.3 comparison analysis
   - v7.2.3 is ~50% stricter (5.37% vs 11.25% detection rate)
   - 91.95% agreement between versions
   - See `docs/ODDPUB_V5_VS_V7_COMPARISON.md`

## In Progress

1. ⏳ Dashboard data build running on Biowulf HPC (6.57M PMCIDs from comm + noncomm)
2. ⏳ Final poster figure generation

## Next Steps

1. Verify dashboard data build output
2. Run repair script to fix registry source_tarball and add license column
3. Process remaining ~490K articles with oddpub (7% missing)
4. Create final poster figures with updated data
5. (Post-poster) Update Python version in venv and container

---

## Completed (2025-11-26 Evening)

1. ✅ Fixed critical bug in `create_compact_rtrans.py` - funder matching now occurs BEFORE field filtering
2. ✅ Regenerated all 1,647 compact parquet files with correct funder matching (1.89M total matches)
3. ✅ Created `funder_data_sharing_trends.py` with memory-efficient batch processing
4. ✅ Analyzed full dataset (5.6M records) for data sharing trends by funder
5. ✅ Generated 3 graphs and 3 CSVs: counts, totals, percentages (2003-2025)
6. ✅ Validated funder match counts against Nov 24 baseline (within 10% - acceptable)

**Key Finding:** HHMI shows 45.61% data sharing rate - significantly higher than other funders (16-27%)

## Completed (2025-11-26 Morning)

1. ✅ Created and tested `create_compact_rtrans.py`
2. ✅ Generated compact parquet files (142 columns, ~370 MB for test dataset)
3. ✅ Validated output schema: 109 short fields + 31 funder columns + 2 metadata fields
4. ✅ Confirmed funder matching working correctly (vectorized, 43x speedup)
5. ✅ Metadata enrichment complete (file_size, chars_in_body added)

## Completed (2025-11-24)

1. ✅ Deleted 8 obsolete populate/merge scripts that failed due to OOM
2. ✅ Documented why the merge approach failed (95% data overlap between sources)
3. ✅ Added biomedical_research_funders.csv to repo
4. ✅ Updated funder-mapping-parquet.py to use relative paths
5. ✅ Analyzed field lengths and added max_length/median_length to data_dictionary.csv
6. ✅ Created PMCID/year range indices for both metadata and rtrans directories
7. ✅ Committed and ready to push

## Key Discovery

**extracted_metadata and rtrans have 95% duplicate data:**
- Both have: pmid, pmcid, doi, journal, publisher, year, coi_text, fund_text, etc.
- **Only 2 fields unique to metadata:** `file_size` and `chars_in_body`

**Solution:** Use rtrans as primary source, supplement with 2 missing fields.

## Compact Dataset Schema (142 columns)

**Location:** `~/claude/pmcoaXMLs/compact_rtrans_test/` (test dataset with 1,647 parquet files)

**Key Columns for Visualization:**
- **Identifiers:** `pmid`, `pmcid_pmc`, `doi`
- **Year:** `year_epub` (electronic publication), `year_ppub` (print publication)
- **Open Science Metrics:**
  - `is_open_code`: Boolean indicating code sharing
  - `is_open_data`: Boolean indicating data sharing
  - `is_relevant_code`: High-confidence code sharing indicator
  - `is_relevant_data`: High-confidence data sharing indicator
- **Funder Columns (31 total):** `funder_nih`, `funder_ec`, `funder_nsfc`, `funder_wellcome`, `funder_mrc`, etc.
  - Binary (0/1) indicating if article was funded by that organization
  - Based on vectorized pattern matching in funding text fields
- **Metadata:** `file_size`, `chars_in_body`
- **Other Analysis Fields:** COI indicators, funding indicators, registration indicators (all boolean)

**Funder Column Naming:**
- Format: `funder_{acronym_lowercase}` (e.g., `funder_nih`, `funder_nsfc`)
- Extract acronym by removing "funder_" prefix and converting to uppercase for display

## Current Task

### Adapt `funder-line-graph_v15.py` for Parquet Input

**Purpose:** Create line graphs showing data/code sharing rates by major funders over time

**Previous Version:** `~/claude/osm/scripts/funder-line-graph_v15.py`
- Designed for CSV input with 31 funder boolean columns
- Expected columns: 'year', 'is_code_pred', plus 31 funder columns (columns 1-32)
- Generated count and percentage plots with multiple line styles

**Adaptations Needed:**

1. **Input Format Change:**
   - OLD: Single CSV file
   - NEW: Directory of parquet files (`~/claude/pmcoaXMLs/compact_rtrans_test/`)

2. **Schema Differences:**
   - OLD: `year` column (single integer)
   - NEW: `year_epub` and `year_ppub` columns (choose primary)
   - OLD: `is_code_pred` boolean
   - NEW: `is_open_code` and `is_relevant_code` booleans (choose appropriate metric)
   - OLD: Funder columns in positions 1-31
   - NEW: Funder columns with prefix `funder_*` (e.g., `funder_nih`, `funder_ec`)

3. **Data Loading:**
   - Read all parquet files from directory
   - Concatenate into single DataFrame
   - Handle potential memory constraints (142 columns × ~6.5M rows)

4. **Output Requirements:**
   - Count plot: Number of open code/data articles by funder over time
   - Percentage plot: Percentage of funder-funded articles with open code/data (2015-2020)
   - CSV exports: count and percentage data
   - PNG plots: High-resolution (300 DPI) with legends

**New Script Location:** `analysis/funder_data_sharing_trends.py`

**Command-Line Interface:**
```bash
python analysis/funder_data_sharing_trends.py \
  --input-dir ~/claude/pmcoaXMLs/compact_rtrans_test \
  --output-prefix results/funder_data_sharing \
  --year-column year_epub \
  --metric is_open_code \
  --year-range 2015 2020 \
  --log INFO
```

**Key Adaptations:**
1. Replace `pd.read_csv()` with batch parquet loading
2. Map column names: `year` → `year_epub`, `is_code_pred` → `is_open_code`
3. Dynamically detect funder columns with `funder_*` prefix
4. Add memory-efficient chunked processing if needed
5. Update plot titles and labels to reflect "data sharing" or "code sharing"
6. Optionally support both `is_open_code` and `is_open_data` metrics

**Testing:**
- Test on compact_rtrans_test first (smaller dataset)
- Verify funder acronym extraction works with new naming
- Confirm plots match expected trends
- Validate CSV output format

## Data Dictionary Information

**From data_dictionary.csv:**
- 109 fields have max_length <= 30 (suitable for compact dataset)
- 5 fields have max_length 31-100
- 6 fields have max_length > 1000 (these will be excluded)

**Long text fields to exclude (>1000 chars):**
1. coi_text (max: 21,957, median: 82)
2. fund_text (max: 11,435, median: 149)
3. affiliation_institution (max: 9,955, median: 58)
4. fund_pmc_source (max: 4,733, median: 0)
5. fund_pmc_institute (max: 3,680, median: 0)
6. register_text (max: 3,132, median: 0)

## Index Files for Reference

**Metadata index:** `/home/ec2-user/claude/pmcoaXMLs/extracted_metadata_parquet/INDEX.json`
- 25 files
- PMCID ranges (e.g., PMC000: 176545-556014)
- Year ranges (e.g., 2003-2024)

**Rtrans index:** `/home/ec2-user/claude/pmcoaXMLs/rtrans_out_full_parquets/INDEX.json`
- 1,647 files
- PMCID ranges per file
- Year ranges per file

Use these indices to optimize PMCID lookups between datasets.

## Funder Matching Algorithm

**Reference:** funder_analysis/funder-mapping-parquet.py

**Key logic:**
```python
# Load funders
funders_df = pd.read_csv('biomedical_research_funders.csv')
# Columns: Name, Acronym

# Search in 4 funding columns:
funding_columns = ['fund_text', 'fund_pmc_institute',
                   'fund_pmc_source', 'fund_pmc_anysource']

# For each funder:
for name, acronym in zip(funders_df['Name'], funders_df['Acronym']):
    # Case-insensitive name match
    name_matches = text.contains(name, case=False)

    # Case-sensitive acronym match (avoid false positives)
    acronym_matches = text.contains(acronym, case=True)

    # Match if either name or acronym found
    is_match = name_matches | acronym_matches
```

## Estimated Runtime

- **Metadata lookup build:** ~30 seconds (load 25 files, create PMCID dict)
- **Per rtrans file processing:** ~0.5-1 second
- **Total for 1,647 files:** ~15-25 minutes
- **Memory usage:** ~500 MB (one file at a time)

## Timeline

- ✅ **Week of Nov 25:** Created and tested create_compact_rtrans.py
- **Week of Dec 1:** Develop visualization scripts, run on full dataset
- **Week of Dec 2-8:** Generate poster figures and statistics
- **Dec 9-10:** Finalize poster design
- **Dec 11:** INCF Conference presentation

## Success Criteria

### Compact Dataset Creation (✅ COMPLETE)
1. ✅ All 1,647 rtrans files processed without OOM
2. ✅ file_size & chars_in_body added for all records with PMCID match
3. ✅ Funder binary grid correctly identifies 31 funders
4. ✅ Output files have 142 columns (109 short fields + 31 funders + 2 metadata)
5. ✅ Total compact dataset size ~450 MB (vs 1.8 GB rtrans)
6. ✅ Ready for pandas analysis on laptop/desktop

### Visualization Development (IN PROGRESS)
1. ⏳ Adapt funder line graph script for parquet input
2. ⏳ Generate trends for both open code AND open data metrics
3. ⏳ Create publication-quality figures (300 DPI, clear legends)
4. ⏳ Export summary statistics tables (CSV format)
5. ⏳ Validate trends match expected patterns
6. ⏳ Test on full dataset (~6.5M articles)

## Notes

- Keep original rtrans files unchanged (use as reference)
- Compact files are for analysis convenience only
- Can always regenerate from rtrans + metadata sources
- Consider adding record count and date range to output filename
