# Next Steps for INCF Poster Analysis

**Date:** 2025-11-24
**Status:** Repository reorganized, ready for compact dataset creation

## Completed Today

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

## Remaining Task

### Create `create_compact_rtrans.py`

**Purpose:** Process rtrans files individually to create compact analysis-ready datasets

**Input:** rtrans_out_full_parquets/*.parquet (1,647 files)

**Processing per file:**
1. Load rtrans parquet file
2. Look up file_size & chars_in_body from extracted_metadata by PMCID
3. Apply funder matching algorithm (from funder-mapping-parquet.py)
4. Filter fields: keep only those with max_length <= X (default 30)
5. Add funder binary grid (31 columns, one per funder)
6. Save compact parquet

**Output:** compact_rtrans/*.parquet

**Benefits:**
- No OOM issues (processes files individually)
- Compact output (~20-30 fields instead of 120)
- Ready for analysis (funders as binary grid)
- Includes missing file_size/chars_in_body

### Script Requirements

**Command line arguments:**
- `--input-dir`: rtrans_out_full_parquets directory
- `--metadata-dir`: extracted_metadata_parquet directory
- `--output-dir`: output directory for compact files
- `--max-field-length`: maximum field length to include (default: 30)
- `--funder-db`: path to biomedical_research_funders.csv (default: relative path)

**Key functions:**
1. `load_metadata_lookup()` - Create PMCID → (file_size, chars_in_body) dict
2. `match_funders(row, funder_db)` - Return binary array of 31 funder matches
3. `filter_short_fields(df, max_length, data_dict)` - Keep only short fields
4. `process_rtrans_file(file, metadata_lookup, funder_db, max_length)` - Main processing

**Output schema:**
```
Short fields from rtrans (where max_length <= 30):
- pmid, pmcid_pmc, doi (identifiers)
- year_epub, year_ppub (publication dates)
- Boolean fields (all sophisticated analysis fields)
- Short text fields

Added fields:
- file_size (from metadata)
- chars_in_body (from metadata)

Funder grid (31 binary columns):
- funder_nih, funder_nsfc, funder_ec, funder_wellcome, etc.
- 1 = funder matched, 0 = no match
```

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

- **Now:** Create script skeleton
- **Week of Nov 25:** Test and validate on sample
- **Week of Dec 1:** Run full processing, begin analysis
- **Week of Dec 8:** Finalize poster visualizations
- **Dec 11:** INCF Conference presentation

## Success Criteria

1. ✅ All 1,647 rtrans files processed without OOM
2. ✅ file_size & chars_in_body added for all records with PMCID match
3. ✅ Funder binary grid correctly identifies 31 funders
4. ✅ Output files have ~30-40 fields (vs 120 in rtrans)
5. ✅ Total compact dataset size < 500 MB (vs 1.8 GB rtrans)
6. ✅ Ready for pandas analysis on laptop/desktop

## Notes

- Keep original rtrans files unchanged (use as reference)
- Compact files are for analysis convenience only
- Can always regenerate from rtrans + metadata sources
- Consider adding record count and date range to output filename
