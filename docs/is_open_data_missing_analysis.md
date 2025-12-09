# Analysis: Missing is_open_data Values in Populated Metadata

## Executive Summary

**Finding:** 2,575,626 records (39.8%) in the populated_metadata files have `None/Null` values for `is_open_data` instead of `True/False`.

**Root Causes:**
1. **Missing PMIDs (57.3%)**: 1,476,897 records lack PMID values in extracted_metadata
2. **PMID Mismatch (5.9%)**: 151,519 records have PMIDs not present in rtrans output
3. **Dataset Discrepancy (36.8%)**: ~947,210 records represent broader mismatch between input datasets

## Data Pipeline Overview

The data flows through three stages:

```
1. extracted_metadata_parquet/     6,470,793 records (baseline XML extraction)
   └─> All is_open_data values initialized as None

2. rtrans_out_full_parquets/       6,550,117 records (R transparency analysis)
   └─> All is_open_data values are True/False (boolean)

3. populated_metadata/              6,470,793 records (merged by PMID)
   └─> is_open_data: 60.2% populated, 39.8% remain None
```

## Detailed Analysis

### Stage 1: Source Data Characteristics

**extracted_metadata (6,470,793 records)**
- Created by Python `extract_from_tarballs.py` or `extract_xml_metadata.py`
- Contains 122 columns (18 "copied" + 102 "sophisticated" + 2 metrics)
- `is_open_data` initialized as `None` for all records (placeholder for R analysis)
- PMID field quality:
  - Valid PMIDs: 4,993,808 (77.18%)
  - Empty string '': 1,476,862 (22.82%)
  - Null: 35 (<0.01%)

**rtrans_out_full_parquets (6,550,117 records)**
- Created by R `rt_all_pmc()` function via Docker
- Contains 120 columns with comprehensive pattern analysis
- `is_open_data` is boolean (True/False) for ALL records
- All records have valid PMIDs
- Different record set than extracted_metadata (79K more records)

### Stage 2: Merge Process (populate_metadata_iterative.py)

**Matching Strategy:**
```python
merged = pd.merge(
    metadata_df,
    rtrans_chunk[['pmid'] + available_fields],
    on='pmid',          # ← Matches on PMID, not PMCID!
    how='left',
    suffixes=('', '_rtrans')
)
```

**Merge Results:**
- **Successfully matched:** 3,895,167 records (60.2%)
  - These got `is_open_data` values from rtrans (True or False)

- **Failed to match:** 2,575,626 records (39.8%)
  - These kept `None` for `is_open_data`

### Stage 3: Why 2.57M Records Failed to Match

#### Cause 1: Missing PMIDs in Metadata (57.3%)

**Records:** 1,476,897

**Issue:** These records in extracted_metadata have empty string '' for PMID

**Why it happens:**
- Some PMC XML files don't contain `<article-id pub-id-type="pmid">` elements
- The Python extractor sets PMID to '' when not found
- During merge, empty strings don't match any rtrans PMID
- Result: `is_open_data` remains `None`

**Example records:**
```
pmcid_pmc: PMC176546
pmid: ''                    ← Empty string
is_open_data: None          ← Stays None after merge
```

#### Cause 2: PMID Not in Rtrans Output (5.9%)

**Records:** 151,519

**Issue:** These metadata records have valid PMIDs, but those PMIDs don't exist in rtrans output

**Why it happens:**
- extracted_metadata and rtrans_out were processed from DIFFERENT input sets
- PMID overlap analysis:
  - Metadata valid PMIDs: 4,993,808
  - Rtrans PMIDs: 5,007,953
  - Overlap: 4,842,289 (96.97%)
  - Metadata-only: 151,519 (3.03%)
  - Rtrans-only: 165,664 (3.31%)

**Possible reasons:**
- Different tar.gz archives processed
- Different extraction timing (incremental updates)
- Some XMLs failed rtrans processing but succeeded in metadata extraction

#### Cause 3: Broader Dataset Mismatch (~36.8%)

**Records:** ~947,210 (unexplained by PMID issues alone)

**Issue:** Even accounting for missing/mismatched PMIDs, there's still a gap

**Calculation:**
```
Total missing is_open_data:  2,575,626
Empty/null PMIDs:           -1,476,897
Valid PMID but no match:      -151,519
                            -----------
Unexplained:                   947,210
```

**Potential causes:**
1. **String conversion issues:** PMID stored as different types (int vs string) causing match failures
2. **Whitespace/formatting:** PMIDs with leading/trailing whitespace
3. **Rtrans processing failures:** Records that reached rtrans but failed pattern analysis
4. **Multiple runs:** Different batches processed at different times

## Data Quality Metrics

### Overall Success Rates

| Stage | Records | PMIDs | Success Rate |
|-------|---------|-------|--------------|
| Source XMLs (CSV lists) | 7,363,150 | 7,329,717 unique | - |
| extracted_metadata | 6,470,793 | 4,993,808 valid | 88.2% of source |
| rtrans_out | 6,550,117 | 5,007,953 valid | 89.0% of source |
| populated (with is_open_data) | 3,895,167 | 3,895,167 | 60.2% of metadata |

### PMID Quality in populated_metadata

```
Total records:              6,470,793 (100.0%)
├─ is_open_data = True:       595,240 (  9.2%)
├─ is_open_data = False:    3,299,927 ( 51.0%)
└─ is_open_data = None:     2,575,626 ( 39.8%)
    ├─ Empty/null PMID:     1,476,897 ( 57.3% of None)
    ├─ Valid PMID, no match:  151,519 (  5.9% of None)
    └─ Other reasons:         947,210 ( 36.8% of None)
```

## Source Dataset Discrepancy

The analysis revealed that extracted_metadata and rtrans_out were processed from overlapping but NOT IDENTICAL input sets:

**XML Source Files:**
- Total PMC XMLs in raw_download CSVs: 7,363,150
- Commercial use (oa_comm): 4,880,961
- Non-commercial (oa_noncomm): 2,068,831

**Processing Results:**
```
                    Records    PMCIDs    Coverage
Source CSVs:       7,363,150  7,329,717    100%
extracted_metadata 6,470,793  4,993,808     88%
rtrans_out:        6,550,117  5,007,953     89%
Overlap (PMIDs):        -     4,842,289     66%
```

The 11-12% of source files missing from each output suggests:
- Some XMLs failed parsing in Python extraction
- Some XMLs failed R processing
- Possibly different input batches used

## Recommendations

### 1. Improve PMID Extraction (addresses 57%)

**Problem:** 1.48M records have empty PMIDs

**Solution:**
- Modify Python extractor to use PMCID as fallback when PMID missing
- Add PMCID-based matching as secondary strategy in populate script
- Extract PMID from `<ext-link>` elements as additional source

**Code change for populate_metadata_iterative.py:**
```python
# Current: matches only on pmid
merged = pd.merge(metadata_df, rtrans_chunk, on='pmid', how='left')

# Proposed: match on pmid first, then pmcid_pmc for unmatched
merged = pd.merge(metadata_df, rtrans_chunk, on='pmid', how='left')
unmatched = merged[merged['is_open_data_rtrans'].isna()]
if len(unmatched) > 0:
    # Try matching unmatched records by PMCID
    pmcid_merge = pd.merge(
        unmatched,
        rtrans_chunk,
        on='pmcid_pmc',
        how='left',
        suffixes=('', '_pmcid')
    )
    # Update merged with pmcid matches
    ...
```

### 2. Harmonize Input Datasets (addresses 6%)

**Problem:** 151K metadata PMIDs not in rtrans

**Solution:**
- Document which tar.gz archives were used for each stage
- Reprocess both stages from same input set
- Implement input validation before processing

**Process:**
```bash
# 1. Generate canonical list of XMLs to process
python generate_input_list.py \
  --source raw_download/ \
  --output xmls_to_process.txt

# 2. Run Python extraction on exact list
python extract_from_tarballs.py \
  --input-list xmls_to_process.txt \
  --output extracted_metadata/

# 3. Run R processing on same list
docker run rtransparent \
  --input-list xmls_to_process.txt \
  --output rtrans_out/

# 4. Populate with matching inputs
python populate_metadata_iterative.py \
  --metadata extracted_metadata/ \
  --rtrans rtrans_out/ \
  --output populated/
```

### 3. Investigate Unexplained Gap (addresses 37%)

**Problem:** ~947K records unexplained by PMID issues

**Analysis needed:**
```python
# Identify these records
metadata_pmids = set(valid, non-empty PMIDs from metadata)
rtrans_pmids = set(PMIDs from rtrans)
overlap_pmids = metadata_pmids & rtrans_pmids

# These should match but don't:
problematic = populated[
    (populated['pmid'].isin(overlap_pmids)) &
    (populated['is_open_data'].isna())
]

# Check for:
# 1. PMID type mismatches (string vs int)
# 2. Whitespace issues
# 3. Duplicate PMIDs in source data
# 4. Merge logic bugs
```

### 4. Add Data Quality Checks

**Validation points:**
1. **Pre-extraction:** Verify XML completeness and PMID availability
2. **Post-extraction:** Check PMID coverage rate
3. **Pre-populate:** Verify PMID overlap between inputs
4. **Post-populate:** Report match rates and investigate low rates

**Example check:**
```python
def validate_merge_readiness(metadata_dir, rtrans_dir):
    metadata_pmids = extract_pmids(metadata_dir)
    rtrans_pmids = extract_pmids(rtrans_dir)

    overlap = len(metadata_pmids & rtrans_pmids)
    expected_match_rate = overlap / len(metadata_pmids)

    if expected_match_rate < 0.90:
        raise Warning(
            f"Low PMID overlap: {expected_match_rate:.1%}. "
            f"Expected >90%. Check if inputs are from same source."
        )
```

### 5. Alternative Matching Strategy

**Current:** Matches strictly on PMID
**Proposed:** Multi-field fuzzy matching

```python
# Priority 1: Match on PMID (current approach)
# Priority 2: Match on PMCID
# Priority 3: Match on DOI
# Priority 4: Match on (journal, year, title_hash)

def multi_field_merge(metadata_df, rtrans_df):
    # Try PMID first
    merged = merge_on_pmid(metadata_df, rtrans_df)

    # For unmatched, try PMCID
    unmatched = merged[merged['is_open_data'].isna()]
    if len(unmatched) > 0:
        merged = merge_on_pmcid(unmatched, rtrans_df, merged)

    # For still unmatched, try DOI
    unmatched = merged[merged['is_open_data'].isna()]
    if len(unmatched) > 0:
        merged = merge_on_doi(unmatched, rtrans_df, merged)

    return merged
```

## Impact Assessment

### Current State
- **Usable records:** 3,895,167 (60.2%)
- **Unusable records:** 2,575,626 (39.8%)

### After Implementing Recommendations

**Optimistic scenario (all fixes work):**
- PMID/PMCID matching: +1,476,897 records (57.3%)
- Dataset harmonization: +151,519 records (5.9%)
- Fix unexplained gap: +947,210 records (36.8%)
- **New usable records:** 6,470,793 (100%)
- **Improvement:** +2,575,626 records (+66%)

**Realistic scenario (some issues persist):**
- PMID/PMCID matching: +1,300,000 records (88% success)
- Dataset harmonization: +130,000 records (86% success)
- Fix unexplained gap: +600,000 records (63% success)
- **New usable records:** 5,925,167 (91.6%)
- **Improvement:** +2,030,000 records (+52%)

## Files Generated During Analysis

- `$EC2_PROJ_BASE_DIR/pmcoaXMLs/rtrans_pmcids.pkl` - Set of PMCIDs from rtrans (6.53M)
- `$EC2_PROJ_BASE_DIR/pmcoaXMLs/source_pmcids.pkl` - Set of PMCIDs from source CSVs (7.33M)
- `$EC2_PROJ_BASE_DIR/pmcoaXMLs/missing_pmcids.pkl` - Set of PMCIDs in source but not rtrans (796K)
- `$EC2_PROJ_BASE_DIR/pmcoaXMLs/metadata_pmids.pkl` - Set of valid PMIDs from metadata (4.99M)
- `$EC2_PROJ_BASE_DIR/pmcoaXMLs/rtrans_pmids.pkl` - Set of PMIDs from rtrans (5.01M)

## Conclusion

The 2.57M records with missing `is_open_data` values result from a combination of:
1. **Incomplete PMID extraction** (largest factor at 57%)
2. **Input dataset mismatch** between stages (smallest factor at 6%)
3. **Unidentified merge issues** (significant but unclear at 37%)

The most impactful fix is implementing PMCID-based fallback matching, which could recover over half the missing records. Full dataset harmonization and investigating the unexplained gap would recover the remainder.

---

**Analysis Date:** 2025-11-24
**Analyst:** Claude Code
**Data Location:** `$EC2_PROJ_BASE_DIR/pmcoaXMLs/`
