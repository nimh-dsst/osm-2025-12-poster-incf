# Strategy: Iterative Field Population
**Document created:** 2025-11-11

## Goal
Populate `is_open_code` and `is_open_data` fields in the 25 metadata parquet files using data from the rtrans_out file.

## Approach

### Phase 1: Split rtrans_out file into manageable chunks
- Input: `rtrans_out/4.02M_2025-09-25.parquet` (4,026,571 rows, already sorted by PMID)
- Output: Multiple smaller files (e.g., 20 files of ~200k rows each)
- Extract only essential columns: `pmid`, `is_open_code`, `is_open_data`
- Keep files sorted by pmid for efficient lookups

### Phase 2: Process each metadata file iteratively
For each of the 25 metadata files:
1. Load the metadata file (~3k to 610k rows each)
2. For each rtrans chunk:
   - Load chunk (only 3 columns, ~200k rows - very memory efficient)
   - Match on pmid (converted to string)
   - Populate blank `is_open_code` and `is_open_data` values where matches exist
3. Save updated metadata to output directory

## Memory Profile
- Metadata file: ~3k-610k rows × 122 columns = manageable
- Rtrans chunk: ~200k rows × 3 columns = very small (~5-10 MB)
- Peak memory: One metadata file + one rtrans chunk + join overhead
- Estimated: < 500 MB per iteration

## Implementation

### Script 1: `split_rtrans.py`
```
python split_rtrans.py \
  --input ~/pmcoaXMLs/rtrans_out/4.02M_2025-09-25.parquet \
  --output-dir ~/pmcoaXMLs/rtrans_out_chunks/ \
  --chunk-size 200000
```
- Creates ~20 chunk files
- Each contains: pmid, is_open_code, is_open_data

### Script 2: `populate_metadata_iterative.py`
```
python populate_metadata_iterative.py \
  --metadata-dir ~/pmcoaXMLs/extracted_metadata_parquet/ \
  --rtrans-chunks ~/pmcoaXMLs/rtrans_out_chunks/ \
  --output-dir ~/pmcoaXMLs/populated_metadata_parquet/
```
- Processes all 25 metadata files
- For each file, checks all rtrans chunks for matches
- Only updates `is_open_code` and `is_open_data` fields
- Saves to new directory (preserves originals)

## Advantages
1. **Memory efficient**: Only one metadata file + one small chunk in memory at a time
2. **Resumable**: Can restart from any file if interrupted
3. **Safe**: Original files preserved
4. **Progress tracking**: Clear visibility into which files are done
5. **Parallelizable**: Could process multiple metadata files in parallel if needed

## Expected Results
- Files with matches: ~77% of rows (based on pmid population rate)
- Fields to populate per matched row: 2 (is_open_code, is_open_data)
- Total updates: ~5M field updates across all files
- Processing time: ~5-10 minutes (estimate)

## Verification
After completion, we can verify:
- Count of populated fields before/after
- Number of rows matched
- Data integrity checks
