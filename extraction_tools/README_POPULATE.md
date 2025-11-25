# Metadata Population - Deprecated Approach

## Summary

**This approach was abandoned as ill-conceived.**

## Problem with Original Approach

The populate_metadata scripts attempted to:
1. Extract baseline metadata from XMLs (18 fields) → `extracted_metadata_parquet/`
2. Run rtransparent R package on XMLs (120 fields) → `rtrans_out_full_parquets/`
3. Merge the two datasets to create complete records

## Why This Failed

**Key realization:** Almost all fields in extracted_metadata are already in rtrans output.

- extracted_metadata has: pmid, pmcid, doi, journal, year, etc. (18 fields)
- rtrans output has: All sophisticated fields PLUS pmid, pmcid, doi, journal, year, etc. (120 fields)
- **Only 2 fields unique to extracted_metadata:** `file_size` and `chars_in_body`

**Memory issues:**
- Merging 1,647 rtrans files (1.8 GB, 6.5M records) caused OOM on 8GB RAM
- Multiple optimization attempts failed (batched concat, hierarchical merge, streaming)
- Even with PMCID range filtering, concat of 30-100 files OOM'd

## Correct Approach

**Use rtrans output directly as the primary dataset.**

Only need to add 2 missing fields:
1. Look up `file_size` and `chars_in_body` from extracted_metadata by PMCID
2. Add funder matching (convert funder text → binary grid)
3. Create compact output (exclude long text fields)

See new script: `create_compact_rtrans.py` (to be created)

## Lessons Learned

1. **Understand data overlap before designing pipelines**
   - We had 95% duplicate data between two sources
   - Should have used rtrans as primary, metadata as supplement

2. **Memory constraints matter**
   - 1,647 × 4K rows × 120 columns = too much for 8GB RAM
   - Streaming/chunking doesn't help when final concat still needed

3. **Check assumptions about data organization**
   - Discovered files were sorted by PMCID (useful optimization)
   - But didn't discover data overlap issue until too late

## Deleted Scripts

The following scripts have been removed:
- `populate_metadata_iterative.py` - Iterated through chunks (too slow)
- `populate_metadata_efficient.py` - Tried to merge all at once (OOM)
- `populate_metadata_streaming.py` - Streamed files one at a time (slow, OOM)
- `populate_metadata_optimized.py` - Used PMCID filtering (still OOM)
- `merge_parquet_*.py` - Three merge utilities (not needed)
- `split_rtrans.py` - Split merged files (wrong approach)

## Forward Path

New approach in `create_compact_rtrans.py`:
- Start with rtrans parquet files (complete sophisticated analysis)
- Add file_size and chars_in_body by PMCID lookup
- Convert funder text to binary grid (31 columns for 31 funders)
- Export only short fields (<30 chars) for efficient storage
- Output: Compact parquet files ready for analysis

This avoids all memory issues by processing files individually.
