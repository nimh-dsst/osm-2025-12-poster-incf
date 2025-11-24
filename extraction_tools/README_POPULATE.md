# Metadata Population Tools

## Quick Reference

**For rtrans_out_full_parquets (1,647 files):** Use `populate_metadata_efficient.py` ✅

**For rtrans_out_chunks (21 files):** Use `populate_metadata_iterative.py`

## populate_metadata_efficient.py (Recommended)

**Use for:** 1,000+ rtrans parquet files
**Performance:** ~10-15 minutes for 1,647 files
**Strategy:** Merge all rtrans once, then populate metadata files

### Usage

```bash
cd extraction_tools
source ../venv/bin/activate

# Auto-detect and populate ALL sophisticated fields (default)
python populate_metadata_efficient.py \
    --metadata-dir ~/claude/pmcoaXMLs/extracted_metadata_parquet \
    --rtrans-dir ~/claude/pmcoaXMLs/rtrans_out_full_parquets \
    --output-dir ~/claude/pmcoaXMLs/populated_metadata_full

# Or specify specific fields
python populate_metadata_efficient.py \
    --metadata-dir ~/claude/pmcoaXMLs/extracted_metadata_parquet \
    --rtrans-dir ~/claude/pmcoaXMLs/rtrans_out_full_parquets \
    --output-dir ~/claude/pmcoaXMLs/populated_metadata_full \
    --fields is_open_code,is_open_data,funder
```

### How It Works

**Phase 1: Merge rtrans files (5-10 minutes)**
- Reads all 1,647 parquet files
- Loads only needed columns (pmid + fields to populate)
- Deduplicates by pmid (keeps last occurrence)
- Shows progress: files/sec and ETA

**Phase 2: Populate metadata (2-5 minutes)**
- For each of 25 metadata files:
  - Single merge with pre-loaded rtrans data
  - Populates blank fields
  - Saves output with all fields

### Auto-Detection Mode (Default)

When `--fields auto` (the default):
1. Reads schema from first rtrans file
2. Identifies all available sophisticated fields (120+ columns)
3. Matches against metadata file structure
4. Populates ALL matching fields where destination is blank
5. Adds new fields (like 'funder') if present in rtrans

**Result:** All 120+ rtransparent columns populated automatically

### What Gets Populated

**Existing fields** (where blank in metadata):
- COI detection: is_coi_pred, is_relevant_coi, is_explicit_coi, etc. (29 fields)
- Funding: is_fund_pred, is_relevant_fund, is_explicit_fund, etc. (45 fields)
- Registration: is_register_pred, is_NCT, is_method, etc. (23 fields)
- Open science: is_open_data, is_open_code, is_relevant_data, etc. (5 fields)

**New fields** (created and populated):
- funder: Array of funding organizations
- Any other fields in rtrans but not in metadata

## populate_metadata_iterative.py (Legacy)

**Use for:** <100 rtrans files
**Performance:** Good for small datasets, slow for 1,647 files
**Strategy:** For each metadata file, iterate through all rtrans files

### Usage

```bash
cd extraction_tools
source ../venv/bin/activate

# For legacy chunks (21 files)
python populate_metadata_iterative.py \
    --metadata-dir ~/claude/pmcoaXMLs/extracted_metadata_parquet \
    --rtrans-dir ~/claude/pmcoaXMLs/rtrans_out_chunks \
    --output-dir ~/claude/pmcoaXMLs/populated_metadata
```

### Performance

- **21 rtrans files:** ~5-10 minutes
- **1,647 rtrans files:** Hours to days ⚠️

**Why slow for 1,647 files:**
- 25 metadata files × 1,647 rtrans files = 41,175 merge operations
- Each operation loads a file from disk
- No caching or optimization

## Field Population Logic

**Existing fields:** Only populate if blank
- Checks: `field.isna() | (field == '')`
- Preserves existing non-blank values

**New fields:** Create column and populate all matches
- Adds column to metadata if it doesn't exist
- Populates all rows with rtrans values

**Array fields (like 'funder'):**
- Blank = null or empty array `[]`
- Populated = non-empty array

**Scalar fields (like 'is_open_data'):**
- Blank = null or empty string
- Populated = any non-null, non-empty value

## Output

Populated parquet files with:
- Same structure as input metadata
- All blank sophisticated fields populated from rtrans
- New fields added (like 'funder')
- Same filename as input

## Troubleshooting

**Memory errors:**
- Script loads all rtrans data into memory (~2-4 GB)
- Ensure at least 8 GB RAM available
- Close other applications

**Process seems hung:**
- Check CPU usage (should be 90-100%)
- Phase 1 takes 5-10 minutes (1,647 files)
- Phase 2 shows progress per file

**Missing fields:**
- Check that field exists in rtrans files
- Use `--fields auto` to populate all available
- Some fields may be legitimately blank in rtrans

## Examples

**Populate everything (recommended):**
```bash
python populate_metadata_efficient.py \
    --metadata-dir ~/claude/pmcoaXMLs/extracted_metadata_parquet \
    --rtrans-dir ~/claude/pmcoaXMLs/rtrans_out_full_parquets \
    --output-dir ~/claude/pmcoaXMLs/populated_metadata_full
```

**Populate only open science fields:**
```bash
python populate_metadata_efficient.py \
    --metadata-dir ~/claude/pmcoaXMLs/extracted_metadata_parquet \
    --rtrans-dir ~/claude/pmcoaXMLs/rtrans_out_full_parquets \
    --output-dir ~/claude/pmcoaXMLs/populated_metadata_openscience \
    --fields is_open_code,is_open_data,is_relevant_code,is_relevant_data
```

**Populate only funding fields:**
```bash
python populate_metadata_efficient.py \
    --metadata-dir ~/claude/pmcoaXMLs/extracted_metadata_parquet \
    --rtrans-dir ~/claude/pmcoaXMLs/rtrans_out_full_parquets \
    --output-dir ~/claude/pmcoaXMLs/populated_metadata_funding \
    --fields is_fund_pred,is_relevant_fund,is_explicit_fund,funder
```
