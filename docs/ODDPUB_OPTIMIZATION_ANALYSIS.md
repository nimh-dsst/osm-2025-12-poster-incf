# Oddpub Pipeline Optimization Analysis

## Current Bottleneck

The major performance issue is in `process_pmcoa_with_oddpub.py` lines 335-346:

```python
with tarfile.open(tarball_path, 'r:gz') as tar:
    members = tar.getmembers()  # <-- THIS IS THE PROBLEM!
    xml_members = [m for m in members if m.name.endswith('.xml')]
    # ... then slice to get chunk ...
    xml_members = xml_members[start_index:end_index]
```

**The problem**: Every chunk job reads the ENTIRE tar.gz file to build the member list, even if it only needs files 500,000-501,000!

### Performance Impact

For a large archive with 600,000 files split into 600 chunks:
- Each chunk job spends ~3-5 minutes reading the entire tar.gz
- 600 chunks × 4 minutes = 2,400 minutes (40 hours) of wasted I/O
- This explains why large archives take disproportionately long!

## Solution Options

### Option 1: Pre-generate File Lists (Immediate Fix)

Already implemented! The `.filelist.csv` files contain the index:

```bash
# Example: oa_comm_xml.PMC009xxxxxx.baseline.2025-06-26.filelist.csv
Member,Size
PMC9000003/PMC9000003.xml,123456
PMC9000004/PMC9000004.xml,134567
...
```

**Fix**: Modify the script to:
1. Read the CSV to get file list for the chunk
2. Use `tar.extractfile(member_name)` directly
3. Never call `tar.getmembers()`

### Option 2: DuckDB Archive Index (Better Long-term)

Create a DuckDB database with tar.gz contents:

```sql
CREATE TABLE pmc_archives (
    archive_name TEXT,
    member_name TEXT,
    pmcid TEXT,
    file_size BIGINT,
    offset BIGINT,  -- byte offset in tar
    xml_content TEXT -- optional: store extracted XML
);
```

Benefits:
- Instant queries: `SELECT * FROM pmc_archives WHERE archive_name = ? LIMIT 1000 OFFSET 500000`
- Optional: Store extracted XML to eliminate tar.gz reads entirely
- Can add indexes on pmcid, journal, etc.

### Option 3: Streaming Tar Iterator (Elegant)

Instead of chunking, use Python's tarfile iteration:

```python
def process_tar_streaming(tarpath, start_idx, chunk_size):
    with tarfile.open(tarpath, 'r:gz') as tar:
        for i, member in enumerate(tar):
            if i < start_idx:
                continue
            if i >= start_idx + chunk_size:
                break
            # Process this file
```

Problem: Still requires reading through all files before start_idx.

### Option 4: Archive Splitting (Preprocessing)

Split large archives into smaller ones:

```bash
# Split PMC009 (600k files) into 60 archives of 10k each
split_archive.py PMC009xxxxxx.tar.gz --size 10000
# Creates: PMC009xxxxxx_001.tar.gz ... PMC009xxxxxx_060.tar.gz
```

Then each job processes one small archive entirely.

## Recommended Implementation

### Immediate Fix (Option 1 - Use CSV lists)

```python
def process_tarball_optimized(tarball_path, csv_path, start_index, chunk_size):
    # Read CSV to get files for this chunk
    df = pd.read_csv(csv_path)
    chunk_files = df.iloc[start_index:start_index + chunk_size]['Member'].tolist()

    with tarfile.open(tarball_path, 'r:gz') as tar:
        for filename in chunk_files:
            # Direct extraction - no enumeration!
            member = tar.getmember(filename)
            xml_data = tar.extractfile(member).read()
            # Process...
```

**Time savings**:
- Current: 3-5 min enumeration per chunk
- Optimized: <1 second CSV read
- For 600 chunks: Save ~40 hours of compute time!

### Long-term Solution (DuckDB)

1. One-time preprocessing:
   ```python
   # Extract all metadata to DuckDB
   extract_to_duckdb.py --input /data/pmcoa/*.tar.gz --output pmcoa.duckdb
   ```

2. Super-fast chunk processing:
   ```python
   con = duckdb.connect('pmcoa.duckdb')
   chunk = con.execute("""
       SELECT member_name, pmcid
       FROM pmc_archives
       WHERE archive_name = ?
       LIMIT ? OFFSET ?
   """, [archive, chunk_size, start_idx]).fetchall()
   ```

3. Optional: Store XML content in DuckDB
   - Pro: No tar.gz reads at all
   - Con: Database size (~500GB for 7M articles)

## Performance Estimates

Current approach (with enumeration bottleneck):
- Small archive (3K files): 5 minutes
- Large archive (600K files): 40+ hours

With CSV optimization:
- Small archive: 5 minutes (no change)
- Large archive: 20 hours (2x speedup)

With DuckDB:
- Any archive: Processing time only (no I/O overhead)
- ~10 hours for large archive

## Implementation Priority

1. **Quick fix**: Update script to use CSV lists (2 hour task, 2x speedup)
2. **Test on small archive**: Verify optimization works
3. **Future**: Build DuckDB infrastructure for next run

## Code Changes Needed

In `process_pmcoa_with_oddpub.py`, replace lines 335-347 with:

```python
# Read file list from CSV
csv_path = tarball_path.with_suffix('.filelist.csv')
if not csv_path.exists():
    logger.error(f"CSV file list not found: {csv_path}")
    return 0

# Get files for this chunk
file_df = pd.read_csv(csv_path)
chunk_files = file_df.iloc[start_index:start_index + chunk_size]['Member'].tolist()
logger.info(f"Processing {len(chunk_files)} files from CSV (index {start_index} to {start_index + len(chunk_files)})")

with tarfile.open(tarball_path, 'r:gz') as tar:
    for member_name in chunk_files:
        try:
            member = tar.getmember(member_name)
            f = tar.extractfile(member)
            # ... rest of processing ...
```

This simple change could cut the remaining processing time in half!