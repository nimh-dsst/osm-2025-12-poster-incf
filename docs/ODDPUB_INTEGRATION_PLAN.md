# oddpub Integration Plan

This document outlines the plan to integrate oddpub v7.2.3 results with the compact_rtrans dataset.

## Current State

### compact_rtrans Dataset (142 columns)
- **Source**: rtransparent R package output + metadata + funder matching
- **Records**: ~6.5M articles
- **Open data fields**: `is_open_data`, `is_open_code` (from oddpub v5 via rtransparent)

### oddpub v7.2.3 Output (15 columns)
- **Source**: Direct oddpub processing of PMC XML body text
- **Records**: ~6.5M articles (92.4% complete, retry in progress)
- **Key fields**:
  - `is_open_data` - Boolean detection flag
  - `is_open_code` - Boolean detection flag
  - `open_data_category` - Detailed categorization
  - `is_reuse` - Data reuse vs. new data sharing
  - `open_data_statements` - Extracted statement text
  - `is_open_data_das` - Detection from Data Availability Statement

## Integration Strategy

### Option A: Add oddpub columns to compact_rtrans (Recommended)

Add 8 new columns from oddpub v7.2.3:

```
Existing compact_rtrans columns (142)
├── is_open_data          # Keep (from rtrans/oddpub v5)
├── is_open_code          # Keep (from rtrans/oddpub v5)
└── [other columns]

New oddpub v7.2.3 columns (+8)
├── oddpub_is_open_data   # Boolean from v7.2.3
├── oddpub_is_open_code   # Boolean from v7.2.3
├── oddpub_open_data_category  # Categorization string
├── oddpub_is_reuse       # Boolean
├── oddpub_is_open_data_das    # Detection from DAS
├── oddpub_is_open_code_cas    # Detection from CAS
├── oddpub_is_code_supplement  # Code in supplement
└── oddpub_is_code_reuse       # Code reuse detected

Total: 150 columns
```

**Pros:**
- Preserves both old and new detection for comparison
- Single unified dataset for analysis
- No breaking changes to existing workflows

**Cons:**
- Slightly larger dataset
- Need to clarify which is_open_data to use in analysis

### Option B: Replace is_open_data/is_open_code

Replace the old oddpub v5 columns with v7.2.3 values.

**Pros:**
- Cleaner schema
- Latest detection algorithm

**Cons:**
- Loses ability to compare versions
- Breaking change for reproducibility

### Recommendation: Option A

Keep both versions with `oddpub_` prefix for new columns to enable:
1. Direct comparison of v5 vs v7.2.3 detection rates
2. Backward compatibility with existing analysis
3. Richer categorization from `open_data_category`

## Implementation Steps

### Step 1: Merge oddpub Results

```bash
# After HPC jobs complete
python extraction_tools/merge_oddpub_results.py \
  --new-dir $HPC_BASE_DIR/osm/oddpub_output \
  --old-dir $HPC_BASE_DIR/osm/osm-2025-12-poster-incf/output \
  --output $EC2_PROJ_BASE_DIR/pmcoaXMLs/oddpub_merged/oddpub_v7.2.3_all.parquet
```

### Step 2: Update create_compact_rtrans.py

Add function to load oddpub lookup and merge by PMCID:

```python
def load_oddpub_lookup(oddpub_file):
    """Load oddpub results as PMCID lookup."""
    df = pd.read_parquet(oddpub_file)
    # Normalize PMCIDs
    df['pmcid_norm'] = df['pmcid'].apply(normalize_pmcid)
    # Select columns to add
    cols = ['pmcid_norm', 'is_open_data', 'is_open_code',
            'open_data_category', 'is_reuse', 'is_open_data_das',
            'is_open_code_cas', 'is_code_supplement', 'is_code_reuse']
    return df[cols].set_index('pmcid_norm').to_dict('index')

# In process_file():
# Add oddpub columns with prefix
oddpub_data = oddpub_lookup.get(pmcid_norm, {})
for col in oddpub_cols:
    df[f'oddpub_{col}'] = oddpub_data.get(col)
```

### Step 3: Add CLI Arguments

```python
parser.add_argument('--oddpub-file', type=str, default=None,
                   help='Path to merged oddpub v7.2.3 parquet file')
```

### Step 4: Update Data Dictionary

Add 8 new columns to `docs/data_dictionary_rtrans.csv`:

| Column | Category | Description |
|--------|----------|-------------|
| oddpub_is_open_data | oddpub_v7 | Open data detected (v7.2.3) |
| oddpub_is_open_code | oddpub_v7 | Open code detected (v7.2.3) |
| oddpub_open_data_category | oddpub_v7 | Categories: field-specific repository, general-purpose repository, supplement, upon request, github, re-use |
| oddpub_is_reuse | oddpub_v7 | Data reuse vs new sharing |
| oddpub_is_open_data_das | oddpub_v7 | Detected in Data Availability Statement |
| oddpub_is_open_code_cas | oddpub_v7 | Detected in Code Availability Statement |
| oddpub_is_code_supplement | oddpub_v7 | Code in supplementary materials |
| oddpub_is_code_reuse | oddpub_v7 | Code reuse detected |

## Comparison Analysis

After integration, create analysis comparing v5 vs v7.2.3:

```python
# Compare detection rates
print(f"v5 open data rate: {df['is_open_data'].mean():.2%}")
print(f"v7.2.3 open data rate: {df['oddpub_is_open_data'].mean():.2%}")

# Cross-tabulation
pd.crosstab(df['is_open_data'], df['oddpub_is_open_data'],
            margins=True, normalize='all')

# Category distribution
df['oddpub_open_data_category'].value_counts()
```

## Timeline

1. **HPC Retry Jobs**: In progress (503 jobs, ~6 hours)
2. **Merge Script**: ✅ Complete (`extraction_tools/merge_oddpub_results.py`)
3. **Update create_compact_rtrans.py**: Next step
4. **Run Integration**: After merge complete
5. **Comparison Analysis**: After integration
6. **Update Funder Trends**: Final step

## Output File Locations

```
$EC2_PROJ_BASE_DIR/pmcoaXMLs/
├── oddpub_merged/
│   └── oddpub_v7.2.3_all.parquet  # Merged oddpub results
├── compact_rtrans/
│   └── [updated files with oddpub columns]  # 150 columns
└── analysis/
    └── oddpub_v5_vs_v7_comparison.csv  # Version comparison
```
