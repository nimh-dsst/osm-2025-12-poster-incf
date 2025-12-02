# Strategy for Relative Percentage Calculations

## Overview

To report OpenSS (Open Data Subset) findings in **relative terms** (percentages), we need denominators from the full PMCOA corpus. This document outlines the strategy for calculating these denominators locally.

## Data Assets

### Available Locally

| Dataset | Records | Key Fields | Size |
|---------|---------|------------|------|
| **Metadata parquet files** | 6,470,793 | journal, publisher, affiliation_country, affiliation_institution, year | ~570 MB |
| **oddpub merged results** | 6,994,457 | is_open_data, is_open_code, pmcid | 409 MB |
| **rtrans parquet files** | ~6.5M | fund_text (for funder matching), all transparency indicators | 1.8 GB |

### License Breakdown (Metadata)

| License Type | Records | Notes |
|--------------|---------|-------|
| Commercial (comm) | 4,544,299 | CC-BY and similar |
| Non-Commercial (noncomm) | 1,926,494 | CC-BY-NC |
| Other | 0 | Not yet extracted |

**Note:** The "other" subset (~500K articles) would need to be extracted from additional tarballs. These have more restrictive licenses and may not be ideal for open data analysis anyway.

## Denominator Calculation Strategy

### Strategy A: Metadata-Only (Recommended)

**Approach:** Use existing metadata parquet files to calculate totals by journal, country, publisher, institution.

**Pros:**
- Already available locally (~570 MB)
- Contains all needed fields: journal, publisher, affiliation_country, affiliation_institution
- Fast to process (~1 minute)

**Cons:**
- ~500K records missing (the "other" license subset)
- 93% coverage is acceptable for poster

**Implementation:**
```python
# Load all metadata files and aggregate
metadata_totals = {
    'total': 6_470_793,
    'by_journal': {},      # journal -> count
    'by_country': {},      # country -> count
    'by_publisher': {},    # publisher -> count
    'by_institution': {},  # institution -> count
    'by_year': {}          # year -> count
}
```

### Strategy B: Full Registry Rebuild (If Needed)

**Approach:** Rebuild registry to include noncomm and other subsets, then run oddpub on all.

**Pros:**
- Complete coverage
- Consistent methodology

**Cons:**
- Requires HPC time
- May not be worth the effort for 7% more data

### Strategy C: Estimate Missing (Statistical)

**Approach:** Use existing 93% as representative sample, apply statistical correction.

**Pros:**
- No additional processing
- Quick

**Cons:**
- Assumptions about missing data being similar

## Recommended Approach

**Use Strategy A** for the poster:

1. **Build corpus totals** from metadata parquet files (local, ~1 min)
2. **Join with OpenSS results** to get both numerators and denominators
3. **Calculate percentages**: `open_data_pct = (open_data_count / total_count) * 100`

## Implementation Plan

### Step 1: Create Corpus Summary Script

```bash
python analysis/build_corpus_summary.py \
    --metadata-dir ~/claude/pmcoaXMLs/extracted_metadata_parquet \
    --output results/corpus_summary.parquet
```

Output schema:
- `category_type`: journal | country | publisher | institution | year
- `category_value`: the actual value (e.g., "PLoS ONE", "USA")
- `total_count`: number of articles in corpus

### Step 2: Update OpenSS Scripts

Add `--corpus-summary` flag to OpenSS analysis scripts:
- Calculate percentage: `(openss_count / corpus_count) * 100`
- Add confidence intervals for small samples

### Step 3: Output Format

| Rank | Journal | Open Data Count | Total Articles | % Open Data | 95% CI |
|------|---------|-----------------|----------------|-------------|--------|
| 1 | PLoS ONE | 16,891 | 245,000 | 6.89% | (6.79-6.99%) |
| 2 | Scientific Reports | 9,874 | 180,000 | 5.49% | (5.38-5.60%) |

## Caveats

1. **License subset bias**: The "other" license articles may have different open data rates
2. **Missing HPC results**: ~490K articles (7%) not yet processed by oddpub
3. **Year coverage**: Newer articles may have higher open data rates (temporal bias)

## Estimated Resource Requirements

| Task | Local | HPC |
|------|-------|-----|
| Build corpus summary | 5 min, 4 GB RAM | Not needed |
| Join OpenSS results | 10 min, 8 GB RAM | Not needed |
| Process remaining 490K | Not recommended | 2-4 hours |

## Decision

**For the INCF poster**: Use Strategy A (local metadata). The 93% coverage is sufficient and avoids additional HPC time.

**Post-poster**: Consider Strategy B to achieve 100% coverage for publication.
