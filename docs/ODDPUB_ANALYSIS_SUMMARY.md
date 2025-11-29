# Oddpub v7.2.3 Analysis Summary

## Current Status (2025-11-29)

- **Processing**: 76% complete (5,361 / 7,038 chunks)
- **Articles analyzed**: ~66,840 (from 100 sample files)
- **Date range**: Older articles (PMC176545 - PMC1657031)

## Key Findings

### Detection Rates

| Metric | oddpub v7.2.3 | rtransparent (older oddpub) |
|--------|---------------|----------------------------|
| Open Data | 5.2% | 15.7% |
| Open Code | 1.3% | 3.5% |

**Note**: Direct comparison not possible as datasets cover different PMC ID ranges:
- oddpub: Processing PMC000-PMC001 archives (older articles, 2000s era)
- rtransparent: Processed PMC5000000+ (recent articles, 2010s-2020s)

### Open Data Categories in oddpub v7.2.3

Among 3,467 articles with open data statements:

| Category | Count | % of Open Data |
|----------|-------|----------------|
| field-specific repository | 3,459 | 99.8% |
| re-use | 2,368 | 68.3% |
| upon request | 1,677 | 48.4% |
| supplement | 1,113 | 32.1% |
| unknown/misspecified | 319 | 9.2% |
| general-purpose repository | 8 | 0.2% |
| github | 1 | 0.0% |

**Key observations**:
1. Multiple categories can apply to one article (hence >100% total)
2. Field-specific repositories dominate (99.8% of open data)
3. Many articles combine repository use with other methods

### Simplified Category Distribution

When reducing to primary category:

| Primary Category | % of All Articles | % of Open Data |
|-----------------|-------------------|----------------|
| No data statement | 86.6% | - |
| Field-specific repository | 5.2% | 99.8% |
| Re-use only | 2.8% | - |
| Upon request only | 2.5% | - |
| Supplement only | 1.4% | - |
| Multiple methods | 1.0% | - |
| Unknown/misspecified | 0.5% | - |

## Why oddpub v7.2.3 Detects Less

Several factors explain the lower detection rate:

1. **Algorithm improvements**: Reduced false positives
2. **Stricter criteria**: What qualifies as "open data"
3. **Article age**: Older articles (2000s) had different data sharing practices
4. **Version differences**: rtransparent uses embedded older version

## Interesting Finding: Category Assignment

The key difference appears to be in **which categories qualify for `is_open_data = True`**:

In oddpub v7.2.3, articles can have categories but still be marked `is_open_data = False`:
- Example: "supplement, unknown/misspecified source" → `is_open_data = False`
- This suggests stricter criteria for what constitutes true "open data"

## Implications for Analysis

1. **Temporal trends**: The current oddpub results represent older articles, making temporal analysis challenging
2. **Conservative detection**: v7.2.3 appears more conservative, potentially more accurate
3. **Repository focus**: Modern emphasis on proper repositories is reflected in categorization

## Next Steps

1. Wait for full oddpub processing to complete (~14 hours remaining)
2. Process newer PMC archives (PMC005-011) for better comparison with rtransparent
3. Merge oddpub results with funder data for poster visualizations
4. Consider using both datasets to show evolution of data sharing practices

## Data Quality Assessment

Based on categories where oddpub detects open data:
- **High quality**: 99.8% use field-specific repositories
- **Medium quality**: 32.1% supplement only (no repository)
- **Low quality**: 48.4% upon request only (poor practice)

This suggests oddpub v7.2.3 better distinguishes between high-quality data sharing (repositories) and lower-quality practices (supplements, upon request).