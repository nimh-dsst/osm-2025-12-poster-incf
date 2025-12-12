# Affiliation Country Coverage Analysis

**Date:** 2025-12-12
**Author:** Analysis during dashboard build troubleshooting

## Summary

Investigation of affiliation_country field coverage across two primary data sources revealed significant data quality issues and low overall coverage.

## Coverage Statistics

### rtrans_out_full_parquets (rtransparent R package output)

| Metric | Value |
|--------|-------|
| Total records | 6,550,117 |
| Non-empty affiliation_country | 1,639,895 (25.0%) |
| Empty/null | 4,910,222 (75.0%) |

**Top 10 values (showing data quality issues):**

| Value | Count |
|-------|-------|
| China; China | 75,729 |
| China; China; China | 62,865 |
| China | 60,609 |
| USA | 60,533 |
| China; China; China; China | 44,432 |
| UK | 33,451 |
| China; China; China; China; China | 30,879 |
| Germany | 29,403 |
| USA; USA | 29,127 |
| Japan | 23,653 |

**Observations:**
- Significant duplication issue: country names often repeated with semicolon delimiters (e.g., "China; China; China")
- This appears to be from multiple author affiliations being concatenated without deduplication
- The `normalize_country()` function in `build_dashboard_data_duckdb.py` handles this by extracting the first unique country

### extracted_metadata_parquet (Python XML extraction)

| Metric | Value |
|--------|-------|
| Total records | 6,470,793 |
| Non-empty affiliation_country | 613,421 (9.5%) |
| Empty/null | 5,857,372 (90.5%) |

**Top 10 values:**

| Value | Count |
|-------|-------|
| China | 71,504 |
| USA | 50,529 |
| Germany | 31,556 |
| UK | 28,398 |
| Japan | 25,614 |
| Spain | 21,218 |
| Italy | 18,986 |
| France | 17,680 |
| India | 15,913 |
| South Korea | 15,668 |

**Observations:**
- Cleaner data format (single values, no semicolon duplication)
- Much lower coverage (9.5% vs 25%) - only captures first author affiliation
- Values are already normalized country names

## Data Quality Issues

1. **Low overall coverage**: Even the better source (rtrans) only has 25% non-empty values
2. **Duplicated values**: rtrans data has significant semicolon-separated duplicates requiring normalization
3. **Different extraction logic**: The two sources use different extraction approaches, resulting in different coverage and quality

## Dashboard Impact

The `build_dashboard_data_duckdb.py` script was updated to handle these issues:

1. Empty/blank strings are preserved as-is (not converted to "Other")
2. Semicolon-separated duplicates are normalized to first unique country
3. Only non-empty values that don't match the selected country set are converted to "Other"

This ensures the dashboard correctly shows ~75% empty country vs ~25% with valid data.

## Potential Improvement: OpenAlex Integration

It may be possible to significantly improve affiliation_country coverage by cross-referencing to the **OpenAlex database**.

### OpenAlex Overview

OpenAlex (https://openalex.org) is a free and open catalog of the world's scholarly works, including:
- Over 250 million works (papers, books, datasets)
- Author information with institutional affiliations
- Institution data with geographic locations

### Relevant API Endpoints

**Works API:** https://docs.openalex.org/api-entities/works/work-object

The `authorships` field contains:
```json
{
  "authorships": [
    {
      "author": {"id": "...", "display_name": "..."},
      "institutions": [
        {
          "id": "...",
          "display_name": "University Name",
          "country_code": "US",
          "type": "education"
        }
      ],
      "countries": ["US"]
    }
  ]
}
```

### Integration Strategy

1. **Match by DOI or PMID**: OpenAlex indexes PMC articles and can be queried by:
   - DOI: `https://api.openalex.org/works/doi:10.1234/example`
   - PMID: `https://api.openalex.org/works/pmid:12345678`

2. **Extract country codes**: Each authorship includes `countries` array with ISO country codes

3. **Bulk processing**: OpenAlex provides a snapshot download for bulk processing:
   - https://docs.openalex.org/download-all-data/openalex-snapshot
   - Available in Amazon S3 for efficient bulk access

### Expected Coverage Improvement

OpenAlex typically has better coverage of institutional affiliations because:
- Uses multiple data sources (Crossref, MAG, ORCID, ROR)
- Includes historical data
- Has ongoing data curation

Potential improvement: 25% → 70-80% coverage (estimated based on OpenAlex's general affiliation coverage rates)

### Implementation Considerations

1. **Data size**: Full OpenAlex snapshot is ~500 GB compressed
2. **Matching rate**: Not all PMC articles may be in OpenAlex (primarily recent years)
3. **Country code format**: OpenAlex uses ISO 3166-1 alpha-2 codes (US, GB, CN) vs full names (USA, UK, China)
4. **Processing time**: Bulk lookup of 6.5M articles would require careful optimization

## Recommendations

1. **Short term**: Continue using rtrans affiliation_country with normalization (current approach)
2. **Medium term**: Explore OpenAlex integration via bulk snapshot for improved coverage
3. **Long term**: Consider building a dedicated affiliation resolution pipeline using multiple sources

## References

- OpenAlex documentation: https://docs.openalex.org/
- Works API: https://docs.openalex.org/api-entities/works
- Authorships: https://docs.openalex.org/api-entities/works/work-object#authorships
- Bulk download: https://docs.openalex.org/download-all-data/openalex-snapshot
