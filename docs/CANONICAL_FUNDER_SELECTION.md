# Canonical Funder Selection Strategy

**Created:** 2025-12-07
**Purpose:** Document the principled approach for selecting "canonical" funders from NER-discovered entities

## Background

### Problem Statement

The NER-based funder discovery pipeline (`openss_explore_funders.py`) uses spaCy to extract organization entities from funding acknowledgment text. This produces a long-tail distribution of 58,791 potential funders with count >= 2.

The original `funder_aliases.csv` (43 funders) was created by arbitrarily taking the first ~50 entries and manually identifying aliases. This approach is:
- **Not reproducible** - based on arbitrary cutoff
- **Not principled** - no statistical justification
- **Incomplete** - misses important funders below the arbitrary threshold

### Distribution Characteristics

The funder mention distribution follows a **power law** (Zipf's law):

| Statistic | Value |
|-----------|-------|
| Total funders | 58,791 |
| Mean count | 32 |
| Median count | 4 |
| Std dev | 629 |
| Max count | 89,214 |

On log-scale:
- Mean log10(count): 0.75
- Std log10(count): 0.52

**Coverage analysis:**
- Top 20 funders: 25% of mentions
- Top 50 funders: 35% of mentions
- Top 100 funders: 43% of mentions
- Top 200 funders: 50% of mentions

## Quality Issues in Raw NER Output

### 1. Fragments (Incomplete Names)

NER often extracts partial funder names:

| Fragment | Count | Should be |
|----------|-------|-----------|
| National Institute | 63,193 | Part of many NIH institutes |
| Natural Science Foundation | 31,150 | Part of NSFC |
| Science Foundation | 8,542 | Generic fragment |
| Research Council | 6,826 | Generic fragment |
| Blood Institute | 4,439 | NHLBI |
| National Center | 4,959 | Multiple funders |

### 2. Noise/Ambiguous Entries

| Entry | Count | Issue |
|-------|-------|-------|
| III | 5,194 | Roman numeral from grant numbers |
| HHS | 10,338 | Parent agency of NIH, rarely cited directly |
| the Ministry | 5,658 | Too vague |
| DEAL | 3,067 | Publishing consortium, not funder |
| NHS | 2,496 | Healthcare provider, not research funder |

### 3. Duplicate/Alias Entries

The same funder appears multiple times:

| Canonical | Variations in top 100 |
|-----------|----------------------|
| NSFC | "National Natural Science Foundation" (89K), "NSFC" (7K), "Natural Science Foundation" (31K partial) |
| NIH | "NIH" (71K), "National Institute" (63K partial) |
| DFG | "DFG" (13K), "German Research Foundation" (8K) |
| MRC | "MRC" (7K), "Medical Research Council" (17K) |

## Selection Strategy

### Stage 1: Statistical Threshold

Apply a threshold based on the log-normal distribution:

**Primary criterion:** 4 standard deviations above mean on log-scale
```
threshold = 10^(mean + 4*std) = 10^(0.75 + 4*0.52) = 10^2.83 ≈ 658
```

This selects ~300 funders (top 0.5%).

**Rationale:** In a log-normal distribution, 4σ captures the truly exceptional values while excluding the long tail of rare mentions.

### Stage 2: Quality Filters

#### 2a. Fragment Detection

A name is likely a fragment if:
1. Word count <= 3, AND
2. Appears as a substring in another higher-count entry, AND
3. Does not match a known complete funder name

**Implementation:**
```python
def is_fragment(name, all_names):
    if len(name.split()) > 3:
        return False
    for other in all_names:
        if name != other and name in other and count[other] > count[name]:
            return True
    return False
```

#### 2b. Noise Patterns

Exclude entries matching:
```python
NOISE_PATTERNS = [
    r'^[IVX]+$',           # Roman numerals
    r'^[A-Z]{1,2}$',       # Very short acronyms
    r'^the\s',             # Starts with "the"
    r'\d{4,}',             # Contains 4+ digit numbers
]

NOISE_EXACT = {
    'HHS', 'NHS', 'DEAL', 'III', 'II', 'IV', 'VI',
    'the Ministry', 'Federal Ministry', 'National Center',
}
```

#### 2c. Ambiguous Acronyms

Short acronyms (2-4 chars) are kept only if:
1. They have count >= 5,000, OR
2. They match a known funder database (e.g., Crossref Funder Registry)

### Stage 3: Alias Consolidation

#### 3a. Fuzzy Matching

Use string similarity to cluster potential aliases:
```python
from rapidfuzz import fuzz

def are_aliases(name1, name2):
    # Check if one contains the other
    if name1 in name2 or name2 in name1:
        return True
    # Check fuzzy similarity
    if fuzz.token_sort_ratio(name1, name2) > 85:
        return True
    return False
```

#### 3b. Acronym-Name Matching

Match acronyms to full names:
```python
def acronym_matches(acronym, full_name):
    """Check if acronym could be derived from full_name."""
    words = [w for w in full_name.split() if w[0].isupper()]
    potential_acronym = ''.join(w[0] for w in words)
    return acronym.upper() == potential_acronym
```

#### 3c. Canonical Name Selection

For each cluster of aliases, select canonical name as:
1. The **longest complete name** (not a fragment)
2. Prefer names from authoritative sources (Crossref, ROR)
3. Sum counts across all aliases

### Stage 4: Final Threshold

After consolidation, apply final threshold:

**Criterion:** Merged count >= 2,000

**Rationale:** This ensures:
- Sufficient sample size for statistical analysis
- Funder has meaningful presence in the literature
- Approximately 50-80 final canonical funders

> **⚠️ Limitation:** The merged_count >= 2,000 threshold is itself an arbitrary round number, which undermines the principled statistical approach used in Stage 1. While the 4σ threshold on individual entries (count >= 658) is statistically derived, the final threshold on *merged* counts is not. This inconsistency should be addressed in future versions. See [Next Steps](#future-improvements) for planned improvements.

## Output Format

### funder_aliases_v2.csv

```csv
canonical_name,variant,variant_type,country,merged_count,selection_method
National Institutes of Health,NIH,acronym,USA,134237,4sigma+consolidation
National Institutes of Health,National Institute of Health,spelling,USA,134237,4sigma+consolidation
National Natural Science Foundation of China,NSFC,acronym,China,127588,4sigma+consolidation
...
```

**Columns:**
- `canonical_name`: Official funder name
- `variant`: Alternative name/acronym found in text
- `variant_type`: acronym, spelling, translation, partial, etc.
- `country`: Funder country
- `merged_count`: Total mentions across all variants
- `selection_method`: How this funder was selected

### Selection Statistics

The script outputs:
- `funder_selection_stats.json`: Statistics on each selection stage
- `funder_distribution_analysis.png`: Visualization of distribution and thresholds

## Validation

### Cross-Reference with Authoritative Sources

The final list should be validated against:
1. **Crossref Funder Registry** - standardized funder identifiers
2. **ROR (Research Organization Registry)** - organization identifiers
3. **OpenAlex** - funder metadata

### Coverage Check

Verify that the canonical funders cover a meaningful portion of funding acknowledgments:
- Target: >= 60% of articles with funding text mention at least one canonical funder

## Reproducibility

To regenerate the canonical funder list:

```bash
python funder_analysis/build_canonical_funders.py \
    --input results/openss_explore_v2/all_potential_funders.csv \
    --output funder_analysis/funder_aliases_v2.csv \
    --log-threshold 4.0 \
    --min-merged-count 2000 \
    --verbose
```

## Limitations

1. **NER errors propagate** - If spaCy misidentifies an entity, it may be included
2. **Language bias** - English-centric NER may miss non-English funder names
3. **Acronym ambiguity** - Some acronyms have multiple meanings (e.g., NRF = Korea or South Africa)
4. **Temporal changes** - Funders merge, rename, or dissolve over time

## Execution Results (2025-12-07)

### Selection Pipeline Summary

```
58,791 NER-discovered funders
  → 303 (4σ statistical threshold, count >= 658)
  → 286 (noise removal: III, HHS, the Ministry, etc.)         [-17 entries]
  → 264 (fragment removal: Science Foundation, etc.)          [-22 entries]
  → 142 (alias consolidation: merge variants into canonical)  [-122 entries*]
  → 57 (final threshold: merged_count >= 2,000)               [-85 entries]
```

\* Alias consolidation doesn't "remove" entries—it merges multiple NER variants into single canonical funders (e.g., "DFG" + "German Research Foundation" → 1 canonical funder).

### Reduction Breakdown

| Stage | Count | Reduction | Cumulative % Remaining |
|-------|-------|-----------|------------------------|
| Input (NER discovered) | 58,791 | — | 100.0% |
| 4σ threshold (count ≥ 658) | 303 | -58,488 | 0.52% |
| Noise removal | 286 | -17 | 0.49% |
| Fragment removal | 264 | -22 | 0.45% |
| Alias consolidation | 142 | merged | 0.24% |
| Final threshold (merged ≥ 2,000) | 57 | -85 | **0.10%** |

### Coverage of is_open_data Subset

The open data subset (is_open_data=true from oddpub v7.2.3) contains:
- **374,906 articles** with open data detected
- **335,457 articles** (89.5%) matched to rtrans metadata
- **~1,880,000 total funder mentions** extracted by NER

Coverage by selection stage:

| Selection | Funders | % of Mentions | Cumulative Articles |
|-----------|---------|---------------|---------------------|
| All 58,791 NER entries | 58,791 | 100% | — |
| 4σ threshold only | 303 | ~56% | ~188,000 articles |
| Final 57 canonical | 57 | **~38%** | ~127,000 articles |

**Key insight:** The final 57 canonical funders (0.1% of NER entries) account for approximately 38% of all funding acknowledgment mentions in the open data subset. This is less than the 50% coverage target but represents a practical tradeoff between completeness and data quality.

### Top 20 Canonical Funders

| Rank | Funder | Merged Count | Country |
|------|--------|-------------|---------|
| 1 | National Natural Science Foundation of China | 99,031 | China |
| 2 | National Institutes of Health | 71,044 | USA |
| 3 | National Science Foundation | 45,566 | USA |
| 4 | European Research Council | 25,664 | EU |
| 5 | Medical Research Council | 23,913 | UK |
| 6 | Japan Society for the Promotion of Science | 21,489 | Japan |
| 7 | Deutsche Forschungsgemeinschaft | 21,342 | Germany |
| 8 | Wellcome Trust | 21,058 | UK |
| 9 | National Research Foundation of Korea | 20,271 | Korea |
| 10 | Biotechnology and Biological Sciences Research Council | 16,877 | UK |
| 11 | Agence Nationale de la Recherche | 15,083 | France |
| 12 | European Regional Development Fund | 14,188 | EU |
| 13 | National Cancer Institute | 11,148 | USA |
| 14 | Engineering and Physical Sciences Research Council | 9,922 | UK |
| 15 | Swiss National Science Foundation | 8,624 | Switzerland |
| 16 | National Institute of Allergy and Infectious Diseases | 7,857 | USA |
| 17 | United States Department of Agriculture | 7,773 | USA |
| 18 | Ministerio de Economia y Competitividad | 7,743 | Spain |
| 19 | Japan Agency for Medical Research and Development | 7,405 | Japan |
| 20 | Austrian Science Fund | 7,133 | Austria |

### Key Alias Consolidations

| Canonical Name | Variants Merged | Total Count |
|----------------|-----------------|-------------|
| NSFC | National Natural Science Foundation (89K), NSFC (7K), grants from... (2.5K) | 99,031 |
| DFG | DFG (13K), German Research Foundation (8K) | 21,342 |
| JSPS | Japan Society (11K), JSPS (10K) | 21,489 |
| NRF | National Research Foundation (11K), NRF (9K) | 20,271 |
| BBSRC | Biological Sciences Research Council (8K), BBSRC (8K) | 16,877 |
| ANR | ANR (13K), French National Research Agency (2K) | 15,083 |

### Output Files

- `funder_analysis/funder_aliases_v2.csv` - 81 rows, 57 canonical funders
- `funder_analysis/funder_aliases_v2.stats.json` - Selection statistics

## Future Improvements

### Eliminate Arbitrary Final Threshold

The current approach applies a statistically-derived 4σ threshold to *individual* NER entries, then an arbitrary ≥2,000 threshold to *merged* counts. This inconsistency should be addressed:

**Proposed approach:**
1. After alias consolidation, apply a **second 4σ threshold** on merged counts
2. Calculate log-scale mean/std of the 142 consolidated funders
3. Use `10^(mean + 4*std)` as the final threshold
4. This would replace the arbitrary 2,000 cutoff with a principled statistical threshold

**Benefits:**
- Fully reproducible selection criteria
- No arbitrary round numbers
- Threshold adapts if corpus size changes

**Implementation:** See `docs/NEXT_STEPS.md` for task tracking.

## References

- Crossref Funder Registry: https://www.crossref.org/services/funder-registry/
- ROR: https://ror.org/
- OpenAlex: https://openalex.org/
