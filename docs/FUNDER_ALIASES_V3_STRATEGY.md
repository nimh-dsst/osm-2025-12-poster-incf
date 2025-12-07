# Funder Aliases v3 Strategy

**Created:** 2025-12-07
**Purpose:** Document issues found in funder_aliases_v2.csv and remediation strategy

## Issues Identified

### Issue 1: Missing Common Variants

Several important acronyms and aliases present in funder_aliases.csv (v1) are missing from v2:

| Canonical Funder | Missing Variants | Why |
|-----------------|------------------|-----|
| National Cancer Institute | NCI | Acronym not in NER output with sufficient count |
| European Commission | EU, European Union | Variants not in NER output with sufficient count |

**Root Cause:** The `build_canonical_funders.py` script only processes variants that appear in the NER-discovered funders list above the 4σ threshold. Even though these variants are defined in `EXPLICIT_ALIAS_GROUPS`, they are not added to the output if they weren't discovered by NER.

The consolidate_aliases() function (lines 350-432) iterates over NER candidates and maps them to canonical names, but never adds variants from EXPLICIT_ALIAS_GROUPS that weren't discovered.

**Fix Strategy:**
1. After consolidation, iterate through EXPLICIT_ALIAS_GROUPS
2. For each canonical funder that made it to the final output, ensure all its defined variants are included
3. Mark manually-added variants with `variant_type: 'manual'` and `variant_count: 0`

### Issue 2: Missing Country Information

10 canonical funders have "Unknown" in the country field:

| Canonical Funder | Correct Country | Why Unknown |
|-----------------|-----------------|-------------|
| Ministerio de Economia y Competitividad | Spain | No exact match in get_country() |
| Ministry of Education, Culture, Sports, Science and Technology (MEXT) | Japan | Missing exact mapping |
| Fundacao para a Ciencia e a Tecnologia | Portugal | Accent mismatch (Fundação vs Fundacao) |
| Coordenacao de Aperfeicoamento de Pessoal de Nivel Superior | Brazil | Accent mismatch |
| Ministry of Science and Technology (MOST) | Taiwan* | Ambiguous - multiple countries use this name |
| National Key Research and Development Program | China | Missing exact mapping |
| National Science Centre | Poland | Missing exact mapping |
| Department of Biotechnology (DBT) | India | Missing exact mapping |
| National Human Genome Research Institute | USA | Missing exact mapping (NIH institute!) |
| Associazione Italiana per la Ricerca sul Cancro | Italy | Not EU, should be Italy |

*Note: MOST is ambiguous - used by Taiwan, China, Israel, and others. Need to determine primary usage in corpus.

**Root Cause:** The `get_country()` function has incomplete exact mappings and accent normalization issues.

**Fix Strategy:**
1. Add missing entries to `exact_country_map` in get_country()
2. Normalize accents before matching (remove diacritics)
3. Add country data directly to EXPLICIT_ALIAS_GROUPS (see new schema below)

### Issue 3: Missing Parent-Child Relationships

Several funders are components of larger organizations:

| Child Funder | Parent Funder | Notes |
|-------------|---------------|-------|
| National Cancer Institute (NCI) | National Institutes of Health | NIH institute |
| National Institute of Allergy and Infectious Diseases (NIAID) | National Institutes of Health | NIH institute |
| National Institute of General Medical Sciences (NIGMS) | National Institutes of Health | NIH institute |
| National Heart Lung and Blood Institute (NHLBI) | National Institutes of Health | NIH institute |
| National Human Genome Research Institute (NHGRI) | National Institutes of Health | NIH institute |
| European Research Council (ERC) | European Commission | EC program |
| BBSRC | UK Research and Innovation | UKRI member |
| EPSRC | UK Research and Innovation | UKRI member |
| NERC | UK Research and Innovation | UKRI member |
| ESRC | UK Research and Innovation | UKRI member |
| STFC | UK Research and Innovation | UKRI member |

**Root Cause:** The current schema has no `parent_funder` column.

**Fix Strategy:**
1. Add `parent_funder` column to funder_aliases_v3.csv
2. For child funders, set parent_funder to the canonical name of the parent
3. Update analysis scripts to optionally aggregate child counts into parent

## Proposed Schema Changes

### Current Schema (v2)
```
canonical_name,variant,variant_type,country,variant_count,merged_count,selection_method
```

### New Schema (v3)
```
canonical_name,variant,variant_type,country,parent_funder,variant_count,merged_count,selection_method
```

**New column:**
- `parent_funder`: Canonical name of parent organization, or empty if top-level funder

## Implementation Plan

### Task 1: Update EXPLICIT_ALIAS_GROUPS Structure

Modify `build_canonical_funders.py` to use a richer data structure:

```python
EXPLICIT_ALIAS_GROUPS = [
    {
        'canonical': 'National Cancer Institute',
        'variants': ['NCI'],
        'country': 'USA',
        'parent': 'National Institutes of Health',
    },
    {
        'canonical': 'European Commission',
        'variants': ['EC', 'EU', 'European Union'],
        'country': 'EU',
        'parent': None,
    },
    # ...
]
```

### Task 2: Fix consolidate_aliases() to Include All Defined Variants

After processing NER candidates:
```python
# Ensure all explicit variants are included
for group in EXPLICIT_ALIAS_GROUPS:
    canonical = group['canonical']
    if canonical in final_clusters:
        existing_variants = {v[0].lower() for v in final_clusters[canonical]['variants']}
        for variant in group['variants']:
            if variant.lower() not in existing_variants:
                final_clusters[canonical]['variants'].append((variant, 'manual', 0))
```

### Task 3: Update get_country() with Complete Mappings

Add missing entries:
```python
exact_country_map = {
    # ... existing ...
    'Ministerio de Economia y Competitividad': 'Spain',
    'Ministry of Education, Culture, Sports, Science and Technology': 'Japan',
    'Fundacao para a Ciencia e a Tecnologia': 'Portugal',
    'Coordenacao de Aperfeicoamento de Pessoal de Nivel Superior': 'Brazil',
    'National Key Research and Development Program': 'China',
    'National Science Centre': 'Poland',
    'Department of Biotechnology': 'India',
    'National Human Genome Research Institute': 'USA',
}
```

### Task 4: Update Analysis Scripts for Parent Aggregation

Modify `openss_funder_trends.py` and `funder_data_sharing_summary.py`:

1. Add `--aggregate-children` flag (default: False)
2. When enabled, roll up child funder counts into parent totals
3. Optionally exclude children from individual output to avoid double-counting

```python
def aggregate_to_parents(counts_df, aliases_df):
    """Aggregate child funder counts into parent totals."""
    parent_map = aliases_df.set_index('canonical_name')['parent_funder'].to_dict()

    for funder, parent in parent_map.items():
        if pd.notna(parent) and parent in counts_df.index:
            counts_df.loc[parent] += counts_df.loc[funder]

    return counts_df
```

### Task 5: Regenerate funder_aliases_v3.csv

```bash
python funder_analysis/build_canonical_funders.py \
    --input results/openss_explore_v2/all_potential_funders.csv \
    --output funder_analysis/funder_aliases_v3.csv \
    --verbose
```

## Validation Checklist

After v3 is generated, verify:

- [ ] NCI appears as variant of National Cancer Institute
- [ ] EU and European Union appear as variants of European Commission
- [ ] All 10 "Unknown" countries are populated correctly
- [ ] All NIH institutes have parent_funder = "National Institutes of Health"
- [ ] All UK Research Councils have parent_funder = "UK Research and Innovation"
- [ ] ERC has parent_funder = "European Commission"
- [ ] Total canonical funders count is similar to v2 (57)
- [ ] Analysis scripts work with new schema

## Files to Modify

1. `funder_analysis/build_canonical_funders.py` - Core changes
2. `funder_analysis/normalize_funders.py` - Handle parent_funder column
3. `analysis/openss_funder_trends.py` - Add aggregation option
4. `analysis/funder_data_sharing_summary.py` - Add aggregation option
5. `CLAUDE.md` - Update documentation

## Priority

**High** - These issues affect the accuracy of funder analysis results shown on the poster.

## Timeline

1. Update build_canonical_funders.py with fixes
2. Generate funder_aliases_v3.csv
3. Validate output
4. Update analysis scripts
5. Re-run analyses on HPC
6. Commit and document

## Notes

- The "Unknown" country issue for MOST (Ministry of Science and Technology) may require manual research to determine which country's ministry appears most frequently in the corpus
- Consider whether to include additional NIH institutes that are in funder_aliases.csv v1 but not discovered by NER (e.g., NIMH, NIDDK, NINDS, NIA, NEI, NIDA, NIEHS, NICHD, NIAMS, NIAAA, NCATS)
- The parent-child relationship for UK Research Councils is complex because UKRI was created in 2018 by merging the councils, so historical data may reference councils independently
