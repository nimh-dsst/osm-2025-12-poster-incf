# Funder Aliases Data Dictionary

This document describes the structure and fields of the canonical funder aliases files used for tracking research funding organizations.

## Overview

The funder aliases dataset maps variant names (aliases, acronyms) to canonical funder names, enabling consistent attribution of funding sources across biomedical literature. It includes parent-child relationships for hierarchical organizations (e.g., NIH institutes).

**Current Version:** 4.0

**Files:**
- `funder_aliases_v4.csv` - Flat CSV format (134 rows, 75 canonical funders)
- `funder_aliases_v4.json` - Hierarchical JSON format with nested children

**Build Script:** `funder_analysis/build_canonical_funders.py`

## Corpus Definition

**Important:** The `variant_count` and `merged_count` values in this dataset come from Named Entity Recognition (NER) performed on the **open data subset** of PMC Open Access, not the full corpus:

| Corpus | Size | Description |
|--------|------|-------------|
| **NER Discovery Corpus** | ~375,000 articles | Subset with `is_open_data=true` (oddpub v7.2.3 detection) |
| Full PMC-OA Corpus | ~6.5M articles | All PMC Open Access research articles |

The NER discovery corpus was chosen because:
1. Articles with open data statements are more likely to contain explicit funder acknowledgments
2. Smaller corpus size enables faster NER iteration and threshold tuning
3. Focused on the subset most relevant for data sharing analysis

Future versions may include counts from the full corpus for comparison.

## CSV Format (`funder_aliases_v4.csv`)

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `canonical_name` | string | Official canonical name of the funding organization |
| `variant` | string | A name variant, alias, or acronym that maps to this funder |
| `variant_type` | enum | Classification of the variant (see values below) |
| `country` | string | Country or region of the funding organization |
| `parent_funder` | string | Canonical name of parent organization (if applicable) |
| `funder_type` | enum | Funding source type (see values below) |
| `variant_count` | integer | Number of times this specific variant was detected in corpus |
| `merged_count` | integer | Total count across all variants for this canonical funder |
| `selection_method` | string | Method used to select this funder for inclusion |
| `discovery_method` | enum | How this funder was discovered (see values below) |

### Field Details

#### `canonical_name`
The standardized, official name used to represent this funding organization throughout the analysis. This is the primary key for grouping all variants.

**Examples:**
- `National Institutes of Health`
- `National Science Foundation`
- `European Research Council`

#### `variant`
A text string that, when found in funding acknowledgment text, maps to this canonical funder. Variants include:
- Full official names
- Common abbreviations and acronyms
- Partial name matches
- Alternative spellings

**Examples for "National Science Foundation":**
- `National Science Foundation` (full name)
- `NSF` (acronym)
- `US National Science Foundation` (with country prefix)

#### `variant_type`
Classification of how the variant relates to the canonical name.

| Value | Description |
|-------|-------------|
| `primary` | The main/official name of the organization |
| `acronym` | Standard acronym discovered via NER pipeline |
| `alias` | Alternative name discovered via NER pipeline |
| `acronym_manual` | Acronym added manually (not discovered by NER) |
| `alias_manual` | Alternative name added manually (not discovered by NER) |

#### `country`
The country or region where the funding organization is headquartered.

| Value | Description |
|-------|-------------|
| `USA` | United States of America |
| `UK` | United Kingdom |
| `EU` | European Union (supranational) |
| `China` | People's Republic of China |
| `Japan` | Japan |
| `Germany` | Germany |
| `France` | France |
| `Australia` | Australia |
| `Canada` | Canada |
| `Korea` | Republic of Korea (South Korea) |
| `Switzerland` | Switzerland |
| `Austria` | Austria |
| `Spain` | Spain |
| `Brazil` | Brazil |
| `Portugal` | Portugal |
| `Italy` | Italy |
| `Taiwan` | Taiwan |
| `Netherlands` | Netherlands |
| `Russia` | Russia |
| `Belgium` | Belgium |
| `Poland` | Poland |
| `India` | India |
| `Sweden` | Sweden |
| `Czech Republic` | Czech Republic |

#### `parent_funder`
The canonical name of the parent organization, if this funder is part of a larger umbrella organization. Empty string if no parent.

**Parent-child relationships in v4:**

| Parent | Children |
|--------|----------|
| National Institutes of Health | 23 NIH institutes and centers |
| European Commission | European Research Council |
| UK Research and Innovation | MRC, BBSRC, EPSRC, NERC |

#### `funder_type`
Classification of funding source type.

| Value | Description |
|-------|-------------|
| `government` | Government/taxpayer-funded agency (e.g., NIH, NSF, DFG, NSFC) |
| `philanthropy` | Private foundation or philanthropic organization (e.g., HHMI, Wellcome Trust, Gates Foundation) |
| `supranational` | International/supranational organization (e.g., European Commission, ERC) |

**Distribution in v4:**
- Government: 64 funders (85%)
- Philanthropy: 8 funders (11%)
- Supranational: 3 funders (4%)

#### `variant_count`
The number of times this specific variant text was detected in the **open data subset** (~375k articles) during NER processing.

- For NER-discovered variants: actual count from the open data corpus
- For manually-added variant aliases (e.g., alternate acronyms): `0` (alias not in NER output but added for completeness)

See the **Corpus Definition** section above for details on the NER discovery corpus.

#### `merged_count`
The total count across ALL variants for this canonical funder. This represents the combined detection frequency and is used for ranking funders by prominence.

#### `selection_method`
Describes how this funder met the inclusion criteria.

| Value | Description |
|-------|-------------|
| `4.0sigma+consolidation` | Discovered via 4σ statistical threshold on log-scale counts, then consolidated with aliases |
| `manual_addition` | Added manually to ensure completeness (e.g., NIH institutes) |

#### `discovery_method`
How this funder entry was added to the dataset.

| Value | Description |
|-------|-------------|
| `ner_pipeline` | Discovered automatically via Named Entity Recognition pipeline with 4σ statistical threshold |
| `manual` | Added manually to ensure completeness (primarily additional NIH institutes) |

## JSON Format (`funder_aliases_v4.json`)

### Top-Level Structure

```json
{
  "version": "4.0",
  "description": "Canonical funder aliases with parent-child relationships and discovery method",
  "total_funders": 75,
  "discovery_methods": {
    "ner_pipeline": "Discovered via Named Entity Recognition pipeline with 4σ statistical threshold",
    "manual": "Added manually to ensure completeness (e.g., NIH institutes)"
  },
  "funders": { ... }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `version` | string | Version number of the data format |
| `description` | string | Human-readable description of the dataset |
| `total_funders` | integer | Total number of unique canonical funders |
| `discovery_methods` | object | Definitions of discovery method values |
| `funders` | object | Map of canonical names to funder objects |

### Funder Object Structure

```json
{
  "National Science Foundation": {
    "name": "National Science Foundation",
    "country": "USA",
    "merged_count": 45566,
    "selection_method": "4.0sigma+consolidation",
    "discovery_method": "ner_pipeline",
    "aliases": [ ... ],
    "children": { ... }  // Only present for parent funders
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Canonical name (same as object key) |
| `country` | string | Country/region code |
| `merged_count` | integer | Total detection count across all variants |
| `selection_method` | string | How this funder was selected |
| `discovery_method` | string | `ner_pipeline` or `manual` |
| `aliases` | array | List of alias objects (see below) |
| `children` | object | Nested child funders (only for parents) |

### Alias Object Structure

```json
{
  "variant": "NSF",
  "type": "acronym",
  "count": 14825
}
```

| Field | Type | Description |
|-------|------|-------------|
| `variant` | string | The variant text string |
| `type` | string | Variant type (primary, acronym, alias, etc.) |
| `count` | integer | Detection count for this specific variant |

### Hierarchical Nesting

Parent funders contain a `children` object with nested child funder objects:

```json
{
  "National Institutes of Health": {
    "name": "National Institutes of Health",
    "country": "USA",
    "merged_count": 71044,
    "discovery_method": "ner_pipeline",
    "aliases": [ ... ],
    "children": {
      "National Cancer Institute": {
        "name": "National Cancer Institute",
        "country": "USA",
        "merged_count": 11148,
        "discovery_method": "ner_pipeline",
        "aliases": [ ... ]
      },
      "National Institute of Mental Health": {
        "name": "National Institute of Mental Health",
        "country": "USA",
        "merged_count": 0,
        "discovery_method": "manual",
        "aliases": [ ... ]
      }
    }
  }
}
```

## Statistics (v4)

| Metric | Value |
|--------|-------|
| Total canonical funders | 75 |
| Top-level funders (no parent) | 47 |
| Child funders | 28 |
| NER-discovered funders | 75 |
| Countries represented | 24 |
| Parent organizations | 3 |
| NIH institutes | 23 |
| Government funders | 64 |
| Philanthropy funders | 8 |
| Supranational funders | 3 |

**Note:** All 75 funders have NER counts populated from the open data subset (~375k articles with `is_open_data=true`).

## Parent-Child Relationships

### National Institutes of Health (USA)
23 institutes and centers:

**NER-Discovered (5):**
- National Cancer Institute (NCI)
- National Heart Lung and Blood Institute (NHLBI)
- National Human Genome Research Institute (NHGRI)
- National Institute of Allergy and Infectious Diseases (NIAID)
- National Institute of General Medical Sciences (NIGMS)

**Manually Added (18):**
- Eunice Kennedy Shriver National Institute of Child Health and Human Development (NICHD)
- Fogarty International Center (FIC)
- National Center for Advancing Translational Sciences (NCATS)
- National Center for Complementary and Integrative Health (NCCIH)
- National Eye Institute (NEI)
- National Institute of Arthritis and Musculoskeletal and Skin Diseases (NIAMS)
- National Institute of Biomedical Imaging and Bioengineering (NIBIB)
- National Institute of Dental and Craniofacial Research (NIDCR)
- National Institute of Diabetes and Digestive and Kidney Diseases (NIDDK)
- National Institute of Environmental Health Sciences (NIEHS)
- National Institute of Mental Health (NIMH)
- National Institute of Neurological Disorders and Stroke (NINDS)
- National Institute of Nursing Research (NINR)
- National Institute on Aging (NIA)
- National Institute on Alcohol Abuse and Alcoholism (NIAAA)
- National Institute on Deafness and Other Communication Disorders (NIDCD)
- National Institute on Drug Abuse (NIDA)
- National Library of Medicine (NLM)

### European Commission (EU)
1 child:
- European Research Council (ERC) - NER-discovered

### UK Research and Innovation (UK)
4 research councils:
- Medical Research Council (MRC) - NER-discovered
- Biotechnology and Biological Sciences Research Council (BBSRC) - NER-discovered
- Engineering and Physical Sciences Research Council (EPSRC) - NER-discovered
- Natural Environment Research Council (NERC) - NER-discovered

## Usage Notes

### Funder Matching
When searching funding text for funders:
1. Search for each `variant` (case-insensitive for full names, case-sensitive for acronyms)
2. Map matches to the `canonical_name`
3. Apply parent-child aggregation if desired (roll up children to parents)

### Aggregation
With `--aggregate-children` flag in analysis scripts:
- Child funder matches are attributed to their parent
- Example: NIMH match → counted as NIH
- Prevents double-counting when both parent and child are mentioned

### Version History

| Version | Date | Changes |
|---------|------|---------|
| v1 | 2025-12 | Initial 31 funders from manual curation |
| v2 | 2025-12-07 | 43 funders: top 50 NER matches consolidated into canonical groups |
| v3 | 2025-12-08 | 57 funders: 4σ statistical threshold + consolidation, added parent_funder field |
| v4 | 2025-12-11 | 75 funders: added 23 NIH institutes with NER counts, discovery_method and funder_type fields |

**Key methodology changes:**
- v2: Selected top 50 funders by NER count
- v3+: Uses principled 4σ statistical threshold on log-scale counts (see `build_canonical_funders.py`)
- v4: Populated counts from NER discovery for all NIH institutes (previously 0 for manual additions)

## Related Files

- `biomedical_research_funders.csv` - Original manual funder list (v1)
- `normalize_funders.py` - Python module for funder normalization
- `build_canonical_funders.py` - Script to build canonical funder list from NER output
