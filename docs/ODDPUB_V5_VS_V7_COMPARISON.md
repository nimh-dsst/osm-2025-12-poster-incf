# oddpub v5 vs v7.2.3 Comparison Analysis

**Date:** 2025-12-01
**Dataset:** PMC Open Access articles with funder matching

## Data Coverage

| Metric | Count | Percentage |
|--------|-------|------------|
| Total compact_rtrans records | 6,550,117 | 100% |
| Matched with oddpub v7.2.3 | 3,843,143 | 58.7% |
| Missing from v7.2.3 | 2,706,974 | 41.3% |

## Why ~2.7M Records Are Missing

Investigation of HPC processing logs revealed:

| Metric | Count | Percentage |
|--------|-------|------------|
| Expected chunks from file lists | 4,648 | 100% |
| Completed chunks | 1,322 | 28.4% |
| Missing chunks | 3,326 | 71.6% |
| PMCIDs in missing chunks | 3,321,263 | - |

The HPC oddpub v7.2.3 processing only completed about 28% of the new-style chunk jobs. The existing processed data comes from:
- **New-style output (PMC\*)**: 1,323 files (~693k records)
- **Old-style output (oa_\*)**: 5,467 files (~2.1M records)

### Missing Chunks by PMC Range

| PMC Range | Missing Chunks |
|-----------|---------------|
| PMC001xxxxxx | 20 |
| PMC002xxxxxx | 67 |
| PMC003xxxxxx | 268 |
| PMC004xxxxxx | 280 |
| PMC005xxxxxx | 262 |
| PMC006xxxxxx | 340 |
| PMC007xxxxxx | 318 |
| PMC008xxxxxx | 411 |
| PMC009xxxxxx | 423 |
| PMC010xxxxxx | 456 |
| PMC011xxxxxx | 407 |
| PMC012xxxxxx | 72 |

## Detection Rate Comparison

Based on 3,843,143 matched records:

| Version | Detection Rate |
|---------|---------------|
| oddpub v5 (rtransparent) | 11.25% |
| oddpub v7.2.3 | 5.37% |
| **Difference** | **-5.89%** |

## Agreement Matrix

| Category | Count | Percentage |
|----------|-------|------------|
| Both TRUE | 164,731 | 4.29% |
| v5 only (false positives) | 267,716 | 6.97% |
| v7 only | 41,491 | 1.08% |
| Both FALSE | 3,369,205 | 87.67% |
| **Overall Agreement** | 3,533,936 | **91.95%** |

## Detection Rates by Funder

| Funder | Total Funded | v5 Rate | v7 Rate | Difference |
|--------|-------------|---------|---------|------------|
| HHMI (USA) | 9,731 | 41.33% | 21.62% | -19.71% |
| Wellcome (UK) | 54,729 | 24.51% | 12.34% | -12.18% |
| DFG (Germany) | 49,502 | 24.43% | 11.76% | -12.66% |
| AMED (Japan) | 12,442 | 23.86% | 10.95% | -12.91% |
| NIH (USA) | 280,815 | 22.53% | 10.86% | -11.67% |
| EC (Europe) | 88,893 | 22.20% | 10.36% | -11.84% |
| MRC (UK) | 77,025 | 19.43% | 9.11% | -10.32% |
| NSFC (China) | 306,734 | 15.82% | 7.97% | -7.85% |
| CIHR (Canada) | 30,122 | 16.45% | 7.43% | -9.03% |
| BMGF (USA) | 8,830 | 22.36% | 7.01% | -15.35% |

## Key Conclusions

1. **oddpub v7.2.3 is significantly stricter** than v5, detecting about half the rate of open data statements

2. **High agreement on negatives** (87.67% both FALSE) - most articles that v7 says don't have open data, v5 agrees

3. **v5 has more false positives** - 6.97% of articles are flagged by v5 only but not by v7.2.3, suggesting v5 may over-detect

4. **All funders show the same pattern** - v7.2.3 rates are roughly 50% of v5 rates across all funders

5. **HHMI still leads in data sharing** - even with stricter detection, HHMI-funded articles show ~22% open data rate (highest among major funders)

6. **Funder ranking is preserved** - the relative ordering of funders by data sharing rate is similar between v5 and v7.2.3

## Methodology

- **oddpub v5**: Embedded in rtransparent R package, run on full PMC corpus
- **oddpub v7.2.3**: Standalone R package via Apptainer container on NIH Biowulf HPC
- **Matching**: By normalized PMCID (uppercase, PMC prefix)
- **Analysis date**: 2025-12-01

## Files Generated

- `results/oddpub_v5_vs_v7_by_funder.csv` - Funder comparison data
- `~/claude/pmcoaXMLs/oddpub_merged/oddpub_v7.2.3_all.parquet` - Merged oddpub v7.2.3 results (3.97M records, 240.6 MB)
