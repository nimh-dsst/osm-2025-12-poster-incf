# OpenSS Analysis: Open Data Subset Findings

Analysis of publications with `is_open_data=true` from oddpub v7.2.3.

## Dataset Overview

| Metric | Value |
|--------|-------|
| Total PMC records | 3,969,465 |
| Records with is_open_data=true | 211,970 (5.34%) |
| Matched open data records with metadata | 206,382 |
| Funding texts extracted | 346,209 |
| Unique potential funders found | 92,653 |

## Top Journals in Open Data Publications

| Rank | Journal | Count |
|------|---------|-------|
| 1 | PLoS ONE | 16,891 |
| 2 | Scientific Reports | 9,874 |
| 3 | Frontiers in Microbiology | 7,951 |
| 4 | Nature Communications | 5,098 |
| 5 | BMC Genomics | 4,573 |
| 6 | Nucleic Acids Research | 4,179 |
| 7 | Genome Announcements | 4,080 |
| 8 | Frontiers in Plant Science | 3,823 |
| 9 | International Journal of Molecular Sciences | 3,352 |
| 10 | Frontiers in Immunology | 2,966 |

### Key Observations - Journals
- **PLoS ONE dominates** with 8.2% of all open data publications
- **Frontiers journals** are heavily represented (Microbiology, Plant Science, Immunology, Genetics, etc.)
- **Data journals** like Scientific Data (2,300) and Data in Brief (2,167) are significant
- **Pre-print servers** like bioRxiv appear (1,487)
- **Genomics/sequencing journals** well-represented (BMC Genomics, Genome Announcements, Microbiology Resource Announcements)

## Top Countries in Open Data Publications

| Rank | Country | Count | % of Total |
|------|---------|-------|------------|
| 1 | China | 22,831 | 11.1% |
| 2 | USA | 17,785 | 8.6% |
| 3 | UK | 7,636 | 3.7% |
| 4 | Germany | 6,871 | 3.3% |
| 5 | France | 3,616 | 1.8% |
| 6 | Japan | 3,223 | 1.6% |
| 7 | Canada | 3,208 | 1.6% |
| 8 | Australia | 3,114 | 1.5% |
| 9 | Italy | 2,688 | 1.3% |
| 10 | Spain | 2,667 | 1.3% |

### Key Observations - Countries
- **China leads** with 28% more open data publications than the USA
- **Europe collectively** (UK, Germany, France, Netherlands, Switzerland, etc.) is very strong
- **Asian representation** beyond China includes Japan, South Korea, and India

## Top Publishers in Open Data Publications

| Rank | Publisher | Count |
|------|-----------|-------|
| 1 | Frontiers Media S.A. | 28,205 |
| 2 | MDPI | 17,006 |
| 3 | Nature Publishing Group | 15,716 |
| 4 | BioMed Central | 14,300 |
| 5 | Elsevier | 13,836 |
| 6 | PLoS | 23,413 (combined) |
| 7 | American Society for Microbiology | 9,677 |
| 8 | Oxford University Press | 9,210 |
| 9 | John Wiley and Sons | 7,005 |
| 10 | Cold Spring Harbor Lab | 4,207 (combined) |

### Key Observations - Publishers
- **Open access publishers dominate**: Frontiers, MDPI, PLoS, BMC
- **Traditional publishers** (Elsevier, Wiley, OUP) have significant presence
- **Specialty publishers** like ASM well-represented for microbiology data

## Top Institutions in Open Data Publications

| Rank | Institution | Count |
|------|-------------|-------|
| 1 | School of Medicine (generic) | 15,781 |
| 2 | Chinese Academy of Sciences | 7,559 |
| 3 | University of California (system) | 5,556 |
| 4 | Max Planck Institute | 2,170 |
| 5 | Harvard Medical School | 2,053 |
| 6 | Stanford University | 1,864 |
| 7 | University of Cambridge | 1,851 |
| 8 | University of Oxford | 2,346 |
| 9 | INSERM | 1,543 |
| 10 | National Institutes of Health | 1,325 |

### Key Observations - Institutions
- **Medical schools dominate** affiliations
- **Chinese Academy of Sciences** is the single largest identifiable institution
- **Major research universities** (UC system, Harvard, Stanford, Oxford, Cambridge) well represented
- **Government research institutes** (NIH, INSERM, Max Planck) significant contributors

## Fields/Subject Areas in Open Data Publications

Based on journal distribution analysis, the top fields with open data sharing are:

| Field/Subject Area | Key Journals | Estimated Share |
|--------------------|--------------|-----------------|
| **Microbiology/Genomics** | Frontiers in Microbiology, Genome Announcements, MRA, mBio | ~20% |
| **General Life Sciences** | PLoS ONE, Scientific Reports, Nature Communications | ~18% |
| **Plant Biology** | Frontiers in Plant Science | ~4% |
| **Immunology** | Frontiers in Immunology | ~3% |
| **Genetics** | BMC Genomics, PLoS Genetics, Frontiers in Genetics | ~5% |
| **Cell/Molecular Biology** | Int'l J Mol Sciences, Molecules, Cell Reports | ~4% |
| **Ecology/Evolution** | Ecology and Evolution, BMC Ecology | ~2% |
| **Data-Specific** | Scientific Data, Data in Brief, Nucleic Acids Research | ~4% |
| **Psychology/Neuroscience** | Frontiers in Psychology | ~1% |

### Key Observations - Fields
- **Genomics/sequencing data** is the largest category of shared data
- **Microbiology** strongly represented due to genome announcements and sequence data
- **Multi-disciplinary journals** (PLoS ONE, Sci Reports) capture diverse fields
- **Psychology shows lower data sharing** relative to biological sciences
- **Structural biology** (Nucleic Acids Research) has strong data sharing culture

## Key Finding: Missing Funders in Current Database

Our current funder database (31 entries) is missing several major research funders that appear frequently in open data publications:

### Recommended Additions to Funder Database

| Funder | Count in Open Data | Notes |
|--------|-------------------|-------|
| **National Science Foundation** | 18,666 | US NSF - distinct from NIH! Major omission |
| **ANR** | 8,046 | Agence Nationale de la Recherche (France) |
| **Japan Society for Promotion of Science** | 6,620 | Also appears as JSPS (5,947) |
| **National Research Foundation (Korea)** | 6,620 | Also appears as NRF (5,774) |
| **USDA** | 4,687 | US Department of Agriculture |
| **BBSRC** | 4,451 | UK Biotechnology and Biological Sciences Research Council |
| **Swiss National Science Foundation** | 4,001 | Swiss NSF |
| **NSERC** | 3,199 | Natural Sciences and Engineering Research Council (Canada) |
| **FAPESP** | 2,873 | São Paulo Research Foundation (Brazil) |
| **Austrian Science Fund** | 2,476 | Also appears as FWF |

### False Positives to Exclude

| String | Count | Reason to Exclude |
|--------|-------|-------------------|
| III | 3,126 | Grant number suffixes (e.g., "R01 CA123456-01A1") |
| DEAL | 1,577 | Open access licensing agreements |
| CARS | 2,376 | Multiple meanings (Chinese Agricultural Research System, etc.) |
| HHS | 6,034 | Parent agency of NIH - would double-count |
| Blood Institute | 2,577 | Part of NHLBI name - already covered |
| National Center | 3,049 | Generic phrase, not a specific funder |
| the Ministry | 3,495 | Generic phrase |

## Top Funders in Open Data Publications (Combined View)

Combining both "known" and "novel" funders from the analysis:

| Rank | Funder | Count | Currently in DB? |
|------|--------|-------|------------------|
| 1 | National Natural Science Foundation (NSFC) | 52,322 | Yes |
| 2 | NIH | 43,412 | Yes |
| 3 | National Institute (generic) | 37,200 | Yes |
| 4 | National Science Foundation (NSF) | 18,666 + 9,126 = **27,792** | Partial (acronym only) |
| 5 | Natural Science Foundation (China provincial) | 18,376 | Yes |
| 6 | Wellcome Trust | 11,776 | Yes |
| 7 | Medical Research Council | 9,767 | Yes |
| 8 | European Research Council | 8,951 + 5,725 = **14,676** | Yes |
| 9 | ANR | 8,046 | **No** |
| 10 | DFG | 7,640 | Yes |
| 11 | National Cancer Institute / NCI | 7,104 + 6,185 = **13,289** | Yes |
| 12 | Japan Society / JSPS | 6,620 + 5,947 = **12,567** | **No** |
| 13 | National Research Foundation / NRF | 6,620 + 5,774 = **12,394** | **No** |
| 14 | USDA | 4,687 | **No** |
| 15 | BBSRC | 4,451 | **No** |

## Data Quality Observations

### Funding Text Formats
1. **Prose format**: "This work was supported by the National Institutes of Health grant R01 GM123456"
2. **Semicolon-separated**: "NIH; R01 GM123456; Wellcome Trust; 209031/Z/17/Z"
3. **DOI/URL format**: Some entries contain repository DOIs mixed with funding info

### Matching Challenges
- Same funder appears with different names (e.g., "National Science Foundation" vs "NSF")
- Partial name matches (e.g., "National Natural Science Foundation" contains "Science Foundation")
- Some detected "funders" are actually grant programs, not agencies

## Recommendations

1. **Add National Science Foundation (NSF)** to the database - currently only acronym matches
2. **Add ANR (France)** - 8,046 mentions, significant for European research
3. **Add JSPS (Japan)** - 12,567 combined mentions
4. **Add NRF (Korea)** - 12,394 combined mentions
5. **Add BBSRC (UK)** - 4,451 mentions, complements existing MRC coverage
6. **Add USDA** - 4,687 mentions, important for agricultural/biological research
7. **Consider adding Swiss NSF, NSERC (Canada), FAPESP (Brazil)** for broader international coverage

## Files Generated

- `all_potential_funders.csv` - All funders with counts and known/novel flag
- `novel_funders.csv` - Only funders not in current database
- `funder_ngrams.csv` - N-gram frequency analysis
- `sample_funding_texts.txt` - 100 sample funding text entries for data quality review
- `exploration_summary.txt` - Summary statistics
