# Tracking Data Sharing in Publications from Major Funders

LaTeX source files for poster #8 presented at [EBRAINS Summit 2025](https://summit2025.ebrains.eu/posters), Brussels, December 2025.

## Authors

- Christoph Li - Data Science & Sharing Team, NIMH
- Josh Lawrimore - Clinical Monitoring Research Program Directorate, Frederick National Laboratory for Cancer Research
- Dustin Moraczewski - Data Science & Sharing Team, NIMH
- Adam Thomas - Data Science & Sharing Team, NIMH

## Abstract

Funders throughout the world have sought to increase data sharing and transparency from their grantees using different approaches and incentives. In this project we compare the relative frequency of data sharing between major funders of biomedical science as measured using text mining in the full text from 6.7M publications available in the PubMed Open Access (PMCOA) collection.

## Key Findings

- **6.8%** of research articles in PMC Open Access have detectable open data statements
- Data sharing rates vary consistently by funder (6% to 25%)
- Top performers: HHMI (24.6%), USDA (21.6%), DBT India (19.5%)
- Largest funders: NIH (426K pubs, 13.4%), NSFC (421K pubs, 9.1%)

## Methods

1. 6.7M publications processed with [rtransparent](https://github.com/serghiou/rtransparent)
2. Non-research articles excluded (5.5M remaining)
3. Open data detection using [OddPub v7.2.3](https://github.com/quest-bih/oddpub)
4. 57 canonical funders identified via NER + statistical thresholding
5. Year-wise analysis for 2010-2024

## Files

| File | Description |
|------|-------------|
| `poster.tex` | Main LaTeX document |
| `preamble.tex` | Package imports and styling |
| `funder_table.tex` | Auto-generated funder comparison table |
| `2025-incf-brussels.bib` | BibTeX references |
| `figures/` | PNG figures for the poster |
| `OSM_INCF_Poster_Brussels_2025_12.pdf` | Final compiled poster |

## Data Sources

- **PMC Open Access Subset**: PMC Open Access Subset [Internet]. Bethesda (MD): National Library of Medicine. 2003 - [cited 2025 06 26]. Available from https://pmc.ncbi.nlm.nih.gov/tools/openftlist/
- **Analysis code**: [github.com/nimh-dsst/osm-2025-12-poster-incf](https://github.com/nimh-dsst/osm-2025-12-poster-incf)
- **Dashboard**: [opensciencemetrics.org](https://opensciencemetrics.org)

## Funding

- NIMH Intramural Research Program (ZICMH002960)
- National Cancer Institute (Contract No. 75N91019D00024)

## License

This work is licensed under CC0 1.0 Universal (Public Domain Dedication).

## Citation

```bibtex
@MISC{Thomas2025-osm,
  title  = "Tracking Data Sharing in Publications from Major Funders",
  author = "Thomas, Adam and Li, Christoph and Lawrimore, Joshua and Moraczewski, Dustin",
  year   = 2025,
  url    = "https://github.com/nimh-dsst/osm-2025-12-poster-incf"
}
```
