# PMC Open Access XML Field Reference

This document describes the fields available in PubMed Central Open Access (PMCOA) XML files and their extraction status in our pipeline.

## XML Format

PMCOA uses the JATS (Journal Archiving and Interchange Tag Suite) XML format, specifically the NLM (National Library of Medicine) variant. Articles follow the JATS 1.0, 1.1, 1.2, or 1.3 DTD.

### DTD Version Distribution

The PMC archive spans articles from 2003 to present with evolving DTD versions:
- **JATS 1.0** (2012): Older articles, basic structure
- **JATS 1.1** (2015): Added `<kwd-group>` for keywords
- **JATS 1.2** (2019): Added `<counts>` element, CRediT author roles
- **JATS 1.3** (2021): Added `<processing-meta>`, enhanced MathML support

Newer articles have richer structured metadata (e.g., `<counts>` with fig-count, table-count, word-count).

### Document Structure

```xml
<article article-type="research-article">
  <front>           <!-- Journal and article metadata -->
    <journal-meta>  <!-- Journal information -->
    <article-meta>  <!-- Article identifiers, authors, dates, funding -->
  </front>
  <body>            <!-- Article content (sections, paragraphs, figures) -->
  <back>            <!-- References, acknowledgments, appendices -->
  <floats-group>    <!-- Figures and tables (alternative location) -->
</article>
```

## Field Categories

### Currently Extracted Fields (18 fields)

These fields are populated by our Python extraction tools:

| Field | XML Path | Description |
|-------|----------|-------------|
| pmid | `article-id[@pub-id-type='pmid']` | PubMed ID |
| pmcid_pmc | `article-id[@pub-id-type='pmc']` | PMC ID (number only) |
| pmcid_uid | `article-id[@pub-id-type='pmcid']` | Full PMC ID with prefix |
| doi | `article-id[@pub-id-type='doi']` | Digital Object Identifier |
| journal | `journal-title` | Full journal name |
| publisher | `publisher-name` | Publisher name |
| affiliation_institution | `aff/institution` | First institution |
| affiliation_country | `aff/country` | First country |
| year_epub | `pub-date[@pub-type='epub']/year` | E-publication year |
| year_ppub | `pub-date[@pub-type='ppub']/year` | Print publication year |
| type | `article[@article-type]` | Article type |
| coi_text | `fn[@fn-type='COI-statement']` | COI statement text |
| fund_text | `fn[@fn-type='financial-disclosure']` | Funding text |
| fund_pmc_institute | `funding-group/institution` | Funding institution |
| fund_pmc_source | `funding-source` | Funding source |
| fund_pmc_anysource | `funding-group/*` | Any funding info |
| file_size | (calculated) | XML file size in bytes |
| chars_in_body | (calculated) | Character count in body |

### Available But Not Extracted (Priority Fields)

These fields are available in the XML but not currently extracted by Python:

#### Journal Identifiers
- **journal_id_nlm**: NLM journal abbreviation (e.g., "Nat Commun")
- **journal_id_iso**: ISO journal abbreviation
- **issn_print/issn_electronic**: Journal ISSNs

#### Article Content
- **article_title**: Full article title
- **abstract**: Article abstract text
- **subject**: Article categories/keywords

#### Citation Metadata
- **volume, issue, fpage, lpage**: Citation components
- **elocation_id**: Electronic location identifier

#### Author Details
- **author_names**: Full author list with affiliations
- **n_auth**: Author count
- **corresp**: Corresponding author info

#### Transparency Statements
- **data_availability**: Data availability statement (from back matter)
- **code_availability**: Code availability statement
- **ethics_statement**: Ethics approval
- **fund_award_id**: Grant/award numbers

#### Structural Counts
- **n_ref**: Reference count
- **n_fig_body/n_fig_floats**: Figure counts
- **n_table_body/n_table_floats**: Table counts

## Extraction Pipeline

### Current Architecture

```
PMC XML Archives (tar.gz)
         │
         ▼
┌─────────────────────────────────┐
│ Python: extract_from_tarballs.py│  → 18 metadata fields + 104 placeholders
└─────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│ R: rtransparent rt_all_pmc()   │  → Pattern detection (COI, funding, etc.)
└─────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│ R: oddpub open_data_search()   │  → Open data/code detection
└─────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│ Python: create_compact_rtrans.py│  → Merged dataset (142 columns)
└─────────────────────────────────┘
```

### R Package Extraction

The rtransparent R package extracts additional fields internally for pattern matching:
- Abstract text (for registration detection)
- Methods section (for trial registration)
- Acknowledgments (for funding patterns)
- Full body text (character counts)

These are used for analysis but not exported to the final dataset.

### oddpub Processing

The oddpub R package processes body text to detect:
- Open data statements and categories
- Open code statements
- Data/Code Availability Statements (DAS/CAS)
- Reuse statements

## Data Dictionary Files

- `data_dictionary_pmcoa_xml.csv` - Complete XML field inventory (this reference)
- `data_dictionary_rtrans.csv` - rtransparent output schema (122 columns)
- `data_dictionary_oddpub.csv` - oddpub output schema (15 columns)

## Future Enhancements

Priority fields to add to Python extraction:
1. **journal_id_nlm** - For journal-level analysis
2. **article_title** - For deduplication and display
3. **n_auth** - Author count for collaboration analysis
4. **fund_award_id** - Grant number extraction
5. **license_type** - Open access license classification
6. **keywords** - Subject classification (JATS 1.1+)
7. **funder_doi** - Crossref Funder Registry DOI for standardized funder identification
8. **counts** - Figure, table, word counts (JATS 1.2+ only)

## References

- [JATS Tag Library](https://jats.nlm.nih.gov/archiving/)
- [PMC Tagging Guidelines](https://www.ncbi.nlm.nih.gov/pmc/pmcdoc/tagging-guidelines/)
- [NLM Journal Archiving DTD](https://dtd.nlm.nih.gov/)
