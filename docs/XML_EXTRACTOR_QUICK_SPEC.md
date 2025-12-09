# Quick XML Extractor Implementation Plan

## Goal
Add journal and publisher fields to existing metadata extraction for poster analysis. Target: Complete in 1-2 days.

## Minimum Viable Fields (Just what's needed for poster)

```xml
<!-- Extract these 4 essential fields -->
<journal-id journal-id-type="nlm-ta">PLoS One</journal-id>  <!-- journal_id_nlm -->
<journal-title>PLOS ONE</journal-title>                      <!-- journal_title -->
<publisher-name>Public Library of Science</publisher-name>   <!-- publisher_name -->
<subject>Research Article</subject>                          <!-- article_type -->
```

## Implementation Options (Pick fastest)

### Option A: Modify existing extract_from_tarballs.py (4-6 hours)
- Add 4 fields to existing extractor
- Test locally, build container, run on HPC
- Pros: Integrated solution
- Cons: Risk breaking working code

### Option B: Standalone extractor (2-4 hours)
- New script that ONLY extracts journal/publisher fields + pmcid
- Output: pmcid, journal_id_nlm, journal_title, publisher_name, article_type
- Join with existing data later
- Pros: Fast, no risk to existing pipeline
- Cons: Extra join step

### Option C: Quick and dirty (1-2 hours)
- Simple Python script, no container
- Run on subset of data (enough for poster)
- Extract just top 20 journals/publishers
- Pros: Immediate results
- Cons: Not complete dataset

## Recommended: Option B (Standalone Extractor)

### Quick Implementation

```python
#!/usr/bin/env python3
"""quick_journal_extractor.py - Extract journal info from PMC XMLs"""

import tarfile
import pandas as pd
from lxml import etree
import sys

def extract_journal_info(xml_content, pmcid):
    """Extract only what we need for the poster."""
    try:
        root = etree.fromstring(xml_content)

        # Simple XPath queries
        journal_id = root.findtext(".//journal-id[@journal-id-type='nlm-ta']", "")
        journal_title = root.findtext(".//journal-title", "")
        publisher = root.findtext(".//publisher-name", "")
        article_type = root.findtext(".//subject", "")

        return {
            'pmcid_pmc': pmcid,
            'journal_id_nlm': journal_id,
            'journal_title': journal_title,
            'publisher_name': publisher,
            'article_type': article_type
        }
    except:
        return None

def process_tarfile(tar_path):
    """Stream process tar.gz file."""
    results = []

    with tarfile.open(tar_path, 'r:gz') as tar:
        for member in tar:
            if member.name.endswith('.xml'):
                pmcid = member.name.split('/')[-1].replace('.xml', '')
                xml_content = tar.extractfile(member).read()

                data = extract_journal_info(xml_content, pmcid)
                if data:
                    results.append(data)

                if len(results) >= 10000:
                    yield pd.DataFrame(results)
                    results = []

    if results:
        yield pd.DataFrame(results)

# Main
if __name__ == "__main__":
    tar_path = sys.argv[1]
    output_path = sys.argv[2]

    dfs = []
    for df in process_tarfile(tar_path):
        dfs.append(df)

    final_df = pd.concat(dfs, ignore_index=True)
    final_df.to_parquet(output_path)
    print(f"Extracted {len(final_df)} records")
```

### Container (if needed)

```dockerfile
FROM python:3.11-slim
RUN pip install pandas pyarrow lxml
COPY quick_journal_extractor.py /scripts/
WORKDIR /data
```

### Testing Steps (30 minutes)

1. **Local test (5 min)**:
   ```bash
   python quick_journal_extractor.py test.tar.gz test_out.parquet
   python -c "import pandas as pd; print(pd.read_parquet('test_out.parquet').head())"
   ```

2. **Full test on one file (10 min)**:
   ```bash
   python quick_journal_extractor.py oa_comm_xml.PMC000xxxxxx.baseline.2025-06-26.tar.gz out1.parquet
   ```

3. **Quick container test (15 min)**:
   - Build on curium
   - Test on interactive node
   - If works, proceed to swarm

### Swarm for Full Dataset

```bash
# Generate simple swarm
for f in $HPC_PMCOA_BASE_DIR/pmcoa/files/*baseline*.tar.gz; do
    base=$(basename $f .tar.gz)
    echo "python3 quick_journal_extractor.py $f $HPC_BASE_DIR/osm/journal_extract/${base}.parquet"
done > journal_extract_swarm.txt
```

### Merge with Existing Data (30 minutes)

```python
# After extraction completes
import pandas as pd
import glob

# Read all journal extracts
journal_files = glob.glob('/path/to/journal_extract/*.parquet')
journal_df = pd.concat([pd.read_parquet(f) for f in journal_files])

# Read compact_rtrans data
compact_df = pd.read_parquet('compact_rtrans.parquet')

# Merge
final_df = compact_df.merge(journal_df, on='pmcid_pmc', how='left')

# Save
final_df.to_parquet('compact_rtrans_with_journals.parquet')
```

## Timeline

**Day 1 (Today)**:
- Hour 1-2: Write and test script locally
- Hour 3-4: Test on full baseline file
- Hour 5-6: Build container if needed, submit HPC jobs

**Day 2**:
- Morning: Check results, debug if needed
- Afternoon: Merge data, create poster visualizations

## What We Can Show on Poster

With journal/publisher data, we can add:

1. **Top 20 Journals by Data Sharing Rate**
   - Bar chart showing which journals have highest open data %
   - Compare Nature, Science, PLOS, etc.

2. **Publisher Analysis**
   - Group by publisher (Elsevier, Springer, PLOS, etc.)
   - Show data sharing trends by publisher size

3. **Article Type Analysis**
   - Research articles vs reviews vs case reports
   - Data sharing by article type

4. **Journal Prestige vs Data Sharing**
   - If we get impact factors later
   - Correlation analysis

## Go/No-Go Decision Points

1. **After local test** (1 hour): Does it extract correctly?
2. **After full file test** (2 hours): Is it fast enough?
3. **After HPC test** (4 hours): Will it complete overnight?

If any fail, switch to Option C (sample data only) for poster.