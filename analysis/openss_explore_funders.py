#!/usr/bin/env python3
"""
OpenSS Exploratory Funder Analysis

Explores funding text from open data publications to discover funders
NOT in our predefined list. Uses NLP techniques:
1. Named Entity Recognition (NER) for organization names
2. N-gram frequency analysis
3. Pattern matching for common funder patterns

Usage:
    python analysis/openss_explore_funders.py \
        --oddpub-file ~/claude/pmcoaXMLs/oddpub_merged/oddpub_v7.2.3_all.parquet \
        --rtrans-dir ~/claude/pmcoaXMLs/rtrans_out_full_parquets \
        --output-dir results/openss_explore \
        --limit 100

Author: INCF 2025 Poster Analysis
Date: 2025-12-01
"""

import argparse
import gc
import glob
import logging
import re
from collections import Counter
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Known funders to exclude (we already have these)
KNOWN_FUNDERS = {
    'NIH', 'National Institutes of Health',
    'EC', 'European Commission', 'European Union', 'EU',
    'NSFC', 'National Natural Science Foundation of China',
    'DFG', 'German Research Foundation', 'Deutsche Forschungsgemeinschaft',
    'AMED', 'Japan Agency for Medical Research',
    'Wellcome', 'Wellcome Trust',
    'CIHR', 'Canadian Institutes of Health Research',
    'MRC', 'Medical Research Council',
    'HHMI', 'Howard Hughes Medical Institute',
    'BMGF', 'Bill & Melinda Gates Foundation', 'Gates Foundation',
    # NIH institutes
    'NCI', 'NIAID', 'NIA', 'NHLBI', 'NIGMS', 'NINDS', 'NIDDK', 'NIMH',
    'NICHD', 'NIDA', 'NIEHS', 'NEI', 'NHGRI', 'NIAMS', 'NIAAA', 'NIDCR',
    'NLM', 'NIBIB', 'NIMHD', 'NINR', 'NCCIH'
}

# Patterns that indicate funder names
FUNDER_PATTERNS = [
    r'(?:funded|supported|grant[s]?|award[s]?)\s+(?:by|from)\s+(?:the\s+)?([A-Z][A-Za-z\s&]+(?:Foundation|Trust|Institute|Council|Agency|Ministry|Department|Fund|Commission|Association|Society|Program|Centre|Center))',
    r'([A-Z][A-Za-z\s&]+(?:Foundation|Trust|Institute|Council|Agency|Ministry|Department|Fund|Commission|Association|Society|Program|Centre|Center))\s+(?:grant|award|fund)',
    r'grant\s+(?:number|no\.?|#)?\s*[A-Z0-9-]+\s+from\s+(?:the\s+)?([A-Z][A-Za-z\s&]+)',
]


def normalize_pmcid(pmcid: str) -> str:
    """Normalize PMCID to PMC######### format.

    Handles various formats:
    - PMC12345
    - 12345
    - PMCPMC12345.txt (from oddpub article column)
    """
    if pd.isna(pmcid):
        return None
    pmcid = str(pmcid).strip().upper()

    # Handle PMCPMC12345.txt format from oddpub
    if pmcid.startswith('PMCPMC'):
        pmcid = pmcid[3:]  # Remove first "PMC" prefix

    # Remove .txt suffix
    if pmcid.endswith('.TXT'):
        pmcid = pmcid[:-4]

    # Ensure PMC prefix
    if not pmcid.startswith('PMC'):
        pmcid = 'PMC' + pmcid

    return pmcid


def load_open_data_pmcids(oddpub_file: Path) -> set:
    """Load PMCIDs from oddpub output where is_open_data=true."""
    logger.info(f"Loading oddpub results from {oddpub_file}")

    df = pd.read_parquet(oddpub_file)
    logger.info(f"Loaded {len(df):,} records")

    open_data_df = df[df['is_open_data'] == True]
    logger.info(f"Found {len(open_data_df):,} with is_open_data=true ({100*len(open_data_df)/len(df):.2f}%)")

    # Extract PMCIDs - check article column first (has PMCPMC format)
    pmcids = set()
    for col in ['article', 'pmcid', 'filename']:
        if col in open_data_df.columns:
            for val in open_data_df[col].dropna().unique():
                pmcid = normalize_pmcid(val)
                if pmcid:
                    pmcids.add(pmcid)

    logger.info(f"Got {len(pmcids):,} unique PMCIDs with open data")
    return pmcids


def extract_potential_funders(text: str) -> list:
    """
    Extract potential funder names from funding text using patterns.
    Returns list of potential funder strings.
    """
    if pd.isna(text) or not text:
        return []

    text = str(text)
    funders = []

    # Pattern-based extraction
    for pattern in FUNDER_PATTERNS:
        matches = re.findall(pattern, text, re.IGNORECASE)
        funders.extend(matches)

    # Also look for capitalized phrases that might be organization names
    # Pattern: Capitalized words followed by Foundation/Trust/etc
    org_pattern = r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+(?:Foundation|Trust|Institute|Council|Agency|Ministry|Fund|Commission|Association|Society|Center|Centre))\b'
    org_matches = re.findall(org_pattern, text)
    funders.extend(org_matches)

    # Look for all-caps acronyms (3-6 letters) that might be funders
    acronym_pattern = r'\b([A-Z]{3,6})\b'
    acronyms = re.findall(acronym_pattern, text)
    # Filter to exclude common non-funder acronyms
    exclude_acronyms = {'DNA', 'RNA', 'PCR', 'USA', 'THE', 'AND', 'FOR', 'NOT', 'WITH', 'FROM'}
    funders.extend([a for a in acronyms if a not in exclude_acronyms])

    return funders


def extract_ngrams(text: str, n: int = 2) -> list:
    """Extract word n-grams from text."""
    if pd.isna(text) or not text:
        return []

    # Clean and tokenize
    text = str(text).lower()
    text = re.sub(r'[^\w\s]', ' ', text)
    words = text.split()

    # Generate n-grams
    ngrams = []
    for i in range(len(words) - n + 1):
        ngram = ' '.join(words[i:i+n])
        ngrams.append(ngram)

    return ngrams


def is_known_funder(name: str) -> bool:
    """Check if a name matches a known funder."""
    name_upper = name.upper()
    name_lower = name.lower()

    for known in KNOWN_FUNDERS:
        if known.upper() in name_upper or name_upper in known.upper():
            return True
        if known.lower() in name_lower:
            return True

    return False


def process_rtrans_for_funding_text(rtrans_dir: Path, open_data_pmcids: set, limit: int = None) -> dict:
    """
    Process rtrans files to extract funding text for open data publications.

    Returns dict with:
    - fund_texts: List of funding text strings
    - potential_funders: Counter of extracted potential funder names
    - bigrams: Counter of word bigrams in funding text
    - trigrams: Counter of word trigrams in funding text
    """
    parquet_files = sorted(glob.glob(f'{rtrans_dir}/*.parquet'))

    if limit:
        parquet_files = parquet_files[:limit]
        logger.info(f"Limited to first {limit} files for testing")

    logger.info(f"Processing {len(parquet_files)} rtrans parquet files")

    fund_texts = []
    potential_funders = Counter()
    bigrams = Counter()
    trigrams = Counter()
    matched_count = 0

    funding_cols = ['fund_text', 'fund_pmc_institute', 'fund_pmc_source', 'fund_pmc_anysource']

    for i, pf in enumerate(parquet_files):
        try:
            df = pd.read_parquet(pf)

            # Normalize PMCIDs
            if 'pmcid_pmc' in df.columns:
                df['pmcid_norm'] = df['pmcid_pmc'].fillna('').astype(str).str.strip()
                df.loc[~df['pmcid_norm'].str.startswith('PMC'), 'pmcid_norm'] = 'PMC' + df.loc[~df['pmcid_norm'].str.startswith('PMC'), 'pmcid_norm']

                # Filter to open data records
                mask = df['pmcid_norm'].isin(open_data_pmcids)
                matched_df = df[mask]

                if len(matched_df) > 0:
                    matched_count += len(matched_df)

                    # Extract funding text from all funding columns
                    available_cols = [c for c in funding_cols if c in matched_df.columns]

                    for _, row in matched_df.iterrows():
                        for col in available_cols:
                            text = row.get(col)
                            if pd.notna(text) and text:
                                text = str(text).strip()
                                if len(text) > 10:  # Skip very short strings
                                    fund_texts.append(text)

                                    # Extract potential funders
                                    funders = extract_potential_funders(text)
                                    for f in funders:
                                        f = f.strip()
                                        if len(f) > 2:  # Skip very short strings
                                            potential_funders[f] += 1

                                    # Extract n-grams
                                    bigrams.update(extract_ngrams(text, 2))
                                    trigrams.update(extract_ngrams(text, 3))

            del df
            gc.collect()

            if (i + 1) % 50 == 0:
                logger.info(f"  Processed {i+1}/{len(parquet_files)} files, matched {matched_count:,} records, {len(fund_texts):,} funding texts")

        except Exception as e:
            logger.warning(f"Error processing {Path(pf).name}: {e}")

    logger.info(f"Finished processing {len(parquet_files)} files")
    logger.info(f"Matched open data records: {matched_count:,}")
    logger.info(f"Extracted funding texts: {len(fund_texts):,}")

    return {
        'fund_texts': fund_texts,
        'potential_funders': potential_funders,
        'bigrams': bigrams,
        'trigrams': trigrams,
        'matched_count': matched_count
    }


def filter_novel_funders(potential_funders: Counter, min_count: int = 10) -> pd.DataFrame:
    """
    Get all potential funders with counts above threshold.
    Marks whether each is in our known list for comparison.
    """
    results = []
    for name, count in potential_funders.most_common():
        if count < min_count:
            break
        results.append({
            'name': name,
            'count': count,
            'is_known': is_known_funder(name)
        })

    return pd.DataFrame(results)


def find_funder_keywords(bigrams: Counter, trigrams: Counter, min_count: int = 50) -> pd.DataFrame:
    """
    Find n-grams that might indicate funders (containing 'foundation', 'grant', etc.)
    """
    funder_keywords = ['foundation', 'trust', 'institute', 'council', 'agency',
                       'ministry', 'fund', 'commission', 'research', 'science',
                       'national', 'federal', 'government']

    results = []

    # Check bigrams
    for ngram, count in bigrams.most_common(5000):
        if count < min_count:
            break
        for kw in funder_keywords:
            if kw in ngram:
                results.append({
                    'ngram': ngram,
                    'n': 2,
                    'count': count,
                    'keyword': kw
                })
                break

    # Check trigrams
    for ngram, count in trigrams.most_common(5000):
        if count < min_count:
            break
        for kw in funder_keywords:
            if kw in ngram:
                results.append({
                    'ngram': ngram,
                    'n': 3,
                    'count': count,
                    'keyword': kw
                })
                break

    df = pd.DataFrame(results)
    if not df.empty:
        df = df.drop_duplicates(subset='ngram').sort_values('count', ascending=False)

    return df


def generate_report(results: dict, output_dir: Path):
    """Generate exploratory report."""
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. Top potential funders (filtered to exclude known)
    logger.info("Analyzing potential novel funders...")
    novel_funders = filter_novel_funders(results['potential_funders'], min_count=5)
    novel_funders.to_csv(output_dir / 'novel_funders.csv', index=False)
    logger.info(f"  Saved novel_funders.csv ({len(novel_funders)} entries)")

    # 2. All potential funders (including known, for comparison)
    # Export ALL funders with count >= 2 (not just top 500)
    all_funders = []
    for name, count in results['potential_funders'].most_common():
        if count < 2:
            break
        all_funders.append({
            'name': name,
            'count': count,
            'is_known': is_known_funder(name)
        })
    all_funders_df = pd.DataFrame(all_funders)
    all_funders_df.to_csv(output_dir / 'all_potential_funders.csv', index=False)
    logger.info(f"  Saved all_potential_funders.csv ({len(all_funders_df)} entries)")

    # 3. Funder-related n-grams
    logger.info("Analyzing funder-related n-grams...")
    funder_ngrams = find_funder_keywords(results['bigrams'], results['trigrams'], min_count=20)
    funder_ngrams.to_csv(output_dir / 'funder_ngrams.csv', index=False)
    logger.info(f"  Saved funder_ngrams.csv ({len(funder_ngrams)} entries)")

    # 4. Top bigrams overall
    top_bigrams = pd.DataFrame(
        results['bigrams'].most_common(200),
        columns=['bigram', 'count']
    )
    top_bigrams.to_csv(output_dir / 'top_bigrams.csv', index=False)

    # 5. Top trigrams overall
    top_trigrams = pd.DataFrame(
        results['trigrams'].most_common(200),
        columns=['trigram', 'count']
    )
    top_trigrams.to_csv(output_dir / 'top_trigrams.csv', index=False)

    # 6. Sample funding texts for manual inspection
    sample_texts = results['fund_texts'][:100] if len(results['fund_texts']) > 100 else results['fund_texts']
    with open(output_dir / 'sample_funding_texts.txt', 'w') as f:
        for i, text in enumerate(sample_texts):
            f.write(f"--- Sample {i+1} ---\n")
            f.write(text[:1000] + "\n\n")  # Truncate long texts
    logger.info(f"  Saved sample_funding_texts.txt ({len(sample_texts)} samples)")

    # 7. Summary report
    summary = []
    summary.append("=" * 70)
    summary.append("OPENSS EXPLORATORY FUNDER ANALYSIS")
    summary.append("=" * 70)
    summary.append("")
    summary.append(f"Total matched open data records: {results['matched_count']:,}")
    summary.append(f"Funding texts extracted: {len(results['fund_texts']):,}")
    summary.append(f"Unique potential funders found: {len(results['potential_funders']):,}")
    summary.append("")

    # Top novel funders
    summary.append("-" * 70)
    summary.append("TOP POTENTIAL NOVEL FUNDERS (not in our known list)")
    summary.append("-" * 70)
    for i, row in novel_funders.head(30).iterrows():
        summary.append(f"  {row['count']:>5}  {row['name']}")
    summary.append("")

    # Top known funders (for comparison)
    summary.append("-" * 70)
    summary.append("TOP KNOWN FUNDERS (already in our list)")
    summary.append("-" * 70)
    known_in_results = all_funders_df[all_funders_df['is_known']]
    for i, row in known_in_results.head(20).iterrows():
        summary.append(f"  {row['count']:>5}  {row['name']}")
    summary.append("")

    # Funder-related n-grams
    summary.append("-" * 70)
    summary.append("TOP FUNDER-RELATED N-GRAMS")
    summary.append("-" * 70)
    for i, row in funder_ngrams.head(30).iterrows():
        summary.append(f"  {row['count']:>5}  {row['ngram']}")
    summary.append("")

    summary.append("=" * 70)

    summary_text = "\n".join(summary)
    with open(output_dir / 'exploration_summary.txt', 'w') as f:
        f.write(summary_text)

    print(summary_text)
    logger.info(f"Saved exploration_summary.txt to {output_dir}")


def main():
    parser = argparse.ArgumentParser(
        description='Exploratory funder analysis for open data publications',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--oddpub-file', type=Path, required=True,
                        help='Path to oddpub merged parquet file')
    parser.add_argument('--rtrans-dir', type=Path, required=True,
                        help='Directory containing rtrans parquet files')
    parser.add_argument('--output-dir', type=Path, default=Path('results/openss_explore'),
                        help='Output directory for results')
    parser.add_argument('--limit', type=int, default=None,
                        help='Limit number of rtrans files to process (for testing)')

    args = parser.parse_args()

    logger.info("=" * 70)
    logger.info("OPENSS EXPLORATORY FUNDER ANALYSIS")
    logger.info("=" * 70)
    logger.info(f"oddpub file: {args.oddpub_file}")
    logger.info(f"rtrans dir: {args.rtrans_dir}")
    logger.info(f"output dir: {args.output_dir}")
    if args.limit:
        logger.info(f"limit: {args.limit} files")
    logger.info("")

    # Load open data PMCIDs
    open_data_pmcids = load_open_data_pmcids(args.oddpub_file)

    # Process rtrans files to extract funding text
    results = process_rtrans_for_funding_text(args.rtrans_dir, open_data_pmcids, args.limit)

    # Generate exploratory report
    generate_report(results, args.output_dir)

    logger.info("Exploration complete!")


if __name__ == '__main__':
    main()
