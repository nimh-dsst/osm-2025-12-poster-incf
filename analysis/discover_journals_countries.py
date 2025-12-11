#!/usr/bin/env python3
"""
Discover Significant Journals and Countries from Open Data Publications

Uses Weibull distribution-based selection, appropriate for power-law data
where mean/std assumptions fail.

Selection Method:
  - Fit Weibull distribution to count data
  - Use scale parameter (λ) as threshold
  - F(λ) = 1 - e^(-1) ≈ 63.2%, so entities with count >= λ are the top ~36.8%

JOURNALS:
  Direct Weibull fit on journal counts

COUNTRIES (noisy data, multi-stage pipeline):
  Stage 1: Count with alias normalization (U.S. → USA, etc.)
  Stage 2: Noise removal (city names, fragments, postal codes)
  Stage 3: Weibull fit on cleaned counts

Additionally outputs COVERAGE ANALYSIS showing how many entities
are needed to cover 50%, 80%, 90% of open data articles.

Filters to is_open_data=true subset from oddpub v7.2.3.

Usage:
    python analysis/discover_journals_countries.py \
        --oddpub-file ~/claude/pmcoaXMLs/oddpub_merged/oddpub_v7.2.3_all.parquet \
        --rtrans-dir ~/claude/pmcoaXMLs/rtrans_out_full_parquets \
        --output-dir results/openss_discovery_weibull

Author: INCF 2025 Poster Analysis
Date: 2025-12-11
"""

import argparse
import logging
import os
import re
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# Noise patterns for countries - things that are NOT countries
COUNTRY_NOISE_PATTERNS = [
    r'^\d+$',                    # Pure numbers (postal codes)
    r'^\d{4,}',                  # Starts with 4+ digit numbers
    r'^[A-Z]{1,2}\d',            # UK postal codes like "SW1"
    r'\d{5,}',                   # Contains 5+ digit numbers
]

COUNTRY_NOISE_EXACT = {
    # Single words that are not countries
    'And', 'The', 'Of', 'In', 'For', 'To', 'From',
    # City names commonly misidentified
    'London', 'Paris', 'Berlin', 'Tokyo', 'Beijing', 'Shanghai',
    'Wuhan', 'Nanjing', 'Chengdu', 'Guilin', 'Hefei',
    'Moscow', 'Stockholm', 'Milan', 'Rome', 'Barcelona', 'Madrid', 'Valencia',
    'Edinburgh', 'Oxford', 'Cambridge', 'Leeds', 'Bath', 'Nottingham',
    'Sheffield', 'Exeter', 'Coventry', 'Manchester', 'Bristol',
    'Seattle', 'Chicago', 'Tampa', 'Toronto', 'Stanford',
    'Grenoble', 'Lisbon', 'Frankfurt', 'Hanover', 'Jena',
    'Göttingen', 'Tübingen', 'Umeå', 'Sète', 'Varese',
    'Sapporo', 'Kochi',
    # US states commonly misidentified
    'California', 'Virginia', 'Texas', 'Pennsylvania', 'Maryland',
    'Illinois', 'Florida', 'Colorado', 'Michigan', 'Ohio',
    'Wisconsin', 'Tennessee', 'Connecticut', 'Nebraska', 'Minnesota',
    'Missouri', 'Oregon', 'Arizona', 'Washington', 'Alaska', 'Hawaii',
    'New Jersey', 'New York', 'North Carolina', 'South Carolina', 'South Dakota',
    'Manitoba',
    # Fragments and artifacts
    'Faso', 'Kingdom', 'Arabia', 'Emirates', 'Republic', 'Democratic',
    'Federal', 'United', 'People', 'National',
    # Numeric/code fragments
    'Nci', 'Roc', 'Ksa',
    # University names
    'Soonchunhyang University', 'The University Of Queensland',
    # Other non-countries
    'These Authors Contributed Equally To This Work.',
    'New', 'Zealand',  # Fragments of "New Zealand"
    'Rea', 'Crescent', 'Tyne',
    '-Cent', 'T Bsc-',
    # Very short entries
    'Ma.', 'D.C.',
}

# Country normalization mapping
COUNTRY_NORMALIZE = {
    'CHINA': 'China',
    'P.R. CHINA': 'China',
    'PRC': 'China',
    'UNITED STATES': 'USA',
    'U.S.A': 'USA',
    'U.S.': 'USA',
    'US': 'USA',
    'UNITED KINGDOM': 'UK',
    'U.K.': 'UK',
    'UK': 'UK',
    'ENGLAND': 'UK',
    'SCOTLAND': 'UK',
    'WALES': 'UK',
    'NORTHERN IRELAND': 'UK',
    'GERMANY': 'Germany',
    'DEUTSCHLAND': 'Germany',
    'JAPAN': 'Japan',
    'FRANCE': 'France',
    'CANADA': 'Canada',
    'AUSTRALIA': 'Australia',
    'ITALY': 'Italy',
    'ITALIA': 'Italy',
    'SPAIN': 'Spain',
    'ESPAÑA': 'Spain',
    'NETHERLANDS': 'Netherlands',
    'HOLLAND': 'Netherlands',
    'THE NETHERLANDS': 'Netherlands',
    'SWITZERLAND': 'Switzerland',
    'SCHWEIZ': 'Switzerland',
    'SOUTH KOREA': 'South Korea',
    'KOREA, REPUBLIC OF': 'South Korea',
    'REPUBLIC OF KOREA': 'South Korea',
    'KOREA': 'South Korea',
    'INDIA': 'India',
    'BRAZIL': 'Brazil',
    'BRASIL': 'Brazil',
    'SWEDEN': 'Sweden',
    'BELGIUM': 'Belgium',
    'AUSTRIA': 'Austria',
    'POLAND': 'Poland',
    'DENMARK': 'Denmark',
    'NORWAY': 'Norway',
    'FINLAND': 'Finland',
    'ISRAEL': 'Israel',
    'SINGAPORE': 'Singapore',
    'TAIWAN': 'Taiwan',
    'MEXICO': 'Mexico',
    'MÉXICO': 'Mexico',
    'PORTUGAL': 'Portugal',
    'IRAN': 'Iran',
    'TURKEY': 'Turkey',
    'TÜRKIYE': 'Turkey',
    'RUSSIA': 'Russia',
    'RUSSIAN FEDERATION': 'Russia',
    'GREECE': 'Greece',
    'IRELAND': 'Ireland',
    'CZECH REPUBLIC': 'Czech Republic',
    'CZECHIA': 'Czech Republic',
    'NEW ZEALAND': 'New Zealand',
    'ARGENTINA': 'Argentina',
    'CHILE': 'Chile',
    'THAILAND': 'Thailand',
    'MALAYSIA': 'Malaysia',
    'SOUTH AFRICA': 'South Africa',
    'EGYPT': 'Egypt',
    'NIGERIA': 'Nigeria',
    'SAUDI ARABIA': 'Saudi Arabia',
    'PAKISTAN': 'Pakistan',
    'INDONESIA': 'Indonesia',
    'VIETNAM': 'Vietnam',
    'VIET NAM': 'Vietnam',
    'PHILIPPINES': 'Philippines',
    'COLOMBIA': 'Colombia',
    'PERU': 'Peru',
    'CZECH': 'Czech Republic',
    'HUNGARY': 'Hungary',
    'ROMANIA': 'Romania',
    'UKRAINE': 'Ukraine',
    'HONG KONG': 'Hong Kong',
}


def is_country_noise(name: str) -> bool:
    """Check if a country name is actually noise (not a real country)."""
    if not name or len(name.strip()) < 2:
        return True

    name = name.strip()

    # Check exact matches
    if name in COUNTRY_NOISE_EXACT:
        return True
    if name.title() in COUNTRY_NOISE_EXACT:
        return True

    # Check regex patterns
    for pattern in COUNTRY_NOISE_PATTERNS:
        if re.match(pattern, name):
            return True

    # Check for entries with commas (city, country format) - keep the country part
    if ',' in name and not name.startswith('"'):
        return True  # Will be handled separately in normalize_country

    # Check for entries that are just numbers after stripping
    stripped = re.sub(r'[^\w]', '', name)
    if stripped.isdigit():
        return True

    return False


def calculate_coverage(counts: dict, target_percentages: list = [50, 80, 90, 95]) -> dict:
    """Calculate how many entities needed to cover X% of total.

    Returns dict with coverage analysis.
    """
    if not counts:
        return {}

    # Sort by count descending
    sorted_items = sorted(counts.items(), key=lambda x: -x[1])
    total = sum(counts.values())

    coverage = {}
    cumulative = 0

    for i, (name, count) in enumerate(sorted_items):
        cumulative += count
        pct = (cumulative / total) * 100

        # Record when we pass each target
        for target in target_percentages:
            key = f'n_for_{target}pct'
            if key not in coverage and pct >= target:
                coverage[key] = i + 1
                coverage[f'actual_{target}pct'] = round(pct, 2)

    coverage['total_entities'] = len(counts)
    coverage['total_count'] = total

    # Also record top N coverage
    for n in [10, 20, 30, 50]:
        if len(sorted_items) >= n:
            top_n_sum = sum(c for _, c in sorted_items[:n])
            coverage[f'top_{n}_coverage'] = round((top_n_sum / total) * 100, 2)

    return coverage


def normalize_pmcid(pmcid: str) -> str:
    """Normalize PMCID to PMC######### format."""
    if pd.isna(pmcid):
        return None
    pmcid = str(pmcid).strip().upper()

    # Handle PMCPMC12345.txt format from oddpub
    if pmcid.startswith('PMCPMC'):
        pmcid = pmcid[3:]

    # Remove .txt suffix
    if pmcid.endswith('.TXT'):
        pmcid = pmcid[:-4]

    # Ensure PMC prefix
    if not pmcid.startswith('PMC'):
        pmcid = 'PMC' + pmcid

    return pmcid


def clean_journal_name(name: str) -> str:
    """Clean and normalize journal name."""
    if pd.isna(name) or not name:
        return None
    name = str(name).strip()
    name = name.rstrip('.')
    name = re.sub(r'\s+', ' ', name)
    return name if name else None


def normalize_country(country_str: str) -> list:
    """Normalize country strings, handling semicolon-separated lists.

    Returns list of normalized country names.
    """
    if pd.isna(country_str) or not country_str:
        return []

    # Split by semicolon if present
    parts = str(country_str).split(';')
    countries = []

    for part in parts:
        part = part.strip()
        part_upper = part.upper()

        if part_upper in COUNTRY_NORMALIZE:
            countries.append(COUNTRY_NORMALIZE[part_upper])
        elif part_upper not in ['', 'NONE', 'NAN', 'NA']:
            # Check for partial matches
            found = False
            for pattern, normalized in COUNTRY_NORMALIZE.items():
                if pattern in part_upper:
                    countries.append(normalized)
                    found = True
                    break
            if not found and len(part) > 2:
                countries.append(part.title())

    return countries


def apply_sigma_threshold(counts: dict, sigma: float = 4.0) -> dict:
    """Apply sigma threshold on log-scale counts.

    Returns dict with items above threshold and statistics.
    """
    if not counts:
        return {'selected': {}, 'stats': {}}

    values = np.array(list(counts.values()))
    log_values = np.log10(values + 1)  # +1 to avoid log(0)

    mean_log = log_values.mean()
    std_log = log_values.std()
    threshold_log = mean_log + sigma * std_log
    threshold_count = 10 ** threshold_log - 1  # Reverse the +1

    selected = {k: v for k, v in counts.items() if v >= threshold_count}

    stats = {
        'total_unique': len(counts),
        'log_mean': mean_log,
        'log_std': std_log,
        'threshold_log': threshold_log,
        'threshold_count': int(threshold_count),
        'selected_count': len(selected),
        'sigma': sigma,
    }

    return {'selected': selected, 'stats': stats}


def apply_weibull_threshold(counts: dict) -> dict:
    """Apply Weibull distribution-based threshold.

    Fits a Weibull distribution to the count data and uses the scale
    parameter (λ) as the threshold. For Weibull, F(λ) = 1 - e^(-1) ≈ 63.2%,
    meaning ~63.2% of the cumulative distribution mass is below λ.

    Entities with count >= λ are selected (the significant ~36.8%).

    This is appropriate for power-law distributed data where mean/std
    assumptions fail.

    Returns dict with selected items, threshold, and fit statistics.
    """
    if not counts:
        return {'selected': {}, 'stats': {}}

    values = np.array(list(counts.values()), dtype=float)

    # Fit Weibull distribution (uses MLE)
    # scipy.stats.weibull_min: shape (c), loc, scale
    # We fix loc=0 since counts are positive
    shape, loc, scale = stats.weibull_min.fit(values, floc=0)

    # The scale parameter λ is our threshold
    # F(λ) = 1 - exp(-1) ≈ 0.632, so ~36.8% of values exceed λ
    threshold = scale

    # Select entities above threshold
    selected = {k: v for k, v in counts.items() if v >= threshold}

    # Calculate goodness of fit (KS test)
    ks_stat, ks_pvalue = stats.kstest(values, 'weibull_min', args=(shape, loc, scale))

    # Calculate actual coverage (what % of total count is in selected)
    total_count = sum(counts.values())
    selected_count_sum = sum(selected.values())
    coverage_pct = (selected_count_sum / total_count) * 100 if total_count > 0 else 0

    fit_stats = {
        'total_unique': len(counts),
        'weibull_shape': round(shape, 4),
        'weibull_scale': round(scale, 2),
        'threshold_count': int(np.ceil(threshold)),
        'selected_count': len(selected),
        'expected_pct_above': 36.8,  # 1 - F(λ) = e^(-1) ≈ 0.368
        'actual_pct_above': round(len(selected) / len(counts) * 100, 2),
        'coverage_pct': round(coverage_pct, 2),
        'ks_statistic': round(ks_stat, 4),
        'ks_pvalue': round(ks_pvalue, 4),
        'selection_method': 'weibull',
    }

    return {'selected': selected, 'stats': fit_stats}


def load_open_data_pmcids(oddpub_file: str) -> set:
    """Load PMCIDs with is_open_data=true from oddpub results."""
    logger.info(f"Loading oddpub results from {oddpub_file}")

    df = pd.read_parquet(oddpub_file)
    logger.info(f"Loaded {len(df):,} records")

    # Filter to open data
    open_data_df = df[df['is_open_data'] == True]
    logger.info(f"Found {len(open_data_df):,} with is_open_data=true ({len(open_data_df)/len(df)*100:.2f}%)")

    # Get unique PMCIDs
    pmcids = set()
    for col in ['pmcid', 'article', 'filename']:
        if col in open_data_df.columns:
            for val in open_data_df[col].dropna().unique():
                pmcid = normalize_pmcid(val)
                if pmcid:
                    pmcids.add(pmcid)

    logger.info(f"Got {len(pmcids):,} unique PMCIDs with open data")
    return pmcids


def process_rtrans_files(
    rtrans_dir: str,
    open_data_pmcids: set,
    limit: int = None
) -> pd.DataFrame:
    """Process rtrans parquet files and extract journal/country data."""

    rtrans_path = Path(rtrans_dir)
    parquet_files = sorted(rtrans_path.glob("*.parquet"))

    if limit:
        parquet_files = parquet_files[:limit]

    logger.info(f"Processing {len(parquet_files)} rtrans parquet files")

    all_records = []

    for i, pfile in enumerate(parquet_files):
        try:
            df = pd.read_parquet(pfile)

            # Find PMCID column
            pmcid_col = None
            for col in ['pmcid_pmc', 'pmcid', 'pmcid_uid']:
                if col in df.columns:
                    pmcid_col = col
                    break

            if not pmcid_col:
                continue

            # Normalize PMCIDs and filter to open data subset
            df['pmcid_norm'] = df[pmcid_col].apply(normalize_pmcid)
            matched = df[df['pmcid_norm'].isin(open_data_pmcids)]

            if len(matched) > 0:
                for _, row in matched.iterrows():
                    record = {
                        'pmcid': row['pmcid_norm'],
                        'journal': clean_journal_name(row.get('journal')),
                        'publisher': row.get('publisher'),
                        'country': row.get('affiliation_country'),
                    }
                    all_records.append(record)

            if (i + 1) % 200 == 0:
                logger.info(f"  Processed {i+1}/{len(parquet_files)} files, collected {len(all_records):,} records")

        except Exception as e:
            logger.warning(f"Error processing {pfile}: {e}")
            continue

    logger.info(f"Collected {len(all_records):,} records total")
    return pd.DataFrame(all_records)


def analyze_journals(df: pd.DataFrame) -> dict:
    """Analyze journal distribution using Weibull-based selection.

    Stage 1: Count all journals
    Stage 2: Fit Weibull distribution and use scale parameter (λ) as threshold
             This selects journals above the 63.2nd percentile (~36.8% of entities)
    Stage 3: Calculate coverage statistics

    Returns dict with selected journals, statistics, and coverage analysis.
    """
    logger.info("Analyzing journals with Weibull-based selection...")

    # Count journals
    journal_counts = Counter(df['journal'].dropna())
    logger.info(f"  Stage 1: {len(journal_counts):,} unique journals")

    # Get publisher for each journal
    journal_publisher = {}
    for _, row in df.dropna(subset=['journal']).iterrows():
        journal = row['journal']
        publisher = row.get('publisher')
        if journal not in journal_publisher and publisher:
            journal_publisher[journal] = publisher

    # Stage 2: Apply Weibull threshold
    result = apply_weibull_threshold(dict(journal_counts))
    w_stats = result['stats']
    logger.info(f"  Stage 2 (Weibull fit): shape={w_stats['weibull_shape']:.3f}, scale={w_stats['weibull_scale']:.1f}")
    logger.info(f"  Threshold (λ): count >= {w_stats['threshold_count']:,}")
    logger.info(f"  Selected: {w_stats['selected_count']:,} journals ({w_stats['actual_pct_above']:.1f}% of entities, {w_stats['coverage_pct']:.1f}% of articles)")

    # Stage 3: Calculate coverage
    coverage = calculate_coverage(dict(journal_counts))
    result['coverage'] = coverage
    logger.info(f"  Coverage: Top 10 = {coverage.get('top_10_coverage', 'N/A')}%, Top 20 = {coverage.get('top_20_coverage', 'N/A')}%")

    # Build dataframe with publisher info
    rows = []
    for journal, count in sorted(result['selected'].items(), key=lambda x: -x[1]):
        rows.append({
            'journal': journal,
            'publisher': journal_publisher.get(journal, ''),
            'count': count,
        })

    result['dataframe'] = pd.DataFrame(rows)
    result['all_journals'] = journal_counts
    result['journal_publisher'] = journal_publisher

    return result


def analyze_countries(df: pd.DataFrame) -> dict:
    """Analyze country distribution using Weibull-based selection.

    Stage 1: Count all countries (with normalization via COUNTRY_NORMALIZE)
    Stage 2: Remove noise (city names, fragments, codes)
    Stage 3: Fit Weibull distribution and use scale parameter (λ) as threshold
             This selects countries above the 63.2nd percentile (~36.8% of entities)

    Returns dict with selected countries, statistics, and pipeline details.
    """
    logger.info("Analyzing countries with Weibull-based selection...")

    # Stage 1: Count all raw countries
    raw_country_counts = Counter()
    for country_str in df['country'].dropna():
        countries = normalize_country(country_str)
        for country in set(countries):
            raw_country_counts[country] += 1

    logger.info(f"  Stage 1 (raw counts after normalization): {len(raw_country_counts):,} unique countries")

    # Stage 2: Remove noise
    clean_country_counts = Counter()
    noise_removed = []
    for country, count in raw_country_counts.items():
        if is_country_noise(country):
            noise_removed.append((country, count))
        else:
            clean_country_counts[country] += count

    logger.info(f"  Stage 2 (noise removal): {len(clean_country_counts):,} countries ({len(noise_removed)} noise entries removed)")

    # Stage 3: Apply Weibull threshold on cleaned data
    weibull_result = apply_weibull_threshold(dict(clean_country_counts))
    w_stats = weibull_result['stats']
    logger.info(f"  Stage 3 (Weibull fit): shape={w_stats['weibull_shape']:.3f}, scale={w_stats['weibull_scale']:.1f}")
    logger.info(f"  Threshold (λ): count >= {w_stats['threshold_count']:,}")
    logger.info(f"  Selected: {w_stats['selected_count']:,} countries ({w_stats['actual_pct_above']:.1f}% of entities, {w_stats['coverage_pct']:.1f}% of articles)")

    # Calculate additional coverage stats
    coverage = calculate_coverage(dict(clean_country_counts))
    logger.info(f"  Coverage: Top 10 = {coverage.get('top_10_coverage', 'N/A')}%, Top 20 = {coverage.get('top_20_coverage', 'N/A')}%")

    # Build result
    result = {
        'selected': weibull_result['selected'],
        'stats': {
            'total_raw': len(raw_country_counts),
            'after_noise_removal': len(clean_country_counts),
            'noise_removed': len(noise_removed),
            'weibull_shape': w_stats['weibull_shape'],
            'weibull_scale': w_stats['weibull_scale'],
            'threshold_count': w_stats['threshold_count'],
            'selected_count': w_stats['selected_count'],
            'expected_pct_above': w_stats['expected_pct_above'],
            'actual_pct_above': w_stats['actual_pct_above'],
            'coverage_pct': w_stats['coverage_pct'],
            'ks_statistic': w_stats['ks_statistic'],
            'ks_pvalue': w_stats['ks_pvalue'],
            'selection_method': 'weibull',
        },
        'coverage': coverage,
        'noise_removed': noise_removed[:50],  # Keep top 50 for reference
    }

    # Build dataframe
    rows = []
    for country, count in sorted(result['selected'].items(), key=lambda x: -x[1]):
        rows.append({
            'country': country,
            'count': count,
        })

    result['dataframe'] = pd.DataFrame(rows)
    result['all_countries'] = clean_country_counts

    return result


def save_results(
    journal_result: dict,
    country_result: dict,
    output_dir: str,
):
    """Save analysis results to files."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Save significant journals (above threshold)
    journal_result['dataframe'].to_csv(
        output_path / 'significant_journals.csv', index=False
    )

    # Save all journals for reference
    all_journals_df = pd.DataFrame([
        {
            'journal': j,
            'publisher': journal_result['journal_publisher'].get(j, ''),
            'count': c
        }
        for j, c in sorted(journal_result['all_journals'].items(), key=lambda x: -x[1])
    ])
    all_journals_df.to_csv(output_path / 'all_journals.csv', index=False)

    # Save significant countries (above threshold)
    country_result['dataframe'].to_csv(
        output_path / 'significant_countries.csv', index=False
    )

    # Save all countries for reference
    all_countries_df = pd.DataFrame([
        {'country': c, 'count': n}
        for c, n in sorted(country_result['all_countries'].items(), key=lambda x: -x[1])
    ])
    all_countries_df.to_csv(output_path / 'all_countries.csv', index=False)

    # Write comprehensive summary
    with open(output_path / 'discovery_summary.txt', 'w') as f:
        f.write("=" * 70 + "\n")
        f.write("JOURNAL AND COUNTRY DISCOVERY (WEIBULL-BASED SELECTION)\n")
        f.write("=" * 70 + "\n\n")

        f.write("Selection Method:\n")
        f.write("  Fit Weibull distribution to count data\n")
        f.write("  Threshold = scale parameter (λ)\n")
        f.write("  F(λ) = 1 - e^(-1) ≈ 63.2% → selects top ~36.8% of entities\n\n")

        # Journal stats
        j_stats = journal_result['stats']
        j_cov = journal_result.get('coverage', {})

        f.write("JOURNALS\n")
        f.write("-" * 40 + "\n")
        f.write(f"Total unique journals: {j_stats['total_unique']:,}\n")
        f.write(f"Weibull shape (k): {j_stats['weibull_shape']:.4f}\n")
        f.write(f"Weibull scale (λ): {j_stats['weibull_scale']:.2f}\n")
        f.write(f"Threshold: count >= {j_stats['threshold_count']:,}\n")
        f.write(f"Selected: {j_stats['selected_count']:,} ({j_stats['actual_pct_above']:.1f}% of entities)\n")
        f.write(f"Coverage: {j_stats['coverage_pct']:.1f}% of articles\n")
        f.write(f"KS test: stat={j_stats['ks_statistic']:.4f}, p={j_stats['ks_pvalue']:.4f}\n\n")

        f.write("Coverage Analysis:\n")
        f.write(f"  Top 10 journals: {j_cov.get('top_10_coverage', 'N/A')}% of articles\n")
        f.write(f"  Top 20 journals: {j_cov.get('top_20_coverage', 'N/A')}% of articles\n")
        f.write(f"  Top 30 journals: {j_cov.get('top_30_coverage', 'N/A')}% of articles\n")
        f.write(f"  Journals for 50% coverage: {j_cov.get('n_for_50pct', 'N/A')}\n")
        f.write(f"  Journals for 80% coverage: {j_cov.get('n_for_80pct', 'N/A')}\n\n")

        f.write("Top 30 Significant Journals:\n")
        f.write("-" * 40 + "\n")
        for _, row in journal_result['dataframe'].head(30).iterrows():
            f.write(f"  {row['count']:>6,}  {row['journal'][:50]}\n")
        f.write("\n")

        # Country stats
        c_stats = country_result['stats']
        c_cov = country_result.get('coverage', {})

        f.write("COUNTRIES (Noise Removal + Weibull)\n")
        f.write("-" * 40 + "\n")
        f.write(f"Stage 1 - Raw (after normalization): {c_stats['total_raw']:,}\n")
        f.write(f"Stage 2 - After noise removal: {c_stats['after_noise_removal']:,} (-{c_stats['noise_removed']} noise)\n")
        f.write(f"Stage 3 - Weibull threshold:\n")
        f.write(f"  Shape (k): {c_stats['weibull_shape']:.4f}\n")
        f.write(f"  Scale (λ): {c_stats['weibull_scale']:.2f}\n")
        f.write(f"  Threshold: count >= {c_stats['threshold_count']:,}\n")
        f.write(f"  Selected: {c_stats['selected_count']:,} ({c_stats['actual_pct_above']:.1f}% of entities)\n")
        f.write(f"  Coverage: {c_stats['coverage_pct']:.1f}% of articles\n")
        f.write(f"  KS test: stat={c_stats['ks_statistic']:.4f}, p={c_stats['ks_pvalue']:.4f}\n\n")

        f.write("Coverage Analysis:\n")
        f.write(f"  Top 10 countries: {c_cov.get('top_10_coverage', 'N/A')}% of articles\n")
        f.write(f"  Top 20 countries: {c_cov.get('top_20_coverage', 'N/A')}% of articles\n")
        f.write(f"  Top 30 countries: {c_cov.get('top_30_coverage', 'N/A')}% of articles\n")
        f.write(f"  Countries for 50% coverage: {c_cov.get('n_for_50pct', 'N/A')}\n")
        f.write(f"  Countries for 80% coverage: {c_cov.get('n_for_80pct', 'N/A')}\n\n")

        f.write("Top 30 Significant Countries:\n")
        f.write("-" * 40 + "\n")
        for _, row in country_result['dataframe'].head(30).iterrows():
            f.write(f"  {row['count']:>6,}  {row['country']}\n")

        # Show some noise that was removed
        f.write("\n")
        f.write("Sample Noise Removed (top 20 by count):\n")
        f.write("-" * 40 + "\n")
        noise = sorted(country_result.get('noise_removed', []), key=lambda x: -x[1])[:20]
        for name, count in noise:
            f.write(f"  {count:>6,}  {name}\n")

        f.write("\n" + "=" * 70 + "\n")

    logger.info(f"Results saved to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description='Discover significant journals and countries from open data publications',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
    python analysis/discover_journals_countries.py \\
        --oddpub-file ~/claude/pmcoaXMLs/oddpub_merged/oddpub_v7.2.3_all.parquet \\
        --rtrans-dir ~/claude/pmcoaXMLs/rtrans_out_full_parquets \\
        --output-dir results/openss_discovery_weibull

Weibull-Based Selection Strategy:
  Fits Weibull distribution to count data, uses scale parameter (λ) as threshold.
  F(λ) = 1 - e^(-1) ≈ 63.2%, so entities with count >= λ represent the top ~36.8%.

  This is appropriate for power-law distributed data where mean/std assumptions fail.

  JOURNALS: Direct Weibull fit on journal counts
  COUNTRIES: Noise removal → Weibull fit on cleaned counts
        """
    )
    parser.add_argument(
        '--oddpub-file',
        default='~/claude/pmcoaXMLs/oddpub_merged/oddpub_v7.2.3_all.parquet',
        help='Path to merged oddpub parquet file'
    )
    parser.add_argument(
        '--rtrans-dir',
        default='~/claude/pmcoaXMLs/rtrans_out_full_parquets',
        help='Directory containing rtrans parquet files'
    )
    parser.add_argument(
        '--output-dir',
        default='results/openss_discovery_weibull',
        help='Output directory for results'
    )
    parser.add_argument(
        '--limit', type=int, default=None,
        help='Limit number of rtrans files to process (for testing)'
    )

    args = parser.parse_args()

    # Expand paths
    oddpub_file = os.path.expanduser(args.oddpub_file)
    rtrans_dir = os.path.expanduser(args.rtrans_dir)

    logger.info("=" * 70)
    logger.info("JOURNAL AND COUNTRY DISCOVERY (WEIBULL-BASED SELECTION)")
    logger.info("=" * 70)
    logger.info(f"oddpub file: {oddpub_file}")
    logger.info(f"rtrans dir: {rtrans_dir}")
    logger.info(f"output dir: {args.output_dir}")
    logger.info("Selection method: Weibull distribution fit, threshold = scale parameter (λ)")
    logger.info("  F(λ) = 1 - e^(-1) ≈ 63.2% → selects top ~36.8% of entities")
    logger.info("")

    # Load open data PMCIDs
    open_data_pmcids = load_open_data_pmcids(oddpub_file)

    # Process rtrans files
    df = process_rtrans_files(rtrans_dir, open_data_pmcids, args.limit)

    if len(df) == 0:
        logger.error("No matching records found")
        return

    logger.info(f"\nTotal records to analyze: {len(df):,}")

    # Analyze journals (Weibull fit)
    logger.info("")
    journal_result = analyze_journals(df)

    # Analyze countries (noise removal + Weibull fit)
    logger.info("")
    country_result = analyze_countries(df)

    # Save results
    save_results(journal_result, country_result, args.output_dir)

    # Print summary
    print("\n" + "=" * 70)
    print("SUMMARY (WEIBULL-BASED SELECTION)")
    print("=" * 70)

    j_stats = journal_result['stats']
    j_cov = journal_result.get('coverage', {})
    print(f"\nJOURNALS (Weibull threshold):")
    print(f"  Unique journals: {j_stats['total_unique']:,}")
    print(f"  Weibull shape (k): {j_stats['weibull_shape']:.4f}")
    print(f"  Weibull scale (λ): {j_stats['weibull_scale']:.2f}")
    print(f"  Threshold: count >= {j_stats['threshold_count']:,}")
    print(f"  Selected: {j_stats['selected_count']:,} ({j_stats['actual_pct_above']:.1f}% of entities)")
    print(f"  Article coverage: {j_stats['coverage_pct']:.1f}%")
    print(f"  KS test: stat={j_stats['ks_statistic']:.4f}, p={j_stats['ks_pvalue']:.4f}")
    print(f"\n  Top 10:")
    for _, row in journal_result['dataframe'].head(10).iterrows():
        print(f"    {row['count']:>6,}  {row['journal'][:45]}")

    c_stats = country_result['stats']
    c_cov = country_result.get('coverage', {})
    print(f"\nCOUNTRIES (noise removal + Weibull threshold):")
    print(f"  Raw → Cleaned: {c_stats['total_raw']:,} → {c_stats['after_noise_removal']:,}")
    print(f"  Weibull shape (k): {c_stats['weibull_shape']:.4f}")
    print(f"  Weibull scale (λ): {c_stats['weibull_scale']:.2f}")
    print(f"  Threshold: count >= {c_stats['threshold_count']:,}")
    print(f"  Selected: {c_stats['selected_count']:,} ({c_stats['actual_pct_above']:.1f}% of entities)")
    print(f"  Article coverage: {c_stats['coverage_pct']:.1f}%")
    print(f"  KS test: stat={c_stats['ks_statistic']:.4f}, p={c_stats['ks_pvalue']:.4f}")
    print(f"\n  Top 10:")
    for _, row in country_result['dataframe'].head(10).iterrows():
        print(f"    {row['count']:>6,}  {row['country']}")

    print("\n" + "=" * 70)
    print(f"Results saved to {args.output_dir}/")
    print("  - significant_journals.csv (above threshold, with publisher)")
    print("  - significant_countries.csv (above threshold)")
    print("  - all_journals.csv (complete list)")
    print("  - all_countries.csv (complete list)")
    print("  - discovery_summary.txt")


if __name__ == '__main__':
    main()
