#!/usr/bin/env python3
"""
Build Canonical Funders from NER-Discovered Entities

Uses a principled, multi-stage selection strategy:
1. Statistical threshold (4σ on log-scale)
2. Quality filters (remove fragments, noise)
3. Alias consolidation (group variations)
4. Final threshold (merged count >= 2000)

See docs/CANONICAL_FUNDER_SELECTION.md for detailed methodology.

Usage:
    python funder_analysis/build_canonical_funders.py \
        --input results/openss_explore_v2/all_potential_funders.csv \
        --output funder_analysis/funder_aliases_v2.csv \
        --verbose

Author: INCF 2025 Poster Analysis
Date: 2025-12-07
"""

import argparse
import json
import logging
import re
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

try:
    from rapidfuzz import fuzz
    HAS_RAPIDFUZZ = True
except ImportError:
    HAS_RAPIDFUZZ = False
    logging.warning("rapidfuzz not installed, using basic string matching")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# Known noise patterns
NOISE_PATTERNS = [
    r'^[IVX]+$',           # Roman numerals (III, IV, VI)
    r'^[A-Z]{1,2}$',       # Very short acronyms (II, IV)
    r'^the\s',             # Starts with "the"
    r'\d{4,}',             # Contains 4+ digit numbers (grant numbers)
    r'^[A-Z]\d+',          # Letter followed by numbers (grant IDs)
]

NOISE_EXACT = {
    'HHS', 'NHS', 'DEAL', 'III', 'II', 'IV', 'VI', 'VII', 'VIII',
    'the Ministry', 'Federal Ministry', 'National Center',
    'Research Institute', 'Innovation Fund', 'Technology Commission',
    'SFB', 'ICT', 'IOS', 'DEB', 'CAS',  # Ambiguous short acronyms
}

# Known fragments (parts of funder names, not complete)
FRAGMENT_PATTERNS = [
    r'^Science Foundation$',
    r'^Research Council$',
    r'^Research Foundation$',
    r'^National Institute$',
    r'^Natural Science Foundation$',
    r'^Blood Institute$',
    r'^National Center$',
]

# Known complete funders that might look like fragments
KNOWN_COMPLETE_FUNDERS = {
    'National Science Foundation',  # NSF - complete name
    'National Institutes of Health',
    'European Research Council',
    'Medical Research Council',
    'Swiss National Science Foundation',
    'Austrian Science Fund',
    'Swedish Research Council',
    'Czech Science Foundation',
    'Russian Science Foundation',
    'National Research Foundation',  # Korea/South Africa
    'Australian Research Council',
    'Canadian Institutes of Health Research',
    'Japan Society for the Promotion of Science',
    'Deutsche Forschungsgemeinschaft',
    'German Research Foundation',
    'Wellcome Trust',
    'Howard Hughes Medical Institute',
    'Bill and Melinda Gates Foundation',
    'Cancer Research UK',
    'Biotechnology and Biological Sciences Research Council',
    'Engineering and Physical Sciences Research Council',
    'Natural Environment Research Council',
    'Economic and Social Research Council',
    'Science and Technology Facilities Council',
    'Agence Nationale de la Recherche',
    'Netherlands Organisation for Scientific Research',
    'Japan Agency for Medical Research and Development',
    'China Postdoctoral Science Foundation',
    'European Commission',
    'European Regional Development Fund',
    'Max Planck Society',
    'Royal Society',
    'American Heart Association',
    'China Scholarship Council',
}

# Explicit alias groups - each list is a set of names that refer to the same funder
# The first entry is the canonical name
EXPLICIT_ALIAS_GROUPS = [
    # China
    ('National Natural Science Foundation of China', ['NSFC', 'NNSFC', 'National Natural Science Foundation',
     'Natural Science Foundation of China', 'grants from the National Natural Science Foundation']),
    # USA - NIH
    ('National Institutes of Health', ['NIH']),
    # USA - NSF
    ('National Science Foundation', ['NSF', 'US National Science Foundation']),
    # Germany
    ('Deutsche Forschungsgemeinschaft', ['DFG', 'German Research Foundation']),
    # UK - MRC
    ('Medical Research Council', ['MRC', 'UK Medical Research Council']),
    # EU
    ('European Research Council', ['ERC']),
    ('European Commission', ['EC']),
    # France
    ('Agence Nationale de la Recherche', ['ANR', 'French National Research Agency']),
    # Japan
    ('Japan Society for the Promotion of Science', ['JSPS', 'Japan Society']),
    ('Japan Agency for Medical Research and Development', ['AMED', 'Japan Agency']),
    ('Japan Science and Technology Agency', ['JST']),
    # Korea
    ('National Research Foundation of Korea', ['NRF', 'Korea National Research Foundation', 'National Research Foundation']),
    # UK Research Councils
    ('Biotechnology and Biological Sciences Research Council', ['BBSRC', 'Biological Sciences Research Council']),
    ('Engineering and Physical Sciences Research Council', ['EPSRC', 'Engineering Research Council']),
    ('Natural Environment Research Council', ['NERC']),
    ('Economic and Social Research Council', ['ESRC']),
    ('Science and Technology Facilities Council', ['STFC']),
    ('National Institute for Health Research', ['NIHR']),
    # Switzerland
    ('Swiss National Science Foundation', ['SNSF', 'SNF']),
    # Austria
    ('Austrian Science Fund', ['FWF']),
    # Netherlands
    ('Netherlands Organisation for Scientific Research', ['NWO', 'Dutch Research Council']),
    # Canada
    ('Canadian Institutes of Health Research', ['CIHR']),
    ('Natural Sciences and Engineering Research Council of Canada', ['NSERC']),
    # Australia
    ('National Health and Medical Research Council', ['NHMRC']),
    ('Australian Research Council', ['ARC']),
    # Brazil
    ('Fundacao de Amparo a Pesquisa do Estado de Sao Paulo', ['FAPESP']),
    ('Conselho Nacional de Desenvolvimento Cientifico e Tecnologico', ['CNPq']),
    ('Coordenacao de Aperfeicoamento de Pessoal de Nivel Superior', ['CAPES']),
    # USA - Private
    ('Howard Hughes Medical Institute', ['HHMI']),
    ('Bill and Melinda Gates Foundation', ['BMGF', 'Gates Foundation', 'Melinda Gates Foundation']),
    # UK - Private
    ('Wellcome Trust', ['Wellcome', 'a Wellcome Trust']),
    ('Cancer Research UK', ['CRUK']),
    # USA - Government
    ('United States Department of Agriculture', ['USDA']),
    ('Department of Energy', ['DOE']),
    # Germany
    ('Bundesministerium fur Bildung und Forschung', ['BMBF', 'German Federal Ministry']),
    # France
    ('Centre National de la Recherche Scientifique', ['CNRS']),
    # Belgium
    ('Research Foundation Flanders', ['FWO']),
    # Sweden
    ('Swedish Research Council', ['VR', 'Vetenskapsradet']),
    # Portugal
    ('Fundacao para a Ciencia e a Tecnologia', ['FCT']),
    # Spain
    ('Ministerio de Economia y Competitividad', ['MINECO', 'Spanish Ministry']),
    # Italy
    ('Associazione Italiana per la Ricerca sul Cancro', ['AIRC']),
    # UK
    ('UK Research and Innovation', ['UKRI']),
    # China
    ('China Scholarship Council', ['CSC']),
    ('China Postdoctoral Science Foundation', []),
    # EU
    ('European Regional Development Fund', ['ERDF', 'FEDER']),
    # Max Planck
    ('Max Planck Society', ['Max Planck', 'MPG']),
    # Royal Society
    ('Royal Society', []),
    # American Heart Association
    ('American Heart Association', ['AHA']),
    # Russia
    ('Russian Science Foundation', ['RSF']),
    # Czech
    ('Czech Science Foundation', ['GACR']),
    # NIH Institutes
    ('National Cancer Institute', ['NCI']),
    ('National Heart Lung and Blood Institute', ['NHLBI', 'Blood Institute']),
    ('National Institute of Mental Health', ['NIMH']),
    ('National Institute of General Medical Sciences', ['NIGMS']),
    ('National Institute of Allergy and Infectious Diseases', ['NIAID']),
    ('National Institute of Diabetes and Digestive and Kidney Diseases', ['NIDDK']),
    ('National Institute of Neurological Disorders and Stroke', ['NINDS']),
    ('National Institute on Aging', ['NIA']),
    ('National Eye Institute', ['NEI']),
    ('National Institute on Drug Abuse', ['NIDA']),
    ('National Institute of Environmental Health Sciences', ['NIEHS']),
    ('National Institute of Child Health and Human Development', ['NICHD']),
    ('National Institute of Arthritis and Musculoskeletal and Skin Diseases', ['NIAMS']),
    ('National Institute on Alcohol Abuse and Alcoholism', ['NIAAA']),
    ('National Center for Advancing Translational Sciences', ['NCATS']),
]

# Build lookup from variant to canonical
VARIANT_TO_CANONICAL = {}
for canonical, variants in EXPLICIT_ALIAS_GROUPS:
    VARIANT_TO_CANONICAL[canonical.lower()] = canonical
    for v in variants:
        VARIANT_TO_CANONICAL[v.lower()] = canonical

# Known acronym to full name mappings (legacy, kept for reference)
KNOWN_ACRONYMS = {
    'NIH': 'National Institutes of Health',
    'NSF': 'National Science Foundation',
    'NSFC': 'National Natural Science Foundation of China',
    'NNSFC': 'National Natural Science Foundation of China',
    'DFG': 'Deutsche Forschungsgemeinschaft',
    'MRC': 'Medical Research Council',
    'ERC': 'European Research Council',
    'ANR': 'Agence Nationale de la Recherche',
    'JSPS': 'Japan Society for the Promotion of Science',
    'NRF': 'National Research Foundation of Korea',
    'BBSRC': 'Biotechnology and Biological Sciences Research Council',
    'EPSRC': 'Engineering and Physical Sciences Research Council',
    'NERC': 'Natural Environment Research Council',
    'ESRC': 'Economic and Social Research Council',
    'STFC': 'Science and Technology Facilities Council',
    'SNSF': 'Swiss National Science Foundation',
    'SNF': 'Swiss National Science Foundation',
    'FWF': 'Austrian Science Fund',
    'NWO': 'Netherlands Organisation for Scientific Research',
    'AMED': 'Japan Agency for Medical Research and Development',
    'CIHR': 'Canadian Institutes of Health Research',
    'NHMRC': 'National Health and Medical Research Council',
    'ARC': 'Australian Research Council',
    'FAPESP': 'Fundacao de Amparo a Pesquisa do Estado de Sao Paulo',
    'CNPq': 'Conselho Nacional de Desenvolvimento Cientifico e Tecnologico',
    'HHMI': 'Howard Hughes Medical Institute',
    'BMGF': 'Bill and Melinda Gates Foundation',
    'CRUK': 'Cancer Research UK',
    'USDA': 'United States Department of Agriculture',
    'DOE': 'Department of Energy',
    'MEXT': 'Ministry of Education, Culture, Sports, Science and Technology',
    'NSERC': 'Natural Sciences and Engineering Research Council of Canada',
    'FCT': 'Fundacao para a Ciencia e a Tecnologia',
    'CAPES': 'Coordenacao de Aperfeicoamento de Pessoal de Nivel Superior',
    'BMBF': 'Bundesministerium fur Bildung und Forschung',
    'MOST': 'Ministry of Science and Technology',
    'JST': 'Japan Science and Technology Agency',
    'CNRS': 'Centre National de la Recherche Scientifique',
    'FWO': 'Research Foundation Flanders',
    'DBT': 'Department of Biotechnology',
    'UKRI': 'UK Research and Innovation',
    'AIRC': 'Associazione Italiana per la Ricerca sul Cancro',
    # NIH Institutes
    'NCI': 'National Cancer Institute',
    'NHLBI': 'National Heart Lung and Blood Institute',
    'NIMH': 'National Institute of Mental Health',
    'NIGMS': 'National Institute of General Medical Sciences',
    'NIAID': 'National Institute of Allergy and Infectious Diseases',
    'NIDDK': 'National Institute of Diabetes and Digestive and Kidney Diseases',
    'NINDS': 'National Institute of Neurological Disorders and Stroke',
    'NIA': 'National Institute on Aging',
    'NEI': 'National Eye Institute',
    'NIDA': 'National Institute on Drug Abuse',
    'NIEHS': 'National Institute of Environmental Health Sciences',
    'NICHD': 'National Institute of Child Health and Human Development',
    'NIAMS': 'National Institute of Arthritis and Musculoskeletal and Skin Diseases',
    'NIAAA': 'National Institute on Alcohol Abuse and Alcoholism',
    'NCATS': 'National Center for Advancing Translational Sciences',
    'NIHR': 'National Institute for Health Research',
}


def is_noise(name: str) -> bool:
    """Check if a name matches noise patterns."""
    if name in NOISE_EXACT:
        return True
    for pattern in NOISE_PATTERNS:
        if re.match(pattern, name, re.IGNORECASE):
            return True
    return False


def is_fragment(name: str, all_names: set, counts: dict) -> bool:
    """Check if a name is likely a fragment of another funder name."""
    # Known complete funders are not fragments
    if name in KNOWN_COMPLETE_FUNDERS:
        return False

    # Check against fragment patterns
    for pattern in FRAGMENT_PATTERNS:
        if re.match(pattern, name):
            return True

    # Short names that appear as substrings of higher-count entries
    words = name.split()
    if len(words) <= 3:
        name_lower = name.lower()
        for other in all_names:
            if name != other and name_lower in other.lower():
                if counts.get(other, 0) > counts.get(name, 0):
                    return True

    return False


def fuzzy_match(name1: str, name2: str, threshold: int = 85) -> bool:
    """Check if two names are fuzzy matches."""
    if HAS_RAPIDFUZZ:
        return fuzz.token_sort_ratio(name1.lower(), name2.lower()) > threshold
    else:
        # Basic containment check
        n1, n2 = name1.lower(), name2.lower()
        return n1 in n2 or n2 in n1


def acronym_matches_name(acronym: str, full_name: str) -> bool:
    """Check if acronym could be derived from full_name."""
    if len(acronym) < 2 or len(acronym) > 10:
        return False

    words = [w for w in full_name.split() if len(w) > 0 and w[0].isupper()]
    # Skip common words
    skip_words = {'of', 'the', 'for', 'and', 'in', 'on', 'to', 'a', 'an'}
    words = [w for w in words if w.lower() not in skip_words]

    if not words:
        return False

    potential = ''.join(w[0].upper() for w in words)
    return acronym.upper() == potential


def consolidate_aliases(candidates: pd.DataFrame) -> dict:
    """
    Group candidates into clusters of aliases.

    Uses explicit alias groups first, then falls back to fuzzy matching.

    Returns dict: canonical_name -> {variants: [list], merged_count: int, country: str}
    """
    clusters = {}
    used = set()

    # Sort by count descending
    sorted_df = candidates.sort_values('count', ascending=False)

    for _, row in sorted_df.iterrows():
        name = row['name']
        count = row['count']

        if name in used:
            continue

        # First: Check explicit alias mappings (highest priority)
        name_lower = name.lower()
        if name_lower in VARIANT_TO_CANONICAL:
            canonical = VARIANT_TO_CANONICAL[name_lower]
            if canonical not in clusters:
                clusters[canonical] = {
                    'variants': [],
                    'merged_count': 0,
                    'country': get_country(canonical),
                }
            # Determine variant type
            if name_lower == canonical.lower():
                var_type = 'primary'
            elif len(name) <= 6 and name.upper() == name:
                var_type = 'acronym'
            else:
                var_type = 'alias'
            clusters[canonical]['variants'].append((name, var_type, count))
            clusters[canonical]['merged_count'] += count
            used.add(name)
            continue

        # Second: Check known acronyms (legacy fallback)
        if name.upper() in KNOWN_ACRONYMS:
            canonical = KNOWN_ACRONYMS[name.upper()]
            if canonical not in clusters:
                clusters[canonical] = {
                    'variants': [],
                    'merged_count': 0,
                    'country': get_country(canonical),
                }
            clusters[canonical]['variants'].append((name, 'acronym', count))
            clusters[canonical]['merged_count'] += count
            used.add(name)
            continue

        # Third: Only use fuzzy matching if rapidfuzz is available and threshold is high
        if HAS_RAPIDFUZZ:
            matched_cluster = None
            for canonical, info in clusters.items():
                # Only match if very high similarity (>90)
                if fuzz.token_sort_ratio(name.lower(), canonical.lower()) > 90:
                    matched_cluster = canonical
                    break

            if matched_cluster:
                clusters[matched_cluster]['variants'].append((name, 'fuzzy', count))
                clusters[matched_cluster]['merged_count'] += count
                used.add(name)
                continue

        # No match found - start new cluster (only if name looks like a real funder)
        # Skip names that look like fragments or noise that slipped through
        if len(name.split()) >= 2 or (len(name) <= 6 and name.upper() == name and count >= 5000):
            clusters[name] = {
                'variants': [(name, 'primary', count)],
                'merged_count': count,
                'country': get_country(name),
            }
            used.add(name)

    return clusters


def get_country(funder_name: str) -> str:
    """Infer country from funder name."""
    # Exact matches take priority (to avoid false positives from substring matching)
    exact_country_map = {
        'National Institutes of Health': 'USA',
        'National Science Foundation': 'USA',
        'National Natural Science Foundation of China': 'China',
        'Medical Research Council': 'UK',
        'Biotechnology and Biological Sciences Research Council': 'UK',
        'Engineering and Physical Sciences Research Council': 'UK',
        'Natural Environment Research Council': 'UK',
        'Economic and Social Research Council': 'UK',
        'Science and Technology Facilities Council': 'UK',
        'National Institute for Health Research': 'UK',
        'UK Research and Innovation': 'UK',
        'Wellcome Trust': 'UK',
        'Cancer Research UK': 'UK',
        'National Research Foundation of Korea': 'Korea',
        'Swiss National Science Foundation': 'Switzerland',
        'National Health and Medical Research Council': 'Australia',
        'Australian Research Council': 'Australia',
        'Deutsche Forschungsgemeinschaft': 'Germany',
        'Japan Society for the Promotion of Science': 'Japan',
        'Japan Agency for Medical Research and Development': 'Japan',
        'Japan Science and Technology Agency': 'Japan',
        'Agence Nationale de la Recherche': 'France',
        'Centre National de la Recherche Scientifique': 'France',
        'European Research Council': 'EU',
        'European Commission': 'EU',
        'European Regional Development Fund': 'EU',
        'Canadian Institutes of Health Research': 'Canada',
        'Natural Sciences and Engineering Research Council of Canada': 'Canada',
        'Netherlands Organisation for Scientific Research': 'Netherlands',
        'Austrian Science Fund': 'Austria',
        'Swedish Research Council': 'Sweden',
        'Research Foundation Flanders': 'Belgium',
        'Howard Hughes Medical Institute': 'USA',
        'Bill and Melinda Gates Foundation': 'USA',
        'United States Department of Agriculture': 'USA',
        'Department of Energy': 'USA',
        'National Cancer Institute': 'USA',
        'National Heart Lung and Blood Institute': 'USA',
        'National Institute of Mental Health': 'USA',
        'National Institute of General Medical Sciences': 'USA',
        'National Institute of Allergy and Infectious Diseases': 'USA',
        'Fundacao de Amparo a Pesquisa do Estado de Sao Paulo': 'Brazil',
        'Conselho Nacional de Desenvolvimento Cientifico e Tecnologico': 'Brazil',
        'China Postdoctoral Science Foundation': 'China',
        'China Scholarship Council': 'China',
        'Russian Science Foundation': 'Russia',
        'Czech Science Foundation': 'Czech Republic',
        'Max Planck Society': 'Germany',
        'Bundesministerium fur Bildung und Forschung': 'Germany',
        'Royal Society': 'UK',
        'American Heart Association': 'USA',
    }

    # Check exact matches first
    if funder_name in exact_country_map:
        return exact_country_map[funder_name]

    # Fall back to keyword matching
    country_indicators = {
        'USA': ['NIH', 'National Institutes of Health', 'National Science Foundation',
                'Howard Hughes', 'American', 'United States', 'USDA', 'DOE'],
        'China': ['China', 'Chinese', 'NSFC'],
        'UK': ['UK', 'British', 'Wellcome', 'MRC', 'BBSRC', 'EPSRC', 'NERC', 'ESRC', 'STFC'],
        'Germany': ['German', 'DFG', 'Deutsche', 'Forschungsgemeinschaft', 'BMBF'],
        'Japan': ['Japan', 'JSPS', 'AMED', 'JST', 'MEXT'],
        'France': ['France', 'French', 'ANR', 'CNRS'],
        'EU': ['European', 'ERC', 'EU '],
        'Australia': ['Australia', 'NHMRC'],
        'Canada': ['Canada', 'Canadian', 'CIHR', 'NSERC'],
        'Switzerland': ['Swiss', 'SNSF', 'SNF'],
        'Austria': ['Austria', 'FWF'],
        'Netherlands': ['Netherlands', 'Dutch', 'NWO'],
        'Sweden': ['Sweden', 'Swedish'],
        'Brazil': ['Brazil', 'FAPESP', 'CNPq', 'CAPES'],
        'Korea': ['Korea', 'Korean'],
        'Italy': ['Italy', 'Italian', 'AIRC'],
        'Belgium': ['Belgium', 'Flemish', 'FWO'],
        'Portugal': ['Portugal', 'FCT'],
        'Russia': ['Russia', 'Russian'],
        'Czech': ['Czech'],
        'India': ['India', 'DBT'],
        'Spain': ['Spain', 'Spanish', 'MINECO'],
    }

    name_upper = funder_name.upper()
    for country, indicators in country_indicators.items():
        for ind in indicators:
            if ind.upper() in name_upper:
                return country
    return 'Unknown'


def build_canonical_funders(
    input_file: Path,
    output_file: Path,
    log_threshold: float = 4.0,
    min_merged_count: int = 2000,
    verbose: bool = False
) -> dict:
    """
    Build canonical funder list using principled selection.

    Args:
        input_file: Path to all_potential_funders.csv
        output_file: Path to output funder_aliases_v2.csv
        log_threshold: Number of std devs above mean on log scale
        min_merged_count: Minimum merged count for final selection
        verbose: Print detailed progress

    Returns:
        dict with selection statistics
    """
    stats = {
        'input_count': 0,
        'stage1_statistical': 0,
        'stage2_after_noise': 0,
        'stage2_after_fragments': 0,
        'stage3_clusters': 0,
        'stage4_final': 0,
    }

    # Load input
    df = pd.read_csv(input_file)
    stats['input_count'] = len(df)
    logger.info(f"Loaded {len(df):,} potential funders")

    # Stage 1: Statistical threshold
    counts = df['count'].values
    log_counts = np.log10(counts)
    mean_log = log_counts.mean()
    std_log = log_counts.std()
    threshold_log = mean_log + log_threshold * std_log
    threshold_count = 10 ** threshold_log

    logger.info(f"Stage 1: Statistical threshold")
    logger.info(f"  Log-scale: mean={mean_log:.2f}, std={std_log:.2f}")
    logger.info(f"  Threshold ({log_threshold}σ): log={threshold_log:.2f}, count>={threshold_count:.0f}")

    stage1 = df[df['count'] >= threshold_count].copy()
    stats['stage1_statistical'] = len(stage1)
    logger.info(f"  After threshold: {len(stage1):,} funders")

    # Stage 2a: Remove noise
    logger.info(f"Stage 2a: Removing noise patterns")
    noise_mask = stage1['name'].apply(is_noise)
    noise_removed = stage1[noise_mask]['name'].tolist()
    if verbose and noise_removed:
        logger.info(f"  Removed as noise: {noise_removed[:20]}...")
    stage2a = stage1[~noise_mask].copy()
    stats['stage2_after_noise'] = len(stage2a)
    logger.info(f"  After noise removal: {len(stage2a):,} funders")

    # Stage 2b: Remove fragments
    logger.info(f"Stage 2b: Removing fragments")
    all_names = set(stage2a['name'])
    count_dict = dict(zip(stage2a['name'], stage2a['count']))
    fragment_mask = stage2a['name'].apply(lambda x: is_fragment(x, all_names, count_dict))
    fragments_removed = stage2a[fragment_mask]['name'].tolist()
    if verbose and fragments_removed:
        logger.info(f"  Removed as fragments: {fragments_removed[:20]}...")
    stage2b = stage2a[~fragment_mask].copy()
    stats['stage2_after_fragments'] = len(stage2b)
    logger.info(f"  After fragment removal: {len(stage2b):,} funders")

    # Stage 3: Consolidate aliases
    logger.info(f"Stage 3: Consolidating aliases")
    clusters = consolidate_aliases(stage2b)
    stats['stage3_clusters'] = len(clusters)
    logger.info(f"  Consolidated into {len(clusters):,} clusters")

    # Stage 4: Final threshold
    logger.info(f"Stage 4: Applying final threshold (merged_count >= {min_merged_count:,})")
    final_clusters = {k: v for k, v in clusters.items() if v['merged_count'] >= min_merged_count}
    stats['stage4_final'] = len(final_clusters)
    logger.info(f"  Final canonical funders: {len(final_clusters):,}")

    # Build output DataFrame
    rows = []
    for canonical, info in sorted(final_clusters.items(), key=lambda x: -x[1]['merged_count']):
        for variant, var_type, var_count in info['variants']:
            rows.append({
                'canonical_name': canonical,
                'variant': variant,
                'variant_type': var_type,
                'country': info['country'],
                'variant_count': var_count,
                'merged_count': info['merged_count'],
                'selection_method': f'{log_threshold}sigma+consolidation',
            })

    output_df = pd.DataFrame(rows)

    # Save output
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_df.to_csv(output_file, index=False)
    logger.info(f"Saved {len(output_df)} rows ({len(final_clusters)} canonical funders) to {output_file}")

    # Save statistics
    stats_file = output_file.with_suffix('.stats.json')
    with open(stats_file, 'w') as f:
        json.dump(stats, f, indent=2)
    logger.info(f"Saved statistics to {stats_file}")

    # Print summary
    print("\n" + "=" * 70)
    print("CANONICAL FUNDER SELECTION SUMMARY")
    print("=" * 70)
    print(f"{'Stage':<40} {'Count':>10}")
    print("-" * 50)
    print(f"{'Input (NER discovered)':<40} {stats['input_count']:>10,}")
    print(f"{'After statistical threshold':<40} {stats['stage1_statistical']:>10,}")
    print(f"{'After noise removal':<40} {stats['stage2_after_noise']:>10,}")
    print(f"{'After fragment removal':<40} {stats['stage2_after_fragments']:>10,}")
    print(f"{'After alias consolidation':<40} {stats['stage3_clusters']:>10,}")
    print(f"{'Final (merged_count >= {min_merged_count:,})':<40} {stats['stage4_final']:>10,}")
    print("-" * 50)

    # Print top funders
    print("\nTop 20 Canonical Funders by Merged Count:")
    print(f"{'Rank':<5} {'Funder':<45} {'Count':>12} {'Country':<10}")
    print("-" * 75)
    for i, (canonical, info) in enumerate(sorted(final_clusters.items(),
                                                   key=lambda x: -x[1]['merged_count'])[:20], 1):
        print(f"{i:<5} {canonical[:44]:<45} {info['merged_count']:>12,} {info['country']:<10}")

    return stats


def main():
    parser = argparse.ArgumentParser(
        description='Build canonical funder list from NER discoveries',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
    python funder_analysis/build_canonical_funders.py \\
        --input results/openss_explore_v2/all_potential_funders.csv \\
        --output funder_analysis/funder_aliases_v2.csv \\
        --verbose
        """
    )
    parser.add_argument('--input', type=Path, required=True,
                        help='Input CSV from NER funder discovery')
    parser.add_argument('--output', type=Path, required=True,
                        help='Output canonical funders CSV')
    parser.add_argument('--log-threshold', type=float, default=4.0,
                        help='Std devs above mean on log scale (default: 4.0)')
    parser.add_argument('--min-merged-count', type=int, default=2000,
                        help='Min merged count for final selection (default: 2000)')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Print detailed progress')

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    build_canonical_funders(
        args.input,
        args.output,
        args.log_threshold,
        args.min_merged_count,
        args.verbose
    )


if __name__ == '__main__':
    main()
