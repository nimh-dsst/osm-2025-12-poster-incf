#!/usr/bin/env python3
"""
Normalize funder names using alias mapping.

This module provides functions to:
1. Load funder alias mappings
2. Build search patterns that match any variant of a funder
3. Count funder mentions at article level (avoiding double-counting)

Usage:
    from normalize_funders import FunderNormalizer

    normalizer = FunderNormalizer('funder_aliases.csv')

    # Check if text mentions a funder (any variant)
    if normalizer.mentions_funder(text, 'National Science Foundation'):
        ...

    # Get canonical name for a variant
    canonical = normalizer.get_canonical('NSF')  # Returns 'National Science Foundation'

Author: INCF 2025 Poster Analysis
Date: 2025-12-02
"""

import re
from pathlib import Path
from collections import defaultdict

import pandas as pd


class FunderNormalizer:
    """Normalize funder names using alias mapping."""

    def __init__(self, aliases_csv: Path = None):
        """
        Initialize normalizer with alias mapping.

        Args:
            aliases_csv: Path to funder_aliases.csv file.
                        If None, uses default location.
        """
        if aliases_csv is None:
            aliases_csv = Path(__file__).parent / 'funder_aliases.csv'

        self.aliases_csv = Path(aliases_csv)
        self.canonical_to_variants = defaultdict(set)
        self.variant_to_canonical = {}
        self.search_patterns = {}

        self._load_aliases()
        self._build_patterns()

    def _load_aliases(self):
        """Load alias mappings from CSV."""
        if not self.aliases_csv.exists():
            raise FileNotFoundError(f"Alias file not found: {self.aliases_csv}")

        df = pd.read_csv(self.aliases_csv)

        for _, row in df.iterrows():
            canonical = row['canonical_name']
            variant = row['variant']

            # Map variant to canonical
            self.variant_to_canonical[variant.lower()] = canonical
            self.variant_to_canonical[canonical.lower()] = canonical

            # Map canonical to all variants
            self.canonical_to_variants[canonical].add(variant)
            self.canonical_to_variants[canonical].add(canonical)

    def _build_patterns(self):
        """Build regex patterns for each canonical funder."""
        for canonical, variants in self.canonical_to_variants.items():
            # Sort variants by length (longest first) to match most specific
            sorted_variants = sorted(variants, key=len, reverse=True)

            # Build pattern with word boundaries for short names
            pattern_parts = []
            for variant in sorted_variants:
                escaped = re.escape(variant)
                # Use word boundaries for acronyms (<=6 chars, all uppercase)
                if len(variant) <= 6 and variant.isupper():
                    pattern_parts.append(r'\b' + escaped + r'\b')
                else:
                    pattern_parts.append(escaped)

            # Combine with OR
            pattern = '|'.join(pattern_parts)
            self.search_patterns[canonical] = re.compile(pattern, re.IGNORECASE)

    def get_canonical(self, name: str) -> str:
        """
        Get canonical name for a funder variant.

        Args:
            name: Funder name (any variant)

        Returns:
            Canonical name if found, original name otherwise
        """
        return self.variant_to_canonical.get(name.lower(), name)

    def get_variants(self, canonical: str) -> set:
        """
        Get all variants for a canonical funder name.

        Args:
            canonical: Canonical funder name

        Returns:
            Set of all variant names
        """
        return self.canonical_to_variants.get(canonical, {canonical})

    def mentions_funder(self, text: str, canonical: str) -> bool:
        """
        Check if text mentions a funder (any variant).

        This is the key function for avoiding double-counting:
        it returns True if ANY variant is mentioned, but only
        counts once per article.

        Args:
            text: Text to search (funding text, etc.)
            canonical: Canonical funder name

        Returns:
            True if any variant is mentioned
        """
        if pd.isna(text) or not text:
            return False

        text = str(text)

        pattern = self.search_patterns.get(canonical)
        if pattern:
            return bool(pattern.search(text))

        # Fallback for funders not in alias mapping
        return canonical.lower() in text.lower()

    def find_all_funders(self, text: str) -> list:
        """
        Find all canonical funders mentioned in text.

        Args:
            text: Text to search

        Returns:
            List of canonical funder names found
        """
        if pd.isna(text) or not text:
            return []

        found = []
        for canonical, pattern in self.search_patterns.items():
            if pattern.search(str(text)):
                found.append(canonical)

        return found

    def get_all_canonical_names(self) -> list:
        """Get list of all canonical funder names."""
        return list(self.canonical_to_variants.keys())

    def normalize_funder_counts(self, funder_counts: dict) -> dict:
        """
        Normalize funder counts by merging variants.

        Args:
            funder_counts: Dict mapping funder names to counts

        Returns:
            Dict mapping canonical names to merged counts
        """
        normalized = defaultdict(int)
        unmatched = {}

        for funder, count in funder_counts.items():
            canonical = self.get_canonical(funder)
            if canonical != funder:
                # Variant matched - add to canonical
                normalized[canonical] += count
            else:
                # No match - keep as unmatched
                unmatched[funder] = count

        # Merge normalized and unmatched
        result = dict(normalized)
        result.update(unmatched)

        return result


def create_expanded_aliases(potential_funders_csv: Path,
                           base_aliases_csv: Path,
                           output_csv: Path,
                           min_count: int = 1000) -> pd.DataFrame:
    """
    Create expanded alias mapping by analyzing discovered funders.

    This function looks at high-count funders from discovery and
    suggests potential aliases based on string similarity.

    Args:
        potential_funders_csv: CSV from funder discovery
        base_aliases_csv: Base alias mapping
        output_csv: Output path for expanded aliases
        min_count: Minimum count to consider

    Returns:
        DataFrame with suggested aliases
    """
    # Load discovered funders
    funders_df = pd.read_csv(potential_funders_csv)
    funders_df = funders_df[funders_df['count'] >= min_count]

    # Load existing aliases
    normalizer = FunderNormalizer(base_aliases_csv)

    # Find potential new aliases
    suggestions = []

    for _, row in funders_df.iterrows():
        name = row['name']
        count = row['count']

        # Check if already in aliases
        canonical = normalizer.get_canonical(name)
        if canonical != name:
            continue  # Already mapped

        # Look for potential matches based on:
        # 1. Acronyms (short uppercase strings)
        # 2. Substring matches with canonical names

        potential_matches = []
        for can_name in normalizer.get_all_canonical_names():
            # Check if name is acronym of canonical
            if len(name) <= 6 and name.isupper():
                # Could be acronym
                words = can_name.split()
                acronym = ''.join(w[0] for w in words if w[0].isupper())
                if name == acronym:
                    potential_matches.append(can_name)

            # Check substring match
            if name.lower() in can_name.lower() or can_name.lower() in name.lower():
                potential_matches.append(can_name)

        if potential_matches:
            suggestions.append({
                'discovered_name': name,
                'count': count,
                'potential_canonical': potential_matches[0] if len(potential_matches) == 1 else str(potential_matches),
                'num_matches': len(potential_matches)
            })
        elif count >= min_count:
            # High-count funder not in aliases - flag for review
            suggestions.append({
                'discovered_name': name,
                'count': count,
                'potential_canonical': 'NEW_FUNDER',
                'num_matches': 0
            })

    df = pd.DataFrame(suggestions)
    df = df.sort_values('count', ascending=False)
    df.to_csv(output_csv, index=False)

    return df


if __name__ == '__main__':
    # Test the normalizer
    normalizer = FunderNormalizer()

    print("=== Funder Normalizer Test ===\n")

    # Test canonical lookups
    test_variants = ['NSF', 'NSFC', 'DFG', 'NIH', 'Wellcome']
    print("Canonical name lookups:")
    for v in test_variants:
        print(f"  {v} -> {normalizer.get_canonical(v)}")

    # Test pattern matching
    print("\nPattern matching tests:")
    test_texts = [
        "Funded by NIH grant R01-GM123456",
        "This work was supported by the National Science Foundation",
        "Grant from NSF and NIH",
        "Deutsche Forschungsgemeinschaft (DFG) funded this research",
        "No funding received"
    ]

    for text in test_texts:
        funders = normalizer.find_all_funders(text)
        print(f"  '{text[:50]}...' -> {funders}")

    # Show all canonical funders
    print(f"\n{len(normalizer.get_all_canonical_names())} canonical funders loaded")
