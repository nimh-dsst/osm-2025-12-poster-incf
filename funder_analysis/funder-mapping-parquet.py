#!/usr/bin/env python3
"""
Funder Mapping Script for Parquet Files

Adapted from funder-mapping-stdin.py to process parquet files in batches.
Maps funding information to specific funder organizations across large datasets.

Usage: python funder-mapping-parquet.py <parquet_directory> [--output-dir DIR]
"""

import pandas as pd
import numpy as np
import logging
import sys
import re
import argparse
import glob
from pathlib import Path
from collections import defaultdict
import gc

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Funding columns to search
funding_columns = ['fund_text', 'fund_pmc_institute', 'fund_pmc_source', 'fund_pmc_anysource']


def load_funders_db(db_path):
    """Load the funder reference database"""
    try:
        funders_df = pd.read_csv(db_path)
        logger.info(f"Loaded {len(funders_df)} funder organizations from reference database")
        return funders_df
    except FileNotFoundError:
        logger.error(f"Funder database not found at: {db_path}")
        sys.exit(1)


def data_cleaning_processing(df):
    """Clean funding text data by removing special characters"""
    for col in funding_columns:
        if col in df.columns and df[col].dtype == 'object':
            # Remove special characters but keep alphanumeric and whitespace
            df[col] = df[col].str.replace(r'[^\w\s]', '', regex=True)
    return df


def funder_mapping_batch(df, funder_names, funder_acronyms):
    """
    Map funding information to specific funder organizations for a batch

    Returns a dictionary with:
    - counts: dict of funder_name -> count of matches
    - pmcids: dict of funder_name -> set of PMCIDs with matches
    """
    results = {
        'counts': defaultdict(int),
        'pmcids': defaultdict(set)
    }

    # Ensure pmcid column exists
    pmcid_col = 'pmcid_pmc' if 'pmcid_pmc' in df.columns else 'pmcid'
    if pmcid_col not in df.columns:
        logger.warning("No PMCID column found in dataframe")
        return results

    # Process each funder organization
    for name, acronym in zip(funder_names, funder_acronyms):
        matches_mask = pd.Series([False] * len(df), index=df.index)

        # Search across all funding-related columns
        for column in funding_columns:
            if column in df.columns and df[column].dtype == 'object':
                # Search for full funder name (case-insensitive)
                name_matches = df[column].str.contains(name, case=False, na=False)
                # Search for funder acronym (case-sensitive to avoid false positives)
                acronym_matches = df[column].str.contains(acronym, case=True, na=False)

                # Combine matches
                matches_mask = matches_mask | name_matches | acronym_matches

        # Count and store matches
        if matches_mask.any():
            count = matches_mask.sum()
            results['counts'][name] += count
            # Store PMCIDs
            matched_pmcids = df.loc[matches_mask, pmcid_col].astype(str).tolist()
            results['pmcids'][name].update(matched_pmcids)

    return results


def process_parquet_files(parquet_dir, funders_df, batch_size=100):
    """Process all parquet files in batches"""

    # Get all parquet files, sorted numerically
    parquet_files = sorted(
        glob.glob(f'{parquet_dir}/out.*.parquet'),
        key=lambda x: int(Path(x).stem.split('.')[1])
    )

    if not parquet_files:
        logger.error(f"No parquet files found in {parquet_dir}")
        sys.exit(1)

    logger.info(f"Found {len(parquet_files)} parquet files to process")

    # Extract funder information
    funder_names = funders_df['Name'].tolist()
    funder_acronyms = funders_df['Acronym'].tolist()

    # Initialize global results
    global_counts = defaultdict(int)
    global_pmcids = defaultdict(set)
    total_records = 0
    total_files_processed = 0

    # Process files in batches
    num_batches = (len(parquet_files) + batch_size - 1) // batch_size

    for batch_num in range(num_batches):
        start_idx = batch_num * batch_size
        end_idx = min((batch_num + 1) * batch_size, len(parquet_files))
        batch_files = parquet_files[start_idx:end_idx]

        logger.info(f"Batch {batch_num + 1}/{num_batches}: Processing files {start_idx + 1}-{end_idx}")

        for i, pf in enumerate(batch_files, start=start_idx + 1):
            if i % 10 == 0:
                logger.info(f"  Progress: {i}/{len(parquet_files)} files...")

            try:
                # Load parquet file
                df = pd.read_parquet(pf)
                total_records += len(df)
                total_files_processed += 1

                # Clean data
                df = data_cleaning_processing(df)

                # Map funders for this batch
                results = funder_mapping_batch(df, funder_names, funder_acronyms)

                # Accumulate results
                for funder, count in results['counts'].items():
                    global_counts[funder] += count

                for funder, pmcids in results['pmcids'].items():
                    global_pmcids[funder].update(pmcids)

                del df

            except Exception as e:
                logger.error(f"Error processing {Path(pf).name}: {e}")

        # Garbage collection after each batch
        gc.collect()
        logger.info(f"  Batch {batch_num + 1} complete. Total records: {total_records:,}")

    logger.info(f"\nProcessing complete:")
    logger.info(f"  Files processed: {total_files_processed}")
    logger.info(f"  Total records: {total_records:,}")

    return global_counts, global_pmcids, total_records


def save_results(global_counts, global_pmcids, total_records, funders_df, output_dir):
    """Save summary results to files"""

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Create summary dataframe
    summary_data = []
    for name in funders_df['Name']:
        count = global_counts.get(name, 0)
        unique_pmcids = len(global_pmcids.get(name, set()))
        percentage = (count / total_records * 100) if total_records > 0 else 0

        summary_data.append({
            'Funder': name,
            'Acronym': funders_df[funders_df['Name'] == name]['Acronym'].iloc[0],
            'Total_Matches': count,
            'Unique_PMCIDs': unique_pmcids,
            'Percentage': percentage
        })

    summary_df = pd.DataFrame(summary_data)
    summary_df = summary_df.sort_values('Total_Matches', ascending=False)

    # Save summary
    summary_file = output_dir / 'funder_mapping_summary.csv'
    summary_df.to_csv(summary_file, index=False)
    logger.info(f"Summary saved to: {summary_file}")

    # Save detailed PMCID lists for top funders (optional, can be large)
    # Uncomment if needed:
    # for funder in summary_df.head(10)['Funder']:
    #     pmcids = sorted(global_pmcids.get(funder, set()))
    #     pmcid_file = output_dir / f'pmcids_{funder.replace(" ", "_")}.txt'
    #     with open(pmcid_file, 'w') as f:
    #         f.write('\n'.join(pmcids))

    return summary_df


def main():
    parser = argparse.ArgumentParser(
        description='Map funding information in parquet files to specific funder organizations.',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument('parquet_dir', help='Directory containing parquet files')
    parser.add_argument('--funder-db', default='/home/ec2-user/claude/osm/scripts/biomedical_research_funders.csv',
                        help='Path to funder database CSV')
    parser.add_argument('--output-dir', default='.',
                        help='Output directory for results (default: current directory)')
    parser.add_argument('--batch-size', type=int, default=100,
                        help='Number of files to process per batch (default: 100)')

    args = parser.parse_args()

    # Validate input directory
    if not Path(args.parquet_dir).exists():
        logger.error(f"Directory not found: {args.parquet_dir}")
        sys.exit(1)

    try:
        logger.info("="*70)
        logger.info("FUNDER MAPPING - PARQUET FILES")
        logger.info("="*70)

        # Load funder database
        funders_df = load_funders_db(args.funder_db)

        # Process parquet files
        global_counts, global_pmcids, total_records = process_parquet_files(
            args.parquet_dir,
            funders_df,
            args.batch_size
        )

        # Save results
        summary_df = save_results(global_counts, global_pmcids, total_records,
                                   funders_df, args.output_dir)

        # Display summary
        logger.info("\n" + "="*70)
        logger.info("FUNDER MAPPING SUMMARY")
        logger.info("="*70)
        logger.info(f"\nTotal records analyzed: {total_records:,}")
        logger.info(f"\nTop 10 funders by matches:")
        for i, row in summary_df.head(10).iterrows():
            logger.info(f"  {row['Funder']:50s} {row['Total_Matches']:>8,} ({row['Percentage']:5.2f}%)")

        logger.info("\nFunder mapping completed successfully")

    except Exception as e:
        logger.error(f"Failed to process data: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
