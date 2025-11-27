#!/usr/bin/env python3
"""Create batch manifest files for oddpub HPC processing.

This script finds all XML files and splits them into batches for parallel processing.
"""

import os
import sys
import argparse
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(
        description='Create batch manifest files for oddpub processing'
    )
    parser.add_argument(
        'xml_dir',
        help='Directory containing extracted XML files'
    )
    parser.add_argument(
        '-o', '--output-dir',
        default='oddpub_batches',
        help='Output directory for batch manifests (default: oddpub_batches)'
    )
    parser.add_argument(
        '-b', '--batch-size',
        type=int,
        default=1000,
        help='Number of XMLs per batch (default: 1000)'
    )

    args = parser.parse_args()

    xml_dir = Path(args.xml_dir)
    output_dir = Path(args.output_dir)
    batch_size = args.batch_size

    if not xml_dir.exists():
        print(f"Error: XML directory does not exist: {xml_dir}")
        sys.exit(1)

    # Find all XML files
    print(f"Scanning {xml_dir} for XML files...")
    xml_files = sorted(xml_dir.rglob("*.xml"))
    total_files = len(xml_files)

    if total_files == 0:
        print(f"Error: No XML files found in {xml_dir}")
        sys.exit(1)

    num_batches = (total_files + batch_size - 1) // batch_size

    print(f"Found {total_files:,} XML files")
    print(f"Creating {num_batches:,} batches of up to {batch_size} files")

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Write batch manifest files
    for batch_idx in range(num_batches):
        start_idx = batch_idx * batch_size
        end_idx = min((batch_idx + 1) * batch_size, total_files)
        batch_files = xml_files[start_idx:end_idx]

        manifest_file = output_dir / f"batch_{batch_idx:05d}.txt"
        with open(manifest_file, 'w') as f:
            for xml_file in batch_files:
                f.write(f"{xml_file}\n")

        if (batch_idx + 1) % 100 == 0:
            print(f"Created {batch_idx + 1:,} batch manifests...")

    print(f"\nDone! Created {num_batches:,} batch manifest files in {output_dir}")
    print(f"Each batch file lists up to {batch_size} XML files to process")
    print(f"\nNext step: Create swarm file with create_swarm.sh")


if __name__ == "__main__":
    main()
