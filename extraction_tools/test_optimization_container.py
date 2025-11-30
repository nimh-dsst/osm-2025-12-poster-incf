#!/usr/bin/env python3
"""
Test script to verify the optimization works correctly INSIDE a container.
This should be run from within the oddpub container.
"""

import time
import subprocess
import sys
from pathlib import Path

def test_extraction_methods(tar_file):
    """Test both original and optimized extraction methods."""

    print(f"Testing extraction methods on: {tar_file}")
    print("=" * 60)

    # Check if CSV exists
    csv_file = Path(tar_file).with_suffix('').with_suffix('.filelist.csv')
    if not csv_file.exists():
        print(f"ERROR: CSV file not found: {csv_file}")
        print("Cannot test optimization without CSV file list")
        return

    # Test parameters - use smaller sample
    start_index = 100   # Start at file 100
    chunk_size = 100    # Process 100 files

    print(f"\nTest parameters:")
    print(f"  Start index: {start_index}")
    print(f"  Chunk size: {chunk_size}")
    print(f"  Expected files: {start_index} to {start_index + chunk_size - 1}")

    # Test 1: Original method (with enumeration)
    print(f"\nTest 1: Original method (full enumeration)")
    print("-" * 40)

    start_time = time.time()
    result = subprocess.run([
        sys.executable,
        "/scripts/process_pmcoa_with_oddpub.py",  # Use container path
        tar_file,
        "--start-index", str(start_index),
        "--chunk-size", str(chunk_size),
        "--output-file", "/tmp/test_original.parquet"
    ], capture_output=True, text=True)

    original_time = time.time() - start_time
    print(f"Time taken: {original_time:.2f} seconds")

    if result.returncode != 0:
        print(f"ERROR: Original method failed")
        print(f"stdout:\n{result.stdout[-1000:]}")  # Last 1000 chars
        print(f"stderr:\n{result.stderr[-1000:]}")
        return

    print("✓ Original method completed successfully")

    # Test 2: Optimized method (using CSV)
    print(f"\nTest 2: Optimized method (CSV lookup)")
    print("-" * 40)

    # First, copy the optimized script to container location
    subprocess.run([
        "cp", "process_pmcoa_with_oddpub_optimized.py",
        "/tmp/process_pmcoa_with_oddpub_optimized.py"
    ])

    start_time = time.time()
    result = subprocess.run([
        sys.executable,
        "/tmp/process_pmcoa_with_oddpub_optimized.py",
        tar_file,
        "--start-index", str(start_index),
        "--chunk-size", str(chunk_size),
        "--output-file", "/tmp/test_optimized.parquet"
    ], capture_output=True, text=True)

    optimized_time = time.time() - start_time
    print(f"Time taken: {optimized_time:.2f} seconds")

    if result.returncode != 0:
        print(f"ERROR: Optimized method failed")
        print(f"stdout:\n{result.stdout[-1000:]}")
        print(f"stderr:\n{result.stderr[-1000:]}")
        return

    print("✓ Optimized method completed successfully")

    # Results comparison
    print(f"\n" + "=" * 60)
    print("RESULTS:")
    print("-" * 60)
    print(f"Original method:   {original_time:.2f} seconds")
    print(f"Optimized method:  {optimized_time:.2f} seconds")

    if optimized_time > 0:
        speedup = original_time / optimized_time
        print(f"Speedup:          {speedup:.1f}x")

        time_saved = original_time - optimized_time
        print(f"Time saved:       {time_saved:.2f} seconds")

    # Verify outputs are identical
    print(f"\nVerifying outputs are identical...")
    try:
        import pandas as pd
        df1 = pd.read_parquet("/tmp/test_original.parquet")
        df2 = pd.read_parquet("/tmp/test_optimized.parquet")

        print(f"Original output:  {len(df1)} records")
        print(f"Optimized output: {len(df2)} records")

        if len(df1) == len(df2):
            print("✓ Output sizes match")

            # Compare is_open_data counts
            open_data_1 = df1['is_open_data'].sum()
            open_data_2 = df2['is_open_data'].sum()
            print(f"Original open data count:  {open_data_1}")
            print(f"Optimized open data count: {open_data_2}")

            if open_data_1 == open_data_2:
                print("✓ Open data detection matches")
            else:
                print("✗ Open data detection differs!")
        else:
            print("✗ Output sizes differ!")

    except Exception as e:
        print(f"Could not compare outputs: {e}")

    print("=" * 60)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python test_optimization_container.py <tar.gz file>")
        print("Example: python test_optimization_container.py /data/NIMH_scratch/licc/pmcoa/files/oa_comm_xml.PMC005xxxxxx.baseline.2025-06-26.tar.gz")
        print("\nNOTE: This script must be run INSIDE the oddpub container!")
        sys.exit(1)

    test_extraction_methods(sys.argv[1])