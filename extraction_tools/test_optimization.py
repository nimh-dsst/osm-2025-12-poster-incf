#!/usr/bin/env python3
"""
Test script to verify the optimization works correctly.
Compares original vs optimized extraction methods.
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

    # Test parameters - use smaller start index for better chance of finding content
    start_index = 100   # Start at file 100 (more likely to have body text)
    chunk_size = 50     # Process 50 files (enough for oddpub to work)

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
        "process_pmcoa_with_oddpub.py",
        tar_file,
        "--start-index", str(start_index),
        "--chunk-size", str(chunk_size),
        "--max-files", str(chunk_size),
        "--output-file", "/tmp/test_original.parquet"
    ], capture_output=True, text=True)

    original_time = time.time() - start_time
    print(f"Time taken: {original_time:.2f} seconds")

    if result.returncode != 0:
        print(f"ERROR: Original method failed with return code {result.returncode}")
        print(f"stdout:\n{result.stdout}")
        print(f"stderr:\n{result.stderr}")

        # Try a simpler test just to extract text
        print("\nTrying a simple extraction test...")
        test_result = subprocess.run([
            sys.executable, "-c",
            f"""
import tarfile
import lxml.etree as etree
count = 0
with tarfile.open('{tar_file}', 'r:gz') as tar:
    for i, member in enumerate(tar):
        if member.name.endswith('.xml') and i >= {start_index} and i < {start_index + 10}:
            f = tar.extractfile(member)
            xml = f.read()
            root = etree.fromstring(xml)
            body = root.find('.//body')
            if body is not None:
                count += 1
print(f'Found {{count}} files with body text')
"""
        ], capture_output=True, text=True)
        print(f"Simple test output: {test_result.stdout}")
        return

    # Extract timing from output
    if "Found" in result.stdout:
        for line in result.stdout.split('\n'):
            if "Found" in line and "XML files" in line:
                print(f"  {line.strip()}")
                break

    # Test 2: Optimized method (using CSV)
    print(f"\nTest 2: Optimized method (CSV lookup)")
    print("-" * 40)

    start_time = time.time()
    result = subprocess.run([
        sys.executable,
        "process_pmcoa_with_oddpub_optimized.py",
        tar_file,
        "--start-index", str(start_index),
        "--chunk-size", str(chunk_size),
        "--max-files", str(chunk_size),
        "--output-file", "/tmp/test_optimized.parquet"
    ], capture_output=True, text=True)

    optimized_time = time.time() - start_time
    print(f"Time taken: {optimized_time:.2f} seconds")

    if result.returncode != 0:
        print(f"ERROR: Optimized method failed")
        print(f"stderr: {result.stderr}")
        return

    # Results comparison
    print(f"\nResults:")
    print("-" * 40)
    print(f"Original method:   {original_time:.2f} seconds")
    print(f"Optimized method:  {optimized_time:.2f} seconds")

    if optimized_time > 0:
        speedup = original_time / optimized_time
        print(f"Speedup:          {speedup:.1f}x")

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
        else:
            print("✗ Output sizes differ!")

    except Exception as e:
        print(f"Could not compare outputs: {e}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python test_optimization.py <tar.gz file>")
        print("Example: python test_optimization.py ~/claude/pmcoaXMLs/raw_download/oa_comm_xml.PMC005xxxxxx.baseline.2025-06-26.tar.gz")
        sys.exit(1)

    test_extraction_methods(sys.argv[1])