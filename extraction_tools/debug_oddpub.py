#!/usr/bin/env python3
"""
Debug script to test oddpub processing on a small sample
"""

import sys
import subprocess
from pathlib import Path

def test_oddpub_minimal():
    """Test oddpub with minimal files to debug issues."""

    # Use a smaller tar file if available
    test_files = [
        "/data/NIMH_scratch/licc/pmcoa/files/oa_other_xml.incr.2025-07-03.tar.gz",
        "/data/NIMH_scratch/licc/pmcoa/files/oa_comm_xml.PMC000xxxxxx.baseline.2025-06-26.tar.gz"
    ]

    tar_file = None
    for f in test_files:
        if Path(f).exists():
            tar_file = f
            break

    if not tar_file:
        print("No test tar file found")
        return

    print(f"Testing with: {tar_file}")

    # Run with very verbose output
    cmd = [
        sys.executable,
        "process_pmcoa_with_oddpub.py",
        tar_file,
        "--batch-size", "10",
        "--max-files", "20",
        "--output-file", "/tmp/debug_oddpub.parquet"
    ]

    print(f"\nRunning command:")
    print(" ".join(cmd))
    print()

    # Run with real-time output
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    for line in process.stdout:
        print(line, end='')

    returncode = process.wait()

    if returncode == 0:
        print("\n\nSUCCESS! Check /tmp/debug_oddpub.parquet")

        # Try to read the output
        try:
            import pandas as pd
            df = pd.read_parquet("/tmp/debug_oddpub.parquet")
            print(f"\nOutput contains {len(df)} records")
            print(f"Columns: {list(df.columns)}")
        except Exception as e:
            print(f"\nError reading output: {e}")
    else:
        print(f"\n\nFAILED with return code {returncode}")

if __name__ == "__main__":
    test_oddpub_minimal()