#!/usr/bin/env python3
"""
Quick script to check the column names in the filelist CSV files
"""
import pandas as pd
import sys

if len(sys.argv) != 2:
    print("Usage: python check_csv_columns.py <csv_file>")
    print("Example: python check_csv_columns.py /data/NIMH_scratch/licc/pmcoa/files/oa_comm_xml.PMC001xxxxxx.baseline.2025-06-26.filelist.csv")
    sys.exit(1)

csv_file = sys.argv[1]

try:
    # Read just the first few rows to check columns
    df = pd.read_csv(csv_file, nrows=5)
    print(f"CSV file: {csv_file}")
    print(f"Shape: {df.shape}")
    print(f"Columns: {df.columns.tolist()}")
    print("\nFirst few rows:")
    print(df)
except Exception as e:
    print(f"Error reading CSV: {e}")