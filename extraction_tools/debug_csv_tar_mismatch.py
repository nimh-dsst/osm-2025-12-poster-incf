#!/usr/bin/env python3
"""
Debug script to check if CSV file entries match tar contents
"""
import pandas as pd
import tarfile
import sys

if len(sys.argv) != 3:
    print("Usage: python debug_csv_tar_mismatch.py <csv_file> <tar_file>")
    print("Example: python debug_csv_tar_mismatch.py file.filelist.csv file.tar.gz")
    sys.exit(1)

csv_file = sys.argv[1]
tar_file = sys.argv[2]

# Read CSV
print(f"Reading CSV: {csv_file}")
df = pd.read_csv(csv_file)
print(f"CSV has {len(df)} entries")
print(f"Columns: {df.columns.tolist()}")

# Check first few entries
print("\nFirst 5 CSV entries:")
for i in range(min(5, len(df))):
    print(f"  {df['Article File'].iloc[i]}")

# Get tar members
print(f"\nReading tar: {tar_file}")
with tarfile.open(tar_file, 'r:gz') as tar:
    members = {m.name for m in tar.getmembers() if m.isfile()}
    print(f"Tar has {len(members)} files")

# Check first few tar entries
print("\nFirst 5 tar entries:")
for i, name in enumerate(sorted(members)):
    if i >= 5:
        break
    print(f"  {name}")

# Check if CSV entries exist in tar
print("\nChecking if CSV entries exist in tar...")
not_found = 0
for i, row in df.iterrows():
    if i >= 10:  # Only check first 10
        break
    article_file = row['Article File']
    if article_file in members:
        print(f"✓ Found: {article_file}")
    else:
        print(f"✗ NOT FOUND: {article_file}")
        not_found += 1

if not_found > 0:
    print(f"\n{not_found} out of 10 checked entries were not found in tar")
else:
    print("\nAll checked entries were found in tar")