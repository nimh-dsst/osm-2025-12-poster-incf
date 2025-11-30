#!/usr/bin/env python3
"""
Debug script to check chunk range calculation
"""
import pandas as pd
import sys

if len(sys.argv) != 4:
    print("Usage: python debug_chunk_range.py <csv_file> <start_index> <chunk_size>")
    print("Example: python debug_chunk_range.py file.filelist.csv 26000 1000")
    sys.exit(1)

csv_file = sys.argv[1]
start_index = int(sys.argv[2])
chunk_size = int(sys.argv[3])

# Read CSV
print(f"Reading CSV: {csv_file}")
df = pd.read_csv(csv_file)
print(f"CSV has {len(df)} entries")

# Calculate chunk boundaries
total_files = len(df)
end_index = min(start_index + chunk_size, total_files)

print(f"\nChunk calculation:")
print(f"  Start index: {start_index}")
print(f"  Chunk size: {chunk_size}")
print(f"  Total files in CSV: {total_files}")
print(f"  End index: {end_index}")
print(f"  Files in this chunk: {end_index - start_index}")

# Get chunk
chunk_df = df.iloc[start_index:end_index]
print(f"\nActual chunk size: {len(chunk_df)}")

# Show first and last few entries
print(f"\nFirst 5 entries in chunk:")
for i in range(min(5, len(chunk_df))):
    print(f"  [{start_index + i}] {chunk_df['Article File'].iloc[i]}")

if len(chunk_df) > 5:
    print(f"\nLast 5 entries in chunk:")
    for i in range(max(0, len(chunk_df) - 5), len(chunk_df)):
        print(f"  [{start_index + i}] {chunk_df['Article File'].iloc[i]}")