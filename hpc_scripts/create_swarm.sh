#!/bin/bash
# Create swarm file for oddpub batch processing
#
# Usage: create_swarm.sh [batch_dir] [output_dir]

set -e

BATCH_DIR="${1:-/data/oddpub_batches}"
OUTPUT_DIR="${2:-/data/oddpub_output}"

if [ ! -d "$BATCH_DIR" ]; then
    echo "Error: Batch directory does not exist: $BATCH_DIR"
    echo "Usage: $0 [batch_dir] [output_dir]"
    exit 1
fi

echo "Creating swarm file..."
echo "  Batch directory: $BATCH_DIR"
echo "  Output directory: $OUTPUT_DIR"
echo ""

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Count batch files
num_batches=$(ls -1 "$BATCH_DIR"/batch_*.txt 2>/dev/null | wc -l)

if [ "$num_batches" -eq 0 ]; then
    echo "Error: No batch files found in $BATCH_DIR"
    exit 1
fi

echo "Found $num_batches batch files"

# Create swarm file
swarm_file="oddpub_swarm.txt"

> "$swarm_file"  # Clear file

for batch_file in "$BATCH_DIR"/batch_*.txt; do
    batch_name=$(basename "$batch_file" .txt)
    output_file="$OUTPUT_DIR/${batch_name}_results.parquet"
    echo "python process_oddpub_batch.py $batch_file $output_file" >> "$swarm_file"
done

echo ""
echo "Created $swarm_file with $num_batches jobs"
echo ""
echo "Submit with:"
echo "  swarm -f $swarm_file -g 16 -t 4 --time 00:30:00 \\"
echo "        --module python/3.9 R/4.2 --logdir /data/oddpub_logs"
echo ""
echo "Monitor with:"
echo "  jobload -u \$USER"
