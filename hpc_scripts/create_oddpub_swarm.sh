#!/bin/bash
# Create swarm file for processing tar.gz files with oddpub
#
# Usage: create_oddpub_swarm.sh [tar_dir] [output_dir] [script_dir]

set -e

TAR_DIR="${1:-/data/pmcoa}"
OUTPUT_DIR="${2:-/data/oddpub_output}"
SCRIPT_DIR="${3:-/data/oddpub_scripts}"

if [ ! -d "$TAR_DIR" ]; then
    echo "Error: tar.gz directory does not exist: $TAR_DIR"
    echo "Usage: $0 [tar_dir] [output_dir] [script_dir]"
    exit 1
fi

if [ ! -f "$SCRIPT_DIR/process_pmcoa_with_oddpub.py" ]; then
    echo "Error: Processing script not found: $SCRIPT_DIR/process_pmcoa_with_oddpub.py"
    exit 1
fi

echo "Creating swarm file for oddpub processing..."
echo "  tar.gz directory: $TAR_DIR"
echo "  Output directory: $OUTPUT_DIR"
echo "  Script directory: $SCRIPT_DIR"
echo ""

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Count tar.gz files
num_tars=$(ls -1 "$TAR_DIR"/*.tar.gz 2>/dev/null | wc -l)

if [ "$num_tars" -eq 0 ]; then
    echo "Error: No tar.gz files found in $TAR_DIR"
    exit 1
fi

echo "Found $num_tars tar.gz files"

# Create swarm file
swarm_file="oddpub_swarm.txt"

> "$swarm_file"  # Clear file

for tarfile in "$TAR_DIR"/*.tar.gz; do
    basename=$(basename "$tarfile" .tar.gz)
    output_file="$OUTPUT_DIR/${basename}_results.parquet"

    echo "python $SCRIPT_DIR/process_pmcoa_with_oddpub.py --batch-size 500 --output-file $output_file $tarfile" >> "$swarm_file"
done

echo ""
echo "Created $swarm_file with $num_tars jobs"
echo ""
echo "Submit with:"
echo "  swarm -f $swarm_file -g 32 -t 8 --time 01:00:00 \\"
echo "        --module python/3.9 R/4.2 --logdir /data/oddpub_logs"
echo ""
echo "Monitor with:"
echo "  jobload -u \$USER"
echo "  watch -n 30 'ls -1 $OUTPUT_DIR/*.parquet | wc -l'"
