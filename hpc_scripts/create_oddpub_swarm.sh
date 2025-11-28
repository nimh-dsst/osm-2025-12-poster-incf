#!/bin/bash
# Create swarm file for processing tar.gz files with oddpub
# Splits large files into chunks to keep processing time under 10 hours per job
#
# Usage: create_oddpub_swarm.sh [tar_dir] [output_dir] [script_dir]

set -e

TAR_DIR="${1:-/data/pmcoa}"
OUTPUT_DIR="${2:-/data/oddpub_output}"
SCRIPT_DIR="${3:-/data/oddpub_scripts}"

# Chunk size: ~3000 XMLs per chunk = ~9 hours processing time
CHUNK_SIZE=3000

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
echo "  Chunk size: $CHUNK_SIZE XMLs (~9 hours per chunk)"
echo ""

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Count baseline tar.gz files
num_baseline=$(ls -1 "$TAR_DIR"/*baseline*.tar.gz 2>/dev/null | wc -l)

if [ "$num_baseline" -eq 0 ]; then
    echo "Error: No baseline tar.gz files found in $TAR_DIR"
    exit 1
fi

echo "Found $num_baseline baseline tar.gz files"

# Create swarm file
swarm_file="oddpub_swarm.txt"

> "$swarm_file"  # Clear file

total_jobs=0
files_split=0

for tarfile in "$TAR_DIR"/*baseline*.tar.gz; do
    basename=$(basename "$tarfile" .tar.gz)

    # Find corresponding CSV file
    csv_file="${tarfile%.tar.gz}.filelist.csv"

    if [ ! -f "$csv_file" ]; then
        echo "Warning: CSV file not found for $basename, skipping"
        continue
    fi

    # Count XMLs (subtract 1 for header)
    xml_count=$(($(wc -l < "$csv_file") - 1))

    # Check if file needs to be split
    if [ "$xml_count" -gt "$CHUNK_SIZE" ]; then
        # Calculate number of chunks
        num_chunks=$(( (xml_count + CHUNK_SIZE - 1) / CHUNK_SIZE ))
        files_split=$((files_split + 1))

        echo "  $basename: $xml_count XMLs -> $num_chunks chunks"

        # Create a job for each chunk
        for ((chunk=0; chunk<num_chunks; chunk++)); do
            start_index=$((chunk * CHUNK_SIZE))
            output_file="$OUTPUT_DIR/${basename}_chunk${chunk}_results.parquet"

            echo "cd $SCRIPT_DIR && python process_pmcoa_with_oddpub.py --batch-size 500 --start-index $start_index --chunk-size $CHUNK_SIZE --output-file $output_file $tarfile" >> "$swarm_file"
            total_jobs=$((total_jobs + 1))
        done
    else
        # Small file, process in one job
        output_file="$OUTPUT_DIR/${basename}_results.parquet"
        echo "cd $SCRIPT_DIR && python process_pmcoa_with_oddpub.py --batch-size 500 --output-file $output_file $tarfile" >> "$swarm_file"
        total_jobs=$((total_jobs + 1))
    fi
done

echo ""
echo "Created $swarm_file with $total_jobs jobs"
echo "  Files split into chunks: $files_split"
echo "  Single-job files: $((num_baseline - files_split))"
echo ""
echo "Submit with:"
echo "  swarm -f $swarm_file -g 32 -t 8 --time 12:00:00 \\"
echo "        --module python/3.9 R/4.2 --logdir /data/oddpub_logs"
echo ""
echo "Monitor with:"
echo "  jobload -u \$USER"
echo "  watch -n 30 'ls -1 $OUTPUT_DIR/*.parquet | wc -l'"
