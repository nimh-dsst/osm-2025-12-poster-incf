#!/bin/bash
# Generate swarm file for oddpub processing using Apptainer container
# Usage: bash create_oddpub_swarm_container.sh <tar_dir> <output_dir> <container_sif>

set -e

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <tar_dir> <output_dir> <container_sif>"
    echo "Example: $0 /data/pmcoa /data/oddpub_output /data/\$USER/containers/oddpub.sif"
    exit 1
fi

TAR_DIR="$1"
OUTPUT_DIR="$2"
CONTAINER_SIF="$3"
CHUNK_SIZE=1000  # 1000 XMLs per chunk = ~1.9 hours processing time

if [ ! -d "$TAR_DIR" ]; then
    echo "Error: TAR_DIR does not exist: $TAR_DIR"
    exit 1
fi

if [ ! -f "$CONTAINER_SIF" ]; then
    echo "Error: Container file does not exist: $CONTAINER_SIF"
    exit 1
fi

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Count baseline tar.gz files
baseline_files=$(find "$TAR_DIR" -maxdepth 1 -name "*baseline*.tar.gz" | wc -l)
echo "Found $baseline_files baseline tar.gz files"

# Generate swarm file
SWARM_FILE="oddpub_swarm.txt"
> "$SWARM_FILE"

total_jobs=0
files_with_chunks=0
single_job_files=0

for tarfile in "$TAR_DIR"/*baseline*.tar.gz; do
    [ -f "$tarfile" ] || continue

    basename=$(basename "$tarfile" .tar.gz)
    csv_file="${tarfile%.tar.gz}.filelist.csv"

    # Check if CSV file exists
    if [ ! -f "$csv_file" ]; then
        echo "Warning: No CSV file found for $basename, skipping"
        continue
    fi

    # Count XMLs (subtract 1 for header line)
    xml_count=$(($(wc -l < "$csv_file") - 1))

    if [ "$xml_count" -gt "$CHUNK_SIZE" ]; then
        # File needs chunking
        num_chunks=$(( (xml_count + CHUNK_SIZE - 1) / CHUNK_SIZE ))
        files_with_chunks=$((files_with_chunks + 1))

        echo "  $basename: $xml_count XMLs -> $num_chunks chunks"

        # Create a swarm command for each chunk
        for ((chunk=0; chunk<num_chunks; chunk++)); do
            start_index=$((chunk * CHUNK_SIZE))
            output_file="$OUTPUT_DIR/${basename}_chunk${chunk}_results.parquet"

            # Apptainer exec command
            echo ". /usr/local/current/apptainer/app_conf/sing_binds && apptainer exec $CONTAINER_SIF python3 /scripts/process_pmcoa_with_oddpub.py --batch-size 500 --start-index $start_index --chunk-size $CHUNK_SIZE --output-file $output_file $tarfile" >> "$SWARM_FILE"

            total_jobs=$((total_jobs + 1))
        done
    else
        # Small file, process in one job
        single_job_files=$((single_job_files + 1))
        output_file="$OUTPUT_DIR/${basename}_results.parquet"

        # Apptainer exec command
        echo ". /usr/local/current/apptainer/app_conf/sing_binds && apptainer exec $CONTAINER_SIF python3 /scripts/process_pmcoa_with_oddpub.py --batch-size 500 --output-file $output_file $tarfile" >> "$SWARM_FILE"

        total_jobs=$((total_jobs + 1))
    fi
done

echo ""
echo "Created $SWARM_FILE with $total_jobs jobs"
echo "  Files split into chunks: $files_with_chunks"
echo "  Single-job files: $single_job_files"
echo ""
echo "Submit with:"
echo "  swarm -f $SWARM_FILE -g 32 -t 8 --time 14:00:00 --module apptainer --logdir /data/oddpub_logs"
