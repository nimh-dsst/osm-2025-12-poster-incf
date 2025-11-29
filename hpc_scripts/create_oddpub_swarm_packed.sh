#!/bin/bash
# Generate PACKED swarm file for oddpub processing using Apptainer container
# This version runs 8 parallel jobs per node for efficient CPU utilization
# Usage: bash create_oddpub_swarm_packed.sh <tar_dir> <output_dir> <container_sif>

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
SWARM_FILE="oddpub_swarm_packed.txt"
> "$SWARM_FILE"

# Collect all commands in an array first
declare -a commands=()

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
        # File needs to be split into chunks
        num_chunks=$(( (xml_count + CHUNK_SIZE - 1) / CHUNK_SIZE ))

        # Process each chunk
        for ((chunk=0; chunk<num_chunks; chunk++)); do
            start_index=$((chunk * CHUNK_SIZE))
            output_file="$OUTPUT_DIR/${basename}_chunk${chunk}_results.parquet"

            # Skip if output already exists
            if [ -f "$output_file" ]; then
                echo "  Skipping existing: ${basename}_chunk${chunk}_results.parquet"
                continue
            fi

            # Add command to array
            commands+=( ". /usr/local/current/apptainer/app_conf/sing_binds && apptainer exec $CONTAINER_SIF python3 /scripts/process_pmcoa_with_oddpub.py --batch-size 500 --start-index $start_index --chunk-size $CHUNK_SIZE --output-file $output_file $tarfile" )
        done
    else
        # Single-job file
        output_file="$OUTPUT_DIR/${basename}_results.parquet"

        # Skip if output already exists
        if [ -f "$output_file" ]; then
            echo "  Skipping existing: ${basename}_results.parquet"
            continue
        fi

        # Add command to array
        commands+=( ". /usr/local/current/apptainer/app_conf/sing_binds && apptainer exec $CONTAINER_SIF python3 /scripts/process_pmcoa_with_oddpub.py --batch-size 500 --output-file $output_file $tarfile" )
    fi
done

total_jobs=${#commands[@]}
echo "Generated $total_jobs jobs (after skipping existing outputs)"

if [ "$total_jobs" -eq 0 ]; then
    echo "No jobs to run! All outputs already exist."
    exit 0
fi

# Now pack 8 commands per line in the swarm file
# This allows 8 single-threaded jobs to run in parallel on one node
for ((i=0; i<total_jobs; i+=8)); do
    # Get up to 8 commands for this line
    line_commands=""

    for ((j=0; j<8 && i+j<total_jobs; j++)); do
        if [ "$j" -eq 0 ]; then
            # First command, no separator
            line_commands="${commands[i+j]}"
        else
            # Subsequent commands, add " & " separator and background
            line_commands="$line_commands & ${commands[i+j]}"
        fi
    done

    # Add wait at the end to ensure all background jobs complete
    echo "$line_commands & wait" >> "$SWARM_FILE"
done

# Count packed lines
packed_lines=$(wc -l < "$SWARM_FILE")

echo ""
echo "==================== PACKED SWARM FILE CREATED ===================="
echo "Swarm file: $SWARM_FILE"
echo "Total individual jobs: $total_jobs"
echo "Packed swarm lines: $packed_lines (8 jobs per line)"
echo "Expected nodes needed: ~$packed_lines"
echo ""
echo "This packed approach will:"
echo "  - Run 8 single-threaded jobs in parallel per node"
echo "  - Use ~100% of allocated CPUs (instead of 12-50%)"
echo "  - Complete ~8x faster with the same node allocation"
echo ""
echo "Submit with:"
echo ""
echo "  swarm -f $SWARM_FILE \\"
echo "      -g 32 \\"
echo "      -t 8 \\"
echo "      --time 03:00:00 \\"
echo "      --gres=lscratch:10 \\"
echo "      --module apptainer \\"
echo "      --logdir /data/NIMH_scratch/adamt/osm/logs/oddpub_\$(date +%Y%m%d_%H%M%S)"
echo ""
echo "Note: Each line runs 8 jobs in parallel, so processing time remains ~1.9 hours per line"
echo "=================================================================="