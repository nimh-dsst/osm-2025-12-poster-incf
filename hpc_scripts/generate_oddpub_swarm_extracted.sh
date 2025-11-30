#!/bin/bash
# Generate swarm file for processing extracted XML files with oddpub
# This version processes already-extracted XML files (no tar.gz extraction needed)

set -e

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <xml_base_dir> <output_dir> <container_sif>"
    echo ""
    echo "Example:"
    echo "  $0 /data/NIMH_scratch/adamt/pmcoa \\"
    echo "     /data/NIMH_scratch/adamt/osm/oddpub_output \\"
    echo "     /data/adamt/containers/oddpub_optimized.sif"
    echo ""
    echo "XML base dir should contain subdirectories like PMC000xxxxxx, PMC001xxxxxx, etc."
    exit 1
fi

XML_BASE_DIR="$1"
OUTPUT_DIR="$2"
CONTAINER_SIF="$3"
FILES_PER_JOB=1000  # Process 1000 XMLs per job

if [ ! -d "$XML_BASE_DIR" ]; then
    echo "ERROR: XML base directory does not exist: $XML_BASE_DIR"
    exit 1
fi

if [ ! -f "$CONTAINER_SIF" ]; then
    echo "ERROR: Container file does not exist: $CONTAINER_SIF"
    exit 1
fi

# Create output directory if needed
mkdir -p "$OUTPUT_DIR"

# Generate swarm file
SWARM_FILE="oddpub_extracted_swarm.txt"
> "$SWARM_FILE"

total_jobs=0
total_files=0

echo "Scanning subdirectories in $XML_BASE_DIR..."
echo ""

for subdir in "$XML_BASE_DIR"/PMC*; do
    [ -d "$subdir" ] || continue

    dirname=$(basename "$subdir")

    # Count XML files in this subdirectory
    xml_count=$(find "$subdir" -name "*.xml" -type f | wc -l)

    if [ "$xml_count" -eq 0 ]; then
        echo "  $dirname: No XML files found, skipping"
        continue
    fi

    total_files=$((total_files + xml_count))

    # Calculate number of jobs needed
    if [ "$xml_count" -le "$FILES_PER_JOB" ]; then
        # Single job for this subdirectory
        num_jobs=1
        output_file="$OUTPUT_DIR/${dirname}_results.parquet"

        echo ". /usr/local/current/apptainer/app_conf/sing_binds && apptainer exec $CONTAINER_SIF python3 /scripts/process_extracted_xmls_with_oddpub.py --batch-size 500 --output-file $output_file $subdir" >> "$SWARM_FILE"

        total_jobs=$((total_jobs + 1))
        echo "  $dirname: $xml_count files -> 1 job"
    else
        # Split into multiple jobs
        num_jobs=$(( (xml_count + FILES_PER_JOB - 1) / FILES_PER_JOB ))

        # Get list of all XML files in sorted order
        xml_files=( $(find "$subdir" -name "*.xml" -type f | sort) )

        # Create jobs for each chunk
        for ((job=0; job<num_jobs; job++)); do
            start_idx=$((job * FILES_PER_JOB))
            end_idx=$(( (job + 1) * FILES_PER_JOB ))

            # Get this chunk of files
            chunk_files=("${xml_files[@]:$start_idx:$FILES_PER_JOB}")

            # Create a file list for this chunk
            chunk_list_file="$OUTPUT_DIR/.${dirname}_chunk${job}_files.txt"
            printf '%s\n' "${chunk_files[@]}" > "$chunk_list_file"

            output_file="$OUTPUT_DIR/${dirname}_chunk${job}_results.parquet"

            # Add command to swarm file
            # Use xargs to pass the file list
            echo ". /usr/local/current/apptainer/app_conf/sing_binds && cat $chunk_list_file | xargs -I {} apptainer exec $CONTAINER_SIF python3 /scripts/process_extracted_xmls_with_oddpub.py --batch-size 500 --output-file $output_file {}" >> "$SWARM_FILE"

            total_jobs=$((total_jobs + 1))
        done

        echo "  $dirname: $xml_count files -> $num_jobs jobs"
    fi
done

echo ""
echo "==================== SWARM GENERATION SUMMARY ===================="
echo "Total XML files found: $total_files"
echo "Total jobs created: $total_jobs"
echo "Files per job: ~$FILES_PER_JOB"
echo ""
echo "Created: $SWARM_FILE"
echo ""
echo "Submit with:"
echo ""
echo "  swarm -f $SWARM_FILE \\"
echo "      -g 32 \\"
echo "      -t 8 \\"
echo "      --time 02:00:00 \\"
echo "      --gres=lscratch:10 \\"
echo "      --module apptainer \\"
echo "      --logdir /data/NIMH_scratch/adamt/osm/logs/oddpub_extracted"
echo ""
echo "=================================================================="
