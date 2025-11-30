#!/bin/bash
# Generate swarm file for processing extracted XML files with oddpub
# FAST VERSION: Uses .filelist.csv files instead of find command

set -e

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <xml_base_dir> <output_dir> <container_sif>"
    echo ""
    echo "Example:"
    echo "  $0 /data/NIMH_scratch/adamt/pmcoa \\"
    echo "     /data/NIMH_scratch/adamt/osm/oddpub_output \\"
    echo "     /data/adamt/containers/oddpub_optimized.sif"
    echo ""
    echo "XML base dir should contain:"
    echo "  - Subdirectories: PMC000xxxxxx, PMC001xxxxxx, etc."
    echo "  - CSV files: *.filelist.csv (copied from tar.gz location)"
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

echo "Scanning for .filelist.csv files in $XML_BASE_DIR..."
echo ""

for csv_file in "$XML_BASE_DIR"/*.baseline.*.filelist.csv; do
    [ -f "$csv_file" ] || continue

    # Extract the base name (e.g., oa_comm_xml.PMC005xxxxxx.baseline.2025-06-26)
    basename=$(basename "$csv_file" .filelist.csv)

    # Extract just the PMC directory name (e.g., PMC005xxxxxx)
    pmc_dir=$(echo "$basename" | grep -oP 'PMC\d+x+')

    # Check if corresponding XML directory exists
    xml_subdir="$XML_BASE_DIR/$pmc_dir"
    if [ ! -d "$xml_subdir" ]; then
        echo "  Warning: XML directory not found for $basename, skipping"
        continue
    fi

    # Count XMLs (subtract 1 for header line)
    xml_count=$(($(wc -l < "$csv_file") - 1))

    if [ "$xml_count" -eq 0 ]; then
        echo "  $pmc_dir: No files in CSV, skipping"
        continue
    fi

    total_files=$((total_files + xml_count))

    # Calculate number of jobs needed
    if [ "$xml_count" -le "$FILES_PER_JOB" ]; then
        # Single job for this subdirectory
        num_jobs=1
        output_file="$OUTPUT_DIR/${pmc_dir}_results.parquet"

        # Create a temporary file list with full paths
        temp_filelist="$OUTPUT_DIR/.${pmc_dir}_files.txt"

        # Skip header and prepend XML_BASE_DIR to each path
        tail -n +2 "$csv_file" | cut -d',' -f1 | sed "s|^|$XML_BASE_DIR/|" > "$temp_filelist"

        # Generate swarm command that reads from file list
        echo ". /usr/local/current/apptainer/app_conf/sing_binds && cat $temp_filelist | tr '\\n' ' ' | xargs apptainer exec $CONTAINER_SIF python3 /scripts/process_extracted_xmls_with_oddpub.py --batch-size 500 --output-file $output_file" >> "$SWARM_FILE"

        total_jobs=$((total_jobs + 1))
        echo "  $pmc_dir: $xml_count files -> 1 job"
    else
        # Split into multiple jobs
        num_jobs=$(( (xml_count + FILES_PER_JOB - 1) / FILES_PER_JOB ))

        # Create jobs for each chunk
        for ((job=0; job<num_jobs; job++)); do
            start_line=$((job * FILES_PER_JOB + 2))  # +2 to skip header
            end_line=$(( (job + 1) * FILES_PER_JOB + 1))

            # Create a file list for this chunk with full paths
            chunk_list_file="$OUTPUT_DIR/.${pmc_dir}_chunk${job}_files.txt"

            # Extract chunk and prepend XML_BASE_DIR
            sed -n "${start_line},${end_line}p" "$csv_file" | cut -d',' -f1 | sed "s|^|$XML_BASE_DIR/|" > "$chunk_list_file"

            output_file="$OUTPUT_DIR/${pmc_dir}_chunk${job}_results.parquet"

            # Add command to swarm file
            echo ". /usr/local/current/apptainer/app_conf/sing_binds && cat $chunk_list_file | tr '\\n' ' ' | xargs apptainer exec $CONTAINER_SIF python3 /scripts/process_extracted_xmls_with_oddpub.py --batch-size 500 --output-file $output_file" >> "$SWARM_FILE"

            total_jobs=$((total_jobs + 1))
        done

        echo "  $pmc_dir: $xml_count files -> $num_jobs jobs"
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
