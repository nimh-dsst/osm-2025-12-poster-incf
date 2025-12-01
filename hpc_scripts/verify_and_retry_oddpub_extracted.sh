#!/bin/bash
# Verify oddpub output completeness for extracted XML files and generate retry swarm
# Uses CSV file lists for fast enumeration, packs 4 jobs per line
# Usage: bash verify_and_retry_oddpub_extracted.sh <xml_base_dir> <output_dir> <container_sif>

set -e

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <xml_base_dir> <output_dir> <container_sif>"
    echo "Example: $0 /data/NIMH_scratch/adamt/pmcoa /data/NIMH_scratch/adamt/osm/oddpub_output /data/adamt/containers/oddpub_optimized.sif"
    exit 1
fi

XML_BASE_DIR="$1"
OUTPUT_DIR="$2"
CONTAINER_SIF="$3"
FILES_PER_JOB=1000  # Must match the value used in generate script
JOBS_PER_LINE=4     # Pack 4 jobs per swarm line (reduced from 8 to avoid timeouts)

if [ ! -d "$XML_BASE_DIR" ]; then
    echo "Error: XML base directory does not exist: $XML_BASE_DIR"
    exit 1
fi

if [ ! -f "$CONTAINER_SIF" ]; then
    echo "Error: Container file does not exist: $CONTAINER_SIF"
    exit 1
fi

if [ ! -d "$OUTPUT_DIR" ]; then
    echo "Error: OUTPUT_DIR does not exist: $OUTPUT_DIR"
    exit 1
fi

# Count actual output files (new style PMC* only)
actual_outputs=$(ls "$OUTPUT_DIR"/PMC*.parquet 2>/dev/null | wc -l)
echo "Found $actual_outputs existing PMC* output files in $OUTPUT_DIR"
echo ""

# Collect retry commands in an array
declare -a commands=()

total_expected=0
total_missing=0

echo "Scanning CSV file lists in $XML_BASE_DIR..."
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
        continue
    fi

    # Calculate number of jobs needed
    if [ "$xml_count" -le "$FILES_PER_JOB" ]; then
        # Single job for this subdirectory
        total_expected=$((total_expected + 1))
        output_file="$OUTPUT_DIR/${pmc_dir}_results.parquet"

        if [ ! -f "$output_file" ]; then
            total_missing=$((total_missing + 1))

            # Create file list
            temp_filelist="$OUTPUT_DIR/.${pmc_dir}_files.txt"
            tail -n +2 "$csv_file" | cut -d',' -f1 | sed "s|^|$XML_BASE_DIR/|" > "$temp_filelist"

            # Add retry command
            commands+=("(. /usr/local/current/apptainer/app_conf/sing_binds && apptainer exec $CONTAINER_SIF python3 /scripts/process_extracted_xmls_with_oddpub.py --file-list $temp_filelist --batch-size 500 --output-file $output_file)")
        fi
    else
        # Multiple jobs expected
        num_jobs=$(( (xml_count + FILES_PER_JOB - 1) / FILES_PER_JOB ))
        total_expected=$((total_expected + num_jobs))

        # Check each job
        for ((job=0; job<num_jobs; job++)); do
            output_file="$OUTPUT_DIR/${pmc_dir}_chunk${job}_results.parquet"

            if [ ! -f "$output_file" ]; then
                total_missing=$((total_missing + 1))

                start_line=$((job * FILES_PER_JOB + 2))  # +2 to skip header
                end_line=$(( (job + 1) * FILES_PER_JOB + 1))

                # Create file list for this chunk
                chunk_list_file="$OUTPUT_DIR/.${pmc_dir}_chunk${job}_files.txt"
                sed -n "${start_line},${end_line}p" "$csv_file" | cut -d',' -f1 | sed "s|^|$XML_BASE_DIR/|" > "$chunk_list_file"

                # Add retry command
                commands+=("(. /usr/local/current/apptainer/app_conf/sing_binds && apptainer exec $CONTAINER_SIF python3 /scripts/process_extracted_xmls_with_oddpub.py --file-list $chunk_list_file --batch-size 500 --output-file $output_file)")
            fi
        done
    fi
done

echo ""
echo "==================== VERIFICATION SUMMARY ===================="
echo "Expected outputs:  $total_expected"
echo "Actual outputs:    $actual_outputs"
echo "Missing outputs:   $total_missing"
echo ""

if [ "$total_missing" -eq 0 ]; then
    echo "✅ ALL JOBS COMPLETED SUCCESSFULLY!"
    echo ""
    echo "No retry needed. You can proceed to merge results."
else
    # Generate packed retry swarm file
    RETRY_SWARM_FILE="oddpub_extracted_retry_swarm.txt"
    > "$RETRY_SWARM_FILE"

    # Pack JOBS_PER_LINE commands per line
    for ((i=0; i<${#commands[@]}; i+=JOBS_PER_LINE)); do
        line_commands=""

        for ((j=0; j<JOBS_PER_LINE && i+j<${#commands[@]}; j++)); do
            if [ "$j" -eq 0 ]; then
                line_commands="${commands[i+j]}"
            else
                line_commands="$line_commands & ${commands[i+j]}"
            fi
        done

        echo "$line_commands & wait" >> "$RETRY_SWARM_FILE"
    done

    packed_lines=$(wc -l < "$RETRY_SWARM_FILE")
    success_rate=$(awk "BEGIN {printf \"%.1f\", (($actual_outputs / $total_expected) * 100)}")
    failure_rate=$(awk "BEGIN {printf \"%.1f\", (($total_missing / $total_expected) * 100)}")

    echo "Success rate:  $success_rate% ($actual_outputs/$total_expected)"
    echo "Failure rate:  $failure_rate% ($total_missing/$total_expected)"
    echo ""
    echo "⚠️  RETRY NEEDED"
    echo ""
    echo "Created $RETRY_SWARM_FILE with:"
    echo "  - $total_missing individual jobs"
    echo "  - $packed_lines packed swarm lines ($JOBS_PER_LINE jobs per line)"
    echo ""
    echo "Submit retry jobs with:"
    echo ""
    echo "  swarm -f $RETRY_SWARM_FILE \\"
    echo "      -g 32 \\"
    echo "      -t 4 \\"
    echo "      --time 06:00:00 \\"
    echo "      --gres=lscratch:10 \\"
    echo "      --module apptainer \\"
    echo "      --logdir /data/NIMH_scratch/adamt/osm/logs/oddpub_extracted_retry"
fi

echo "=============================================================="
