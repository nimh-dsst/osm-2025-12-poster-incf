#!/bin/bash
# Verify oddpub output completeness for extracted XML files and generate retry swarm
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

# Count actual output files
actual_outputs=$(find "$OUTPUT_DIR" -name "*.parquet" -type f | wc -l)
echo "Found $actual_outputs existing output files in $OUTPUT_DIR"
echo ""

# Generate retry swarm file
RETRY_SWARM_FILE="oddpub_extracted_retry_swarm.txt"
> "$RETRY_SWARM_FILE"

total_expected=0
total_missing=0
subdirs_with_missing=0

echo "Checking for missing outputs..."
echo ""

for subdir in "$XML_BASE_DIR"/PMC*; do
    [ -d "$subdir" ] || continue

    dirname=$(basename "$subdir")

    # Count XML files in this subdirectory
    xml_count=$(find "$subdir" -name "*.xml" -type f | wc -l)

    if [ "$xml_count" -eq 0 ]; then
        continue
    fi

    # Calculate expected outputs
    if [ "$xml_count" -le "$FILES_PER_JOB" ]; then
        # Single job expected
        num_jobs=1
        total_expected=$((total_expected + 1))

        output_file="$OUTPUT_DIR/${dirname}_results.parquet"

        if [ ! -f "$output_file" ]; then
            total_missing=$((total_missing + 1))
            subdirs_with_missing=$((subdirs_with_missing + 1))
            echo "  $dirname: MISSING (single job, $xml_count files)"

            # Generate retry command
            echo ". /usr/local/current/apptainer/app_conf/sing_binds && apptainer exec $CONTAINER_SIF python3 /scripts/process_extracted_xmls_with_oddpub.py --batch-size 500 --output-file $output_file $subdir" >> "$RETRY_SWARM_FILE"
        fi
    else
        # Multiple jobs expected
        num_jobs=$(( (xml_count + FILES_PER_JOB - 1) / FILES_PER_JOB ))
        total_expected=$((total_expected + num_jobs))

        missing_jobs=0
        missing_job_list=""

        # Check each job
        for ((job=0; job<num_jobs; job++)); do
            output_file="$OUTPUT_DIR/${dirname}_chunk${job}_results.parquet"

            if [ ! -f "$output_file" ]; then
                missing_jobs=$((missing_jobs + 1))
                missing_job_list="$missing_job_list $job"

                # Generate retry command for this chunk
                start_idx=$((job * FILES_PER_JOB))

                # Get list of all XML files in sorted order
                xml_files=( $(find "$subdir" -name "*.xml" -type f | sort) )

                # Get this chunk of files
                chunk_files=("${xml_files[@]:$start_idx:$FILES_PER_JOB}")

                # Create a file list for this chunk
                chunk_list_file="$OUTPUT_DIR/.${dirname}_chunk${job}_files.txt"
                printf '%s\n' "${chunk_files[@]}" > "$chunk_list_file"

                # Add retry command
                echo ". /usr/local/current/apptainer/app_conf/sing_binds && cat $chunk_list_file | xargs -I {} apptainer exec $CONTAINER_SIF python3 /scripts/process_extracted_xmls_with_oddpub.py --batch-size 500 --output-file $output_file {}" >> "$RETRY_SWARM_FILE"
            fi
        done

        if [ "$missing_jobs" -gt 0 ]; then
            subdirs_with_missing=$((subdirs_with_missing + 1))
            total_missing=$((total_missing + missing_jobs))
            echo "  $dirname: $missing_jobs/$num_jobs jobs missing (chunks:$missing_job_list)"
        fi
    fi
done

echo ""
echo "==================== VERIFICATION SUMMARY ===================="
echo "Expected outputs:  $total_expected"
echo "Actual outputs:    $actual_outputs"
echo "Missing outputs:   $total_missing"
echo ""
echo "Subdirectories with missing outputs: $subdirs_with_missing"
echo ""

if [ "$total_missing" -eq 0 ]; then
    echo "✅ ALL JOBS COMPLETED SUCCESSFULLY!"
    echo ""
    echo "No retry needed. You can proceed to merge results."
    rm -f "$RETRY_SWARM_FILE"
else
    success_rate=$(awk "BEGIN {printf \"%.1f\", (($actual_outputs / $total_expected) * 100)}")
    failure_rate=$(awk "BEGIN {printf \"%.1f\", (($total_missing / $total_expected) * 100)}")

    echo "Success rate:  $success_rate% ($actual_outputs/$total_expected)"
    echo "Failure rate:  $failure_rate% ($total_missing/$total_expected)"
    echo ""
    echo "⚠️  RETRY NEEDED"
    echo ""
    echo "Created $RETRY_SWARM_FILE with $total_missing retry jobs"
    echo ""
    echo "Submit retry jobs with:"
    echo ""
    echo "  swarm -f $RETRY_SWARM_FILE \\"
    echo "      -g 32 \\"
    echo "      -t 8 \\"
    echo "      --time 02:00:00 \\"
    echo "      --gres=lscratch:10 \\"
    echo "      --module apptainer \\"
    echo "      --logdir /data/NIMH_scratch/adamt/osm/logs/oddpub_extracted_retry"
fi

echo "=============================================================="
