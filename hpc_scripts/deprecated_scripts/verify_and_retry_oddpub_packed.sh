#!/bin/bash
# Verify oddpub output completeness and generate PACKED retry swarm for failed jobs
# This version packs 8 jobs per swarm line for efficient CPU utilization
# Usage: bash verify_and_retry_oddpub_packed.sh <tar_dir> <output_dir> <container_sif>

set -e

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <tar_dir> <output_dir> <container_sif>"
    echo "Example: $0 /data/NIMH_scratch/licc/pmcoa/files /data/NIMH_scratch/adamt/osm/osm-2025-12-poster-incf/output /data/adamt/containers/oddpub.sif"
    exit 1
fi

TAR_DIR="$1"
OUTPUT_DIR="$2"
CONTAINER_SIF="$3"
CHUNK_SIZE=1000  # Must match the chunk size used in original swarm

if [ ! -d "$TAR_DIR" ]; then
    echo "Error: TAR_DIR does not exist: $TAR_DIR"
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

# Collect retry commands in an array
declare -a commands=()

total_expected=0
total_missing=0
files_with_missing_chunks=0
single_job_files_missing=0

echo "Checking for missing outputs..."
echo ""

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
        # File was split into chunks
        num_chunks=$(( (xml_count + CHUNK_SIZE - 1) / CHUNK_SIZE ))
        total_expected=$((total_expected + num_chunks))

        missing_chunks=0
        missing_chunk_list=""

        # Check each chunk
        for ((chunk=0; chunk<num_chunks; chunk++)); do
            output_file="$OUTPUT_DIR/${basename}_chunk${chunk}_results.parquet"

            if [ ! -f "$output_file" ]; then
                missing_chunks=$((missing_chunks + 1))
                missing_chunk_list="$missing_chunk_list $chunk"

                # Add retry command to array
                start_index=$((chunk * CHUNK_SIZE))
                commands+=( ". /usr/local/current/apptainer/app_conf/sing_binds && apptainer exec $CONTAINER_SIF python3 /scripts/process_pmcoa_with_oddpub.py --batch-size 500 --start-index $start_index --chunk-size $CHUNK_SIZE --output-file $output_file $tarfile" )
            fi
        done

        if [ "$missing_chunks" -gt 0 ]; then
            files_with_missing_chunks=$((files_with_missing_chunks + 1))
            total_missing=$((total_missing + missing_chunks))
            echo "  $basename: $missing_chunks/$num_chunks chunks missing (chunks:$missing_chunk_list)"
        fi
    else
        # Single-job file
        total_expected=$((total_expected + 1))
        output_file="$OUTPUT_DIR/${basename}_results.parquet"

        if [ ! -f "$output_file" ]; then
            single_job_files_missing=$((single_job_files_missing + 1))
            total_missing=$((total_missing + 1))
            echo "  $basename: MISSING (single-job file)"

            # Add retry command to array
            commands+=( ". /usr/local/current/apptainer/app_conf/sing_binds && apptainer exec $CONTAINER_SIF python3 /scripts/process_pmcoa_with_oddpub.py --batch-size 500 --output-file $output_file $tarfile" )
        fi
    fi
done

echo ""
echo "==================== VERIFICATION SUMMARY ===================="
echo "Expected outputs:  $total_expected"
echo "Actual outputs:    $actual_outputs"
echo "Missing outputs:   $total_missing"
echo ""
echo "Files with missing chunks:  $files_with_missing_chunks"
echo "Missing single-job files:   $single_job_files_missing"
echo ""

if [ "$total_missing" -eq 0 ]; then
    echo "✅ ALL JOBS COMPLETED SUCCESSFULLY!"
    echo ""
    echo "No retry needed. You can proceed to merge results:"
    echo ""
    echo "  . /usr/local/current/apptainer/app_conf/sing_binds"
    echo "  apptainer exec $CONTAINER_SIF \\"
    echo "      python3 /scripts/merge_oddpub_results.py \\"
    echo "      $OUTPUT_DIR \\"
    echo "      /data/NIMH_scratch/adamt/osm/oddpub_results_final.parquet"
else
    # Generate packed retry swarm file
    RETRY_SWARM_FILE="oddpub_retry_packed_swarm.txt"
    > "$RETRY_SWARM_FILE"

    # Pack 8 commands per line
    for ((i=0; i<${#commands[@]}; i+=8)); do
        line_commands=""

        for ((j=0; j<8 && i+j<${#commands[@]}; j++)); do
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
    echo "  - $packed_lines packed swarm lines (8 jobs per line)"
    echo ""
    echo "This packed approach will:"
    echo "  - Run 8 single-threaded jobs in parallel per node"
    echo "  - Use ~100% of allocated CPUs (instead of 12-50%)"
    echo "  - Complete ~8x faster with the same node allocation"
    echo ""
    echo "Submit retry jobs with:"
    echo ""
    echo "  swarm -f $RETRY_SWARM_FILE \\"
    echo "      -g 32 \\"
    echo "      -t 8 \\"
    echo "      --time 03:00:00 \\"
    echo "      --gres=lscratch:10 \\"
    echo "      --module apptainer \\"
    echo "      --logdir /data/NIMH_scratch/adamt/osm/logs/oddpub_retry_\$(date +%Y%m%d_%H%M%S)"
    echo ""
    echo "IMPORTANT: Make sure you rebuild the container with the timeout fix first!"
    echo ""
    echo "  # On curium:"
    echo "  cd /data/adamt/osm-2025-12-poster-incf"
    echo "  gh repo sync --branch develop"
    echo "  cd container"
    echo "  sudo apptainer build --force oddpub.sif oddpub.def"
    echo "  scp oddpub.sif helix.nih.gov:/data/adamt/containers/"
fi

echo "=============================================================="