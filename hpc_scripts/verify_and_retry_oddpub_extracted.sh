#!/bin/bash
# Verify oddpub output completeness for extracted XML files and generate retry swarm
#
# This script identifies incomplete chunks by comparing:
# 1. Expected PMCIDs per chunk (from file lists)
# 2. Actual PMCIDs in output parquet files
# 3. Currently queued/running jobs (from squeue)
#
# A chunk needs retry if:
# - Output parquet is missing or has fewer records than expected, AND
# - No job for that chunk is currently queued or running
#
# Usage: bash verify_and_retry_oddpub_extracted.sh <xml_base_dir> <output_dir> <container_sif> [OPTIONS]

set -e

usage() {
    echo "Usage: $0 <xml_base_dir> <output_dir> <container_sif> [OPTIONS]"
    echo ""
    echo "Arguments:"
    echo "  xml_base_dir    Directory containing PMC XML subdirectories and CSV file lists"
    echo "  output_dir      Directory for oddpub output parquet files"
    echo "  container_sif   Path to oddpub Apptainer container"
    echo ""
    echo "Options:"
    echo "  --jobs-per-line <n>   Jobs to pack per swarm line (default: 4)"
    echo "  --files-per-job <n>   Files per job/chunk (default: 1000)"
    echo "  --dry-run             Show what would be done without creating files"
    echo "  --tolerance <n>       Allow up to n missing records per chunk (default: 0)"
    echo "  --user <username>     Check jobs for this user (default: current user)"
    echo "  --skip-queue-check    Skip checking squeue (useful if not on HPC login node)"
    echo ""
    echo "Example:"
    echo "  $0 /data/NIMH_scratch/adamt/pmcoa \\"
    echo "     /data/NIMH_scratch/adamt/osm/oddpub_output \\"
    echo "     /data/adamt/containers/oddpub_optimized.sif"
    exit 1
}

# Parse arguments
if [ "$#" -lt 3 ]; then
    usage
fi

XML_BASE_DIR="$1"
OUTPUT_DIR="$2"
CONTAINER_SIF="$3"
shift 3

# Default values
FILES_PER_JOB=1000
JOBS_PER_LINE=4
DRY_RUN=false
TOLERANCE=0
HPC_USER=$(whoami)
SKIP_QUEUE_CHECK=false
REMOTE_TRACKING_HOST="osm2025"
REMOTE_TRACKING_DIR="/home/ec2-user/claude/osm-2025-12-poster-incf/hpc_scripts"

# Parse optional arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --jobs-per-line)
            JOBS_PER_LINE="$2"
            shift 2
            ;;
        --files-per-job)
            FILES_PER_JOB="$2"
            shift 2
            ;;
        --tolerance)
            TOLERANCE="$2"
            shift 2
            ;;
        --user)
            HPC_USER="$2"
            shift 2
            ;;
        --skip-queue-check)
            SKIP_QUEUE_CHECK=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Old output directory with different naming convention (from tarball processing)
OLD_OUTPUT_DIR="/data/NIMH_scratch/adamt/osm/osm-2025-12-poster-incf/output"

# Validate inputs
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

echo "=============================================================="
echo "ODDPUB VERIFICATION AND RETRY GENERATOR"
echo "=============================================================="
echo "XML base dir:      $XML_BASE_DIR"
echo "Output dir:        $OUTPUT_DIR"
echo "Old output dir:    $OLD_OUTPUT_DIR"
echo "Container:         $CONTAINER_SIF"
echo "Files per job:     $FILES_PER_JOB"
echo "Jobs per line:     $JOBS_PER_LINE"
echo "Tolerance:         $TOLERANCE missing records allowed per chunk"
echo "HPC user:          $HPC_USER"
echo ""

# Function to count records in a parquet file (using metadata for speed)
count_parquet_records() {
    local parquet_file="$1"
    local count
    # Use pyarrow metadata for fast row count (doesn't read data)
    count=$(python3 -c "import pyarrow.parquet as pq; print(pq.ParquetFile('$parquet_file').metadata.num_rows)" 2>/dev/null) || count="0"
    echo "${count:-0}"
}

# Function to find output file for a chunk (checks both new and old naming)
find_output_file() {
    local chunk_name="$1"  # e.g., PMC005xxxxxx_chunk3 or PMC000xxxxxx

    # Check new-style output first
    local new_file="$OUTPUT_DIR/${chunk_name}_results.parquet"
    if [ -f "$new_file" ]; then
        echo "$new_file"
        return 0
    fi

    # Check old-style output (glob for date portion and oa_*_xml prefix)
    # Extract PMC range from chunk name
    local pmc_range=$(echo "$chunk_name" | grep -oP 'PMC\d+x+')

    if [[ "$chunk_name" =~ _chunk([0-9]+)$ ]]; then
        local chunk_num="${BASH_REMATCH[1]}"
        local old_pattern="$OLD_OUTPUT_DIR/oa_*_xml.${pmc_range}.baseline.*_chunk${chunk_num}_results.parquet"
    else
        local old_pattern="$OLD_OUTPUT_DIR/oa_*_xml.${pmc_range}.baseline.*_results.parquet"
    fi

    # Find matching file
    local old_file=$(compgen -G "$old_pattern" 2>/dev/null | head -1)
    if [ -n "$old_file" ] && [ -f "$old_file" ]; then
        echo "$old_file"
        return 0
    fi

    return 1
}

# Step 1: Get currently queued/running jobs from squeue
echo "Step 1: Checking HPC queue for running/pending jobs..."

declare -A queued_chunks

if [ "$SKIP_QUEUE_CHECK" = true ]; then
    echo "  Skipping queue check (--skip-queue-check specified)"
else
    # Check if squeue is available
    if ! command -v squeue &> /dev/null; then
        echo "  Warning: squeue not found. Skipping queue check."
        echo "  (Use --skip-queue-check to suppress this warning)"
    else
        # Get all jobs for user
        job_ids=$(squeue -u "$HPC_USER" -h -o "%i" 2>/dev/null || true)

        if [ -n "$job_ids" ]; then
            job_count=$(echo "$job_ids" | wc -l)
            echo "  Found $job_count jobs in queue for user $HPC_USER"

            # For swarm jobs, the batch script references cmd.* files in a swarm directory
            # We need to find these directories and read ALL cmd.* files
            declare -A swarm_dirs_checked

            for job_id in $job_ids; do
                # Get the batch script path from scontrol
                batch_script=$(scontrol show job "$job_id" 2>/dev/null | grep -oP '(?<=Command=)[^\s]+' || true)

                if [ -n "$batch_script" ]; then
                    # Extract the swarm directory (parent of swarm.batch)
                    swarm_dir=$(dirname "$batch_script")

                    if [ -d "$swarm_dir" ] && [ -z "${swarm_dirs_checked[$swarm_dir]}" ]; then
                        swarm_dirs_checked["$swarm_dir"]=1

                        # Read ALL cmd.* files in this swarm directory
                        for cmd_file in "$swarm_dir"/cmd.*; do
                            [ -f "$cmd_file" ] || continue
                            # Extract chunk names from output file patterns
                            # Pattern: PMC\d+xxxxxx(_chunk\d+)?_results.parquet
                            chunks_in_cmd=$(grep -oP 'PMC\d+x+(_chunk\d+)?(?=_results\.parquet)' "$cmd_file" 2>/dev/null || true)
                            for chunk in $chunks_in_cmd; do
                                queued_chunks["$chunk"]=1
                            done
                        done
                    fi
                fi
            done

            echo "  Checked ${#swarm_dirs_checked[@]} swarm directories"
            echo "  Identified ${#queued_chunks[@]} chunks currently in queue"
        else
            echo "  No jobs found in queue for user $HPC_USER"
        fi
    fi
fi

# Step 2: Build list of expected chunks and their expected record counts
echo ""
echo "Step 2: Calculating expected chunks from CSV file lists..."

declare -A expected_counts      # chunk_name -> expected record count
declare -A chunk_to_csv         # chunk_name -> csv_file:chunk_num
declare -A chunk_to_filelist    # chunk_name -> file list path

for csv_file in "$XML_BASE_DIR"/*.baseline.*.filelist.csv; do
    [ -f "$csv_file" ] || continue

    # Extract the PMC directory name (e.g., PMC005xxxxxx)
    pmc_dir=$(basename "$csv_file" | grep -oP 'PMC\d+x+')

    if [ -z "$pmc_dir" ]; then
        continue
    fi

    # Check if corresponding XML directory exists
    xml_subdir="$XML_BASE_DIR/$pmc_dir"
    if [ ! -d "$xml_subdir" ]; then
        continue
    fi

    # Count XMLs (subtract 1 for header line)
    xml_count=$(($(wc -l < "$csv_file") - 1))

    if [ "$xml_count" -le 0 ]; then
        continue
    fi

    # Calculate expected chunks
    if [ "$xml_count" -le "$FILES_PER_JOB" ]; then
        # Single job (no chunk suffix)
        expected_counts["$pmc_dir"]=$xml_count
        chunk_to_csv["$pmc_dir"]="$csv_file"
        chunk_to_filelist["$pmc_dir"]="$OUTPUT_DIR/.${pmc_dir}_files.txt"
    else
        # Multiple chunks
        num_jobs=$(( (xml_count + FILES_PER_JOB - 1) / FILES_PER_JOB ))
        for ((job=0; job<num_jobs; job++)); do
            key="${pmc_dir}_chunk${job}"

            # Calculate expected count for this chunk
            start_idx=$((job * FILES_PER_JOB))
            remaining=$((xml_count - start_idx))
            if [ "$remaining" -gt "$FILES_PER_JOB" ]; then
                chunk_count=$FILES_PER_JOB
            else
                chunk_count=$remaining
            fi

            expected_counts["$key"]=$chunk_count
            chunk_to_csv["$key"]="$csv_file:$job"
            chunk_to_filelist["$key"]="$OUTPUT_DIR/.${key}_files.txt"
        done
    fi
done

total_expected=${#expected_counts[@]}
echo "  Found $total_expected expected chunks"

# Step 3: Check each chunk for completeness
echo ""
echo "Step 3: Verifying chunk completeness..."

declare -a incomplete_chunks
declare -A incomplete_reason
complete_count=0
missing_count=0
partial_count=0
queued_count=0

chunk_num=0
for chunk_name in $(echo "${!expected_counts[@]}" | tr ' ' '\n' | sort); do
    chunk_num=$((chunk_num + 1))
    expected=${expected_counts[$chunk_name]}

    # Progress indicator every 500 chunks
    if [ $((chunk_num % 500)) -eq 0 ]; then
        echo "  Checked $chunk_num/$total_expected chunks..."
    fi

    # Find output file
    output_file=$(find_output_file "$chunk_name" || true)

    if [ -z "$output_file" ]; then
        # No output file exists - check if queued
        if [ -n "${queued_chunks[$chunk_name]}" ]; then
            queued_count=$((queued_count + 1))
            continue  # Skip - it's being processed
        fi
        incomplete_chunks+=("$chunk_name")
        incomplete_reason["$chunk_name"]="missing (expected $expected)"
        missing_count=$((missing_count + 1))
        continue
    fi

    # Count actual records in parquet
    actual=$(count_parquet_records "$output_file")

    if [ "$actual" -lt "$((expected - TOLERANCE))" ]; then
        # Incomplete - check if queued for retry
        if [ -n "${queued_chunks[$chunk_name]}" ]; then
            queued_count=$((queued_count + 1))
            continue  # Skip - it's being reprocessed
        fi
        incomplete_chunks+=("$chunk_name")
        incomplete_reason["$chunk_name"]="partial ($actual/$expected)"
        partial_count=$((partial_count + 1))
    else
        complete_count=$((complete_count + 1))
    fi
done

echo "  Checked all $total_expected chunks"

# Sort incomplete chunks
IFS=$'\n' sorted_incomplete=($(sort <<<"${incomplete_chunks[*]}")); unset IFS

# Step 4: Generate summary
echo ""
echo "=============================================================="
echo "VERIFICATION SUMMARY"
echo "=============================================================="
echo "Expected chunks:     $total_expected"
echo "Complete:            $complete_count"
echo "Currently queued:    $queued_count"
echo "Missing (no file):   $missing_count"
echo "Partial (incomplete):$partial_count"
echo "Need retry:          ${#sorted_incomplete[@]}"
echo ""

if [ "${#sorted_incomplete[@]}" -eq 0 ]; then
    echo "All chunks are complete or currently being processed!"
    echo "No retry swarm needed."
    exit 0
fi

# Calculate rates
completion_pct=$(awk "BEGIN {printf \"%.1f\", ($complete_count / $total_expected) * 100}")
incomplete_pct=$(awk "BEGIN {printf \"%.1f\", (${#sorted_incomplete[@]} / $total_expected) * 100}")

echo "Completion rate:     $completion_pct% ($complete_count/$total_expected)"
echo "Need retry:          $incomplete_pct% (${#sorted_incomplete[@]}/$total_expected)"
echo ""

# Show incomplete by PMC range
echo "Incomplete chunks by PMC range:"
declare -A incomplete_by_range
for chunk in "${sorted_incomplete[@]}"; do
    range=$(echo "$chunk" | grep -oP 'PMC\d+')
    incomplete_by_range["$range"]=$((${incomplete_by_range["$range"]:-0} + 1))
done
for range in $(echo "${!incomplete_by_range[@]}" | tr ' ' '\n' | sort); do
    echo "  ${range}xxxxxx: ${incomplete_by_range[$range]}"
done
echo ""

# Show sample of incomplete chunks with reasons
echo "Sample incomplete chunks (first 20):"
for chunk in "${sorted_incomplete[@]:0:20}"; do
    echo "  $chunk - ${incomplete_reason[$chunk]}"
done
echo ""

# Step 5: Generate retry commands
if [ "$DRY_RUN" = true ]; then
    echo "[DRY RUN] Would generate retry for ${#sorted_incomplete[@]} chunks"
    exit 0
fi

echo "Step 5: Generating retry swarm..."

declare -a commands

for chunk in "${sorted_incomplete[@]}"; do
    csv_info="${chunk_to_csv[$chunk]}"
    filelist="${chunk_to_filelist[$chunk]}"

    if [[ "$chunk" =~ ^(PMC[0-9]+x+)_chunk([0-9]+)$ ]]; then
        # Chunked job
        pmc_dir="${BASH_REMATCH[1]}"
        chunk_num="${BASH_REMATCH[2]}"
        csv_file="${csv_info%:*}"

        output_file="$OUTPUT_DIR/${chunk}_results.parquet"

        # Calculate line range for this chunk
        start_line=$((chunk_num * FILES_PER_JOB + 2))  # +2 to skip header
        end_line=$(( (chunk_num + 1) * FILES_PER_JOB + 1))

        # Create file list for this chunk
        sed -n "${start_line},${end_line}p" "$csv_file" | cut -d',' -f1 | sed "s|^|$XML_BASE_DIR/|" > "$filelist"

        commands+=("(. /usr/local/current/apptainer/app_conf/sing_binds && apptainer exec $CONTAINER_SIF python3 /scripts/process_extracted_xmls_with_oddpub.py --file-list $filelist --batch-size 500 --output-file $output_file)")
    else
        # Non-chunked job
        pmc_dir="$chunk"
        csv_file="$csv_info"

        output_file="$OUTPUT_DIR/${pmc_dir}_results.parquet"

        # Create file list from CSV
        tail -n +2 "$csv_file" | cut -d',' -f1 | sed "s|^|$XML_BASE_DIR/|" > "$filelist"

        commands+=("(. /usr/local/current/apptainer/app_conf/sing_binds && apptainer exec $CONTAINER_SIF python3 /scripts/process_extracted_xmls_with_oddpub.py --file-list $filelist --batch-size 500 --output-file $output_file)")
    fi
done

# Generate timestamped swarm file
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RETRY_SWARM_FILE="oddpub_retry_${TIMESTAMP}.swarm"
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

echo ""
echo "=============================================================="
echo "RETRY SWARM GENERATED"
echo "=============================================================="
echo "Created: $RETRY_SWARM_FILE"
echo "  - ${#commands[@]} individual jobs"
echo "  - $packed_lines packed swarm lines ($JOBS_PER_LINE jobs per line)"
echo ""

# Copy to remote tracking directory
echo "Copying swarm file to tracking directory..."
if scp "$RETRY_SWARM_FILE" "${REMOTE_TRACKING_HOST}:${REMOTE_TRACKING_DIR}/" 2>/dev/null; then
    echo "  Copied to ${REMOTE_TRACKING_HOST}:${REMOTE_TRACKING_DIR}/${RETRY_SWARM_FILE}"
else
    echo "  Warning: Could not copy to remote. Copy manually with:"
    echo "  scp $RETRY_SWARM_FILE ${REMOTE_TRACKING_HOST}:${REMOTE_TRACKING_DIR}/"
fi
echo ""

echo "Submit with:"
echo ""
echo "  swarm -f $RETRY_SWARM_FILE \\"
echo "      -g 32 \\"
echo "      -t 4 \\"
echo "      --time 06:00:00 \\"
echo "      --gres=lscratch:10 \\"
echo "      --module apptainer \\"
echo "      --logdir /data/NIMH_scratch/adamt/osm/logs/oddpub_retry"
echo ""
echo "=============================================================="
