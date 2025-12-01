#!/bin/bash
# Verify oddpub output completeness for extracted XML files and generate retry swarm
#
# This script identifies missing chunks by comparing:
# 1. Expected chunks (from CSV file lists)
# 2. Completed outputs (parquet files in output directories)
# 3. Queued/running jobs (from existing swarm files in tracking directory)
#
# Swarm files are timestamped and saved to a tracking directory for reference.
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
    echo "  --swarm-dir <path>    Directory containing previous swarm files to check for queued jobs"
    echo "                        (default: current working directory)"
    echo "  --jobs-per-line <n>   Jobs to pack per swarm line (default: 4)"
    echo "  --files-per-job <n>   Files per job/chunk (default: 1000)"
    echo "  --dry-run             Show what would be done without creating files"
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
# Default swarm tracking directory - use current directory on HPC
SWARM_TRACKING_DIR="$(pwd)"
REMOTE_TRACKING_HOST="osm2025"
REMOTE_TRACKING_DIR="/home/ec2-user/claude/osm-2025-12-poster-incf/hpc_scripts"
DRY_RUN=false

# Parse optional arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --swarm-dir)
            SWARM_TRACKING_DIR="$2"
            shift 2
            ;;
        --jobs-per-line)
            JOBS_PER_LINE="$2"
            shift 2
            ;;
        --files-per-job)
            FILES_PER_JOB="$2"
            shift 2
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
echo "Swarm tracking:    $SWARM_TRACKING_DIR"
echo ""

# Step 1: Build set of completed outputs
echo "Step 1: Scanning for completed outputs..."

declare -A completed_chunks

# Scan new-style outputs in OUTPUT_DIR
for f in "$OUTPUT_DIR"/PMC*_results.parquet "$OUTPUT_DIR"/PMC*_chunk*_results.parquet; do
    [ -f "$f" ] || continue
    basename=$(basename "$f" _results.parquet)
    completed_chunks["$basename"]=1
done
new_count=${#completed_chunks[@]}
echo "  Found $new_count new-style completed outputs"

# Scan old-style outputs in OLD_OUTPUT_DIR
old_count=0
if [ -d "$OLD_OUTPUT_DIR" ]; then
    for f in "$OLD_OUTPUT_DIR"/oa_*_xml.PMC*_results.parquet; do
        [ -f "$f" ] || continue
        # Extract PMC range and chunk from filename like:
        # oa_comm_xml.PMC005xxxxxx.baseline.2025-06-26_chunk0_results.parquet
        basename=$(basename "$f")
        if [[ "$basename" =~ (PMC[0-9]+x+).*_chunk([0-9]+)_results\.parquet ]]; then
            pmc_range="${BASH_REMATCH[1]}"
            chunk_num="${BASH_REMATCH[2]}"
            key="${pmc_range}_chunk${chunk_num}"
            if [ -z "${completed_chunks[$key]}" ]; then
                completed_chunks["$key"]=1
                old_count=$((old_count + 1))
            fi
        elif [[ "$basename" =~ (PMC[0-9]+x+).*_results\.parquet ]]; then
            # Non-chunked output
            pmc_range="${BASH_REMATCH[1]}"
            if [ -z "${completed_chunks[$pmc_range]}" ]; then
                completed_chunks["$pmc_range"]=1
                old_count=$((old_count + 1))
            fi
        fi
    done
fi
echo "  Found $old_count additional old-style completed outputs"
echo "  Total completed: ${#completed_chunks[@]}"

# Step 2: Build set of queued/running jobs from ALL swarm files in tracking directory
echo ""
echo "Step 2: Scanning for queued/running jobs from swarm files..."

declare -A queued_chunks
swarm_files_found=0

if [ -d "$SWARM_TRACKING_DIR" ]; then
    for swarm_file in "$SWARM_TRACKING_DIR"/oddpub_extracted*_swarm*.txt; do
        [ -f "$swarm_file" ] || continue
        swarm_files_found=$((swarm_files_found + 1))

        # Extract chunk names from swarm file
        # Pattern: --output-file .../PMC001xxxxxx_chunk26_results.parquet
        while IFS= read -r line; do
            # Use grep to extract all output file paths
            for output_path in $(echo "$line" | grep -oP '(?<=--output-file\s)[^\s)]+'); do
                basename=$(basename "$output_path" _results.parquet)
                queued_chunks["$basename"]=1
            done
        done < "$swarm_file"
    done
fi

echo "  Scanned $swarm_files_found swarm files"
echo "  Found ${#queued_chunks[@]} chunks referenced in swarm files"

# Step 3: Calculate expected chunks from CSV file lists
echo ""
echo "Step 3: Calculating expected chunks from CSV file lists..."

declare -A expected_chunks
declare -A chunk_to_csv  # Map chunk name to CSV file for later use

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
        expected_chunks["$pmc_dir"]=1
        chunk_to_csv["$pmc_dir"]="$csv_file"
    else
        # Multiple chunks
        num_jobs=$(( (xml_count + FILES_PER_JOB - 1) / FILES_PER_JOB ))
        for ((job=0; job<num_jobs; job++)); do
            key="${pmc_dir}_chunk${job}"
            expected_chunks["$key"]=1
            chunk_to_csv["$key"]="$csv_file:$job"
        done
    fi
done

echo "  Found ${#expected_chunks[@]} expected chunks"

# Step 4: Identify truly missing chunks (not completed AND not queued)
echo ""
echo "Step 4: Identifying missing chunks..."

declare -a missing_chunks

for chunk in "${!expected_chunks[@]}"; do
    # Skip if completed
    if [ -n "${completed_chunks[$chunk]}" ]; then
        continue
    fi
    # Skip if already queued/in swarm file
    if [ -n "${queued_chunks[$chunk]}" ]; then
        continue
    fi
    missing_chunks+=("$chunk")
done

# Sort missing chunks for consistent output
IFS=$'\n' sorted_missing=($(sort <<<"${missing_chunks[*]}")); unset IFS

echo "  Missing chunks: ${#sorted_missing[@]}"

# Step 5: Generate summary
echo ""
echo "=============================================================="
echo "VERIFICATION SUMMARY"
echo "=============================================================="
echo "Expected chunks:     ${#expected_chunks[@]}"
echo "Completed:           ${#completed_chunks[@]}"
echo "Queued (in swarm):   ${#queued_chunks[@]}"
echo "Missing:             ${#sorted_missing[@]}"
echo ""

if [ "${#sorted_missing[@]}" -eq 0 ]; then
    echo "All chunks are either completed or queued!"
    echo "No retry swarm needed."
    exit 0
fi

# Calculate rates
total_expected=${#expected_chunks[@]}
total_done=$((${#completed_chunks[@]}))
total_pending=$((${#queued_chunks[@]} - ${#completed_chunks[@]}))
# Ensure pending doesn't go negative (queued chunks may have completed)
[ "$total_pending" -lt 0 ] && total_pending=0

completion_pct=$(awk "BEGIN {printf \"%.1f\", ($total_done / $total_expected) * 100}")
queued_pct=$(awk "BEGIN {printf \"%.1f\", (${#queued_chunks[@]} / $total_expected) * 100}")
missing_pct=$(awk "BEGIN {printf \"%.1f\", (${#sorted_missing[@]} / $total_expected) * 100}")

echo "Completion rate:     $completion_pct% ($total_done/$total_expected completed)"
echo "In queue/swarm:      $queued_pct% (${#queued_chunks[@]}/$total_expected)"
echo "Still missing:       $missing_pct% (${#sorted_missing[@]}/$total_expected)"
echo ""

# Show missing by PMC range
echo "Missing chunks by PMC range:"
declare -A missing_by_range
for chunk in "${sorted_missing[@]}"; do
    range=$(echo "$chunk" | grep -oP 'PMC\d+')
    missing_by_range["$range"]=$((${missing_by_range["$range"]:-0} + 1))
done
for range in $(echo "${!missing_by_range[@]}" | tr ' ' '\n' | sort); do
    echo "  ${range}xxxxxx: ${missing_by_range[$range]}"
done
echo ""

# Step 6: Generate retry commands
if [ "$DRY_RUN" = true ]; then
    echo "[DRY RUN] Would generate retry for ${#sorted_missing[@]} chunks"
    echo ""
    echo "Sample missing chunks (first 20):"
    for chunk in "${sorted_missing[@]:0:20}"; do
        echo "  $chunk"
    done
    exit 0
fi

echo "Step 6: Generating retry swarm..."

declare -a commands

for chunk in "${sorted_missing[@]}"; do
    csv_info="${chunk_to_csv[$chunk]}"

    if [[ "$chunk" =~ ^(PMC[0-9]+x+)_chunk([0-9]+)$ ]]; then
        # Chunked job
        pmc_dir="${BASH_REMATCH[1]}"
        chunk_num="${BASH_REMATCH[2]}"
        csv_file="${csv_info%:*}"

        output_file="$OUTPUT_DIR/${chunk}_results.parquet"
        chunk_list_file="$OUTPUT_DIR/.${chunk}_files.txt"

        # Calculate line range for this chunk
        start_line=$((chunk_num * FILES_PER_JOB + 2))  # +2 to skip header
        end_line=$(( (chunk_num + 1) * FILES_PER_JOB + 1))

        # Create file list for this chunk
        sed -n "${start_line},${end_line}p" "$csv_file" | cut -d',' -f1 | sed "s|^|$XML_BASE_DIR/|" > "$chunk_list_file"

        commands+=("(. /usr/local/current/apptainer/app_conf/sing_binds && apptainer exec $CONTAINER_SIF python3 /scripts/process_extracted_xmls_with_oddpub.py --file-list $chunk_list_file --batch-size 500 --output-file $output_file)")
    else
        # Non-chunked job
        pmc_dir="$chunk"
        csv_file="$csv_info"

        output_file="$OUTPUT_DIR/${pmc_dir}_results.parquet"
        temp_filelist="$OUTPUT_DIR/.${pmc_dir}_files.txt"

        # Create file list from CSV
        tail -n +2 "$csv_file" | cut -d',' -f1 | sed "s|^|$XML_BASE_DIR/|" > "$temp_filelist"

        commands+=("(. /usr/local/current/apptainer/app_conf/sing_binds && apptainer exec $CONTAINER_SIF python3 /scripts/process_extracted_xmls_with_oddpub.py --file-list $temp_filelist --batch-size 500 --output-file $output_file)")
    fi
done

# Generate timestamped swarm file
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RETRY_SWARM_FILE="oddpub_extracted_retry_swarm_${TIMESTAMP}.txt"
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
echo "      --logdir /data/NIMH_scratch/adamt/osm/logs/oddpub_extracted_retry"
echo ""
echo "=============================================================="
