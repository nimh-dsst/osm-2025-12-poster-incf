#!/bin/bash
# Analyze oddpub HPC processing progress
# Usage: bash analyze_oddpub_progress.sh <output_dir> <log_dir>

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <output_dir> <log_dir>"
    echo "Example: $0 ~/claude/osm-oddpub-out ~/claude/hpc_logs/oddpub_retry"
    exit 1
fi

OUTPUT_DIR="$1"
LOG_DIR="$2"

echo "==================== ODDPUB PROCESSING PROGRESS ===================="
echo "Output directory: $OUTPUT_DIR"
echo "Log directory: $LOG_DIR"
echo "Check time: $(date)"
echo ""

# Count outputs
output_count=$(find "$OUTPUT_DIR" -name "*.parquet" -type f 2>/dev/null | wc -l)
echo "Output files created: $output_count"

# Count logs
error_log_count=$(find "$LOG_DIR" -name "*.e" -type f 2>/dev/null | wc -l)
output_log_count=$(find "$LOG_DIR" -name "*.o" -type f 2>/dev/null | wc -l)
echo "Log files: $error_log_count error logs, $output_log_count output logs"
echo ""

# Check for errors
echo "Checking for errors in logs..."
errors=$(grep -l "ERROR\|Failed\|Exception" "$LOG_DIR"/*.e 2>/dev/null | wc -l)
timeouts=$(grep -l "timeout\|timed out" "$LOG_DIR"/*.e 2>/dev/null | wc -l)

echo "  Logs with ERROR/Failed/Exception: $errors"
echo "  Logs with timeout: $timeouts"
echo ""

# Sample recent completions
recent_outputs=$(find "$OUTPUT_DIR" -name "*.parquet" -type f -mmin -30 2>/dev/null | wc -l)
echo "Files created in last 30 minutes: $recent_outputs"
echo ""

# Sample output file sizes
echo "Sample output file sizes:"
find "$OUTPUT_DIR" -name "*.parquet" -type f 2>/dev/null | head -5 | xargs -I {} sh -c 'ls -lh "{}" | awk "{print \$5, \$9}"'
echo ""

# Estimate progress (assumes 7,038 total jobs)
total_expected=7038
if [ "$output_count" -gt 0 ]; then
    percent=$(awk "BEGIN {printf \"%.1f\", ($output_count / $total_expected) * 100}")
    remaining=$((total_expected - output_count))

    echo "==================== PROGRESS ESTIMATE ===================="
    echo "Completed: $output_count / $total_expected ($percent%)"
    echo "Remaining: $remaining jobs"

    # Calculate completion rate if we have multiple outputs
    if [ "$recent_outputs" -gt 0 ]; then
        rate_per_30min=$recent_outputs
        rate_per_hour=$(echo "$rate_per_30min * 2" | bc)
        hours_remaining=$(awk "BEGIN {printf \"%.1f\", $remaining / $rate_per_hour}")

        echo ""
        echo "Processing rate: ~$rate_per_hour jobs/hour"
        echo "Estimated time to completion: ~${hours_remaining} hours"
    fi
fi

echo ""
echo "=================================================================="
