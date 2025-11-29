#!/bin/bash
# Analyze oddpub HPC processing progress with better time estimates
# Usage: bash analyze_oddpub_progress_improved.sh <output_dir> <log_dir>

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
find "$OUTPUT_DIR" -name "*.parquet" -type f 2>/dev/null | sort | head -5 | xargs -I {} sh -c 'ls -lh "{}" | awk "{print \$5, \$9}"'
echo ""

# Analyze which archives are being processed
echo "Analyzing archive processing patterns..."
echo ""

# Check currently processing archives
current_archives=$(grep -h "Processing single tar.gz file" "$LOG_DIR"/*.o 2>/dev/null | \
    tail -20 | \
    sed 's/.*\/\(oa_[^\/]*\.tar\.gz\).*/\1/' | \
    sort | uniq -c | sort -nr)

if [ -n "$current_archives" ]; then
    echo "Recently processed archives:"
    echo "$current_archives"
    echo ""
fi

# Estimate progress with ranges (assumes 7,038 total jobs)
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

        echo ""
        echo "Processing rate: ~$rate_per_hour jobs/hour"

        # Calculate time range based on observed variability
        # Small archives (PMC000): ~3 chunks, Large archives (PMC009-011): ~500-600 chunks
        # Assuming remaining jobs are mix of sizes

        if [ "$remaining" -gt 0 ] && [ "$rate_per_hour" -gt 0 ]; then
            # Best case: mostly small archives
            best_case_hours=$(awk "BEGIN {printf \"%.1f\", $remaining / ($rate_per_hour * 2)}")

            # Worst case: mostly large archives
            worst_case_hours=$(awk "BEGIN {printf \"%.1f\", $remaining / ($rate_per_hour * 0.5)}")

            # Likely case: mixed
            likely_hours=$(awk "BEGIN {printf \"%.1f\", $remaining / $rate_per_hour}")

            echo ""
            echo "Estimated time to completion:"
            echo "  Best case (small archives): ~${best_case_hours} hours"
            echo "  Likely case (mixed sizes): ~${likely_hours} hours"
            echo "  Worst case (large archives): ~${worst_case_hours} hours"

            # Add context about archive sizes
            echo ""
            echo "Note: Archive sizes vary 200x (3K to 600K XMLs per archive)"
            echo "Current rate suggests processing mix of small and large archives"

            # Add specific context about slowdown
            if [ "$rate_per_hour" -lt 200 ]; then
                echo ""
                echo "⚠️  Processing has slowed - likely working on large archives"
                echo "Large archives (PMC009-011) have 500-600 chunks each"
                echo "vs small archives (PMC000-002) with only 3-50 chunks"
            fi
        fi
    else
        echo ""
        echo "No recent activity - jobs may be processing large archives"
        echo "Large archives (PMC009-011) can take 10-20 hours each"
    fi
fi

# Check if jobs are still running
running_jobs=$(squeue -u $USER 2>/dev/null | grep -c "swarm")
if [ "$running_jobs" -gt 0 ]; then
    echo ""
    echo "Active swarm jobs: $running_jobs"
fi

echo ""
echo "=================================================================="

# Provide recommendations
if [ "$remaining" -lt 100 ] && [ "$remaining" -gt 0 ]; then
    echo ""
    echo "RECOMMENDATION: Near completion - let it finish"
elif [ "$recent_outputs" -eq 0 ] && [ "$running_jobs" -gt 0 ]; then
    echo ""
    echo "RECOMMENDATION: Jobs likely processing large archives - be patient"
    echo "Check specific job progress with: jobload -u \$USER"
elif [ "$percent" == "100.0" ]; then
    echo ""
    echo "✅ PROCESSING COMPLETE! Ready to merge results."
fi