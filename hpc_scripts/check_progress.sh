#!/bin/bash
# Check progress of oddpub batch processing
#
# Usage: check_progress.sh [output_dir] [batch_dir]

OUTPUT_DIR="${1:-/data/oddpub_output}"
BATCH_DIR="${2:-/data/oddpub_batches}"

echo "Checking oddpub processing progress..."
echo "  Output directory: $OUTPUT_DIR"
echo "  Batch directory: $BATCH_DIR"
echo ""

# Count expected and completed batches
expected=$(ls -1 "$BATCH_DIR"/batch_*.txt 2>/dev/null | wc -l)
completed=$(ls -1 "$OUTPUT_DIR"/batch_*_results.parquet 2>/dev/null | wc -l)

if [ "$expected" -eq 0 ]; then
    echo "Error: No batch files found in $BATCH_DIR"
    exit 1
fi

pct=$(awk "BEGIN {printf \"%.1f\", 100*$completed/$expected}")

echo "Progress: $completed / $expected batches ($pct%)"
echo ""

if [ "$completed" -lt "$expected" ]; then
    missing=$((expected - completed))
    echo "Missing $missing batches"
    echo ""
    echo "To find missing batches:"
    echo "  comm -23 \\"
    echo "    <(seq -f \"batch_%05g_results.parquet\" 0 \$((expected-1)) | sort) \\"
    echo "    <(ls $OUTPUT_DIR/*.parquet | xargs -n1 basename | sort) \\"
    echo "    > missing_batches.txt"
    echo ""
else
    echo "All batches completed!"
    echo ""
    echo "Next step: Merge results"
    echo "  python merge_oddpub_results.py $OUTPUT_DIR /data/oddpub_results_final.parquet"
fi
