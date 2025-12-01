#!/bin/bash
# Quick test script for oddpub extracted XML processing
# Usage: ./test_oddpub_extracted.sh [num_files]

set -e

NUM_FILES=${1:-10}
CONTAINER_SIF="/data/adamt/containers/oddpub_optimized.sif"
TEST_FILE_LIST="/tmp/test_${NUM_FILES}_files.txt"
OUTPUT_FILE="/data/NIMH_scratch/adamt/test_${NUM_FILES}_results.parquet"
SOURCE_FILE_LIST="/data/NIMH_scratch/adamt/osm/osm-2025-12-poster-incf/output/.PMC001xxxxxx_chunk5_files.txt"

echo "=============================================="
echo "Testing oddpub extracted XML processing"
echo "=============================================="
echo "Number of files: $NUM_FILES"
echo "Container: $CONTAINER_SIF"
echo "Output: $OUTPUT_FILE"
echo ""

# Create test file list
echo "Creating test file list with $NUM_FILES files..."
head -${NUM_FILES} "$SOURCE_FILE_LIST" > "$TEST_FILE_LIST"
echo "Test file list: $TEST_FILE_LIST"
cat "$TEST_FILE_LIST"
echo ""

# Check container exists
if [ ! -f "$CONTAINER_SIF" ]; then
    echo "ERROR: Container not found: $CONTAINER_SIF"
    echo "Rebuild with: apptainer build $CONTAINER_SIF container/oddpub_optimized.def"
    exit 1
fi

# Run the test
echo "Running oddpub processing..."
echo ""
. /usr/local/current/apptainer/app_conf/sing_binds && apptainer exec \
    "$CONTAINER_SIF" python3 \
    /scripts/process_extracted_xmls_with_oddpub.py \
    --file-list "$TEST_FILE_LIST" \
    --batch-size "$NUM_FILES" \
    --output-file "$OUTPUT_FILE"

echo ""
echo "=============================================="
echo "Checking output..."
echo "=============================================="

# Check output using container's Python (HPC system Python lacks pyarrow)
if [ -f "$OUTPUT_FILE" ]; then
    apptainer exec "$CONTAINER_SIF" python3 -c "
import pandas as pd
df = pd.read_parquet('$OUTPUT_FILE')
print(f'Shape: {df.shape}')
print(f'Columns: {list(df.columns)}')
print()
print('Data:')
print(df.to_string())
"
else
    echo "ERROR: Output file not created: $OUTPUT_FILE"
    exit 1
fi
