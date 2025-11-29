#!/bin/bash
# Test oddpub container with small PMC archive
# Usage: bash test_container.sh [container.sif]

set -e

# Default container path
CONTAINER="${1:-oddpub.sif}"

if [ ! -f "$CONTAINER" ]; then
    echo "Error: Container not found: $CONTAINER"
    echo "Usage: $0 [path/to/oddpub.sif]"
    exit 1
fi

echo "========================================================================"
echo "Testing oddpub container: $CONTAINER"
echo "========================================================================"
echo ""

# Test 1: Check Python version
echo "Test 1: Python version"
apptainer exec "$CONTAINER" python3 --version
echo ""

# Test 2: Check R version
echo "Test 2: R version"
apptainer exec "$CONTAINER" R --version | head -1
echo ""

# Test 3: Check Python packages
echo "Test 3: Python packages"
apptainer exec "$CONTAINER" python3 -c "import pandas; import pyarrow; print('✓ pandas:', pandas.__version__); print('✓ pyarrow:', pyarrow.__version__)"
echo ""

# Test 4: Check R packages
echo "Test 4: R packages"
apptainer exec "$CONTAINER" R --slave -e "library(oddpub); library(dplyr); library(future); cat('✓ oddpub:', as.character(packageVersion('oddpub')), '\n'); cat('✓ dplyr:', as.character(packageVersion('dplyr')), '\n')"
echo ""

# Test 5: Check Rscript is in PATH
echo "Test 5: Rscript location"
apptainer exec "$CONTAINER" which Rscript
echo ""

# Test 6: Check scripts are present
echo "Test 6: Container scripts"
apptainer exec "$CONTAINER" ls -lh /scripts/
echo ""

# Test 7: Check script has correct type hints
echo "Test 7: Python type hints (should show Tuple, not tuple)"
apptainer exec "$CONTAINER" grep "from typing import" /scripts/process_pmcoa_with_oddpub.py
echo ""

# Test 8: Process small tar.gz file (if available)
TEST_TAR="${HOME}/claude/pmcoaXMLs/raw_download/oa_other_xml.incr.2025-07-03.tar.gz"
if [ -f "$TEST_TAR" ]; then
    echo "Test 8: Processing small tar.gz file"
    echo "File: $TEST_TAR"

    # Create temp output directory
    OUTPUT_DIR=$(mktemp -d)
    echo "Output: $OUTPUT_DIR/test_results.parquet"

    # Run processing with limited files
    apptainer exec "$CONTAINER" python3 /scripts/process_pmcoa_with_oddpub.py \
        --batch-size 50 \
        --max-files 10 \
        --output-file "$OUTPUT_DIR/test_results.parquet" \
        "$TEST_TAR" 2>&1 | tail -20

    # Check output
    if [ -f "$OUTPUT_DIR/test_results.parquet" ]; then
        echo "✓ Output file created"
        apptainer exec "$CONTAINER" python3 -c "import pandas as pd; df = pd.read_parquet('$OUTPUT_DIR/test_results.parquet'); print(f'Records: {len(df)}'); print(f'Columns: {list(df.columns)[:5]}...')"
        rm -rf "$OUTPUT_DIR"
    else
        echo "✗ Output file not created"
        rm -rf "$OUTPUT_DIR"
        exit 1
    fi
else
    echo "Test 8: Skipped (test file not found: $TEST_TAR)"
fi

echo ""
echo "========================================================================"
echo "All tests passed!"
echo "========================================================================"
