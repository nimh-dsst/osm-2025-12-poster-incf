#!/bin/bash
# Build oddpub Apptainer container
# Requires root/sudo access

set -e

CONTAINER_NAME="oddpub.sif"
DEF_FILE="oddpub.def"

if [ ! -f "$DEF_FILE" ]; then
    echo "Error: Definition file not found: $DEF_FILE"
    echo "Run this script from the container/ directory"
    exit 1
fi

if [ "$EUID" -ne 0 ]; then
    echo "This script requires root/sudo access"
    echo "Please run: sudo $0"
    exit 1
fi

echo "Building Apptainer container: $CONTAINER_NAME"
echo "This will take 10-20 minutes..."
echo ""

# Check if apptainer is installed
if ! command -v apptainer &> /dev/null; then
    echo "Error: apptainer not found"
    echo "Install with: sudo apt-get install -y apptainer"
    exit 1
fi

# Set cache directory to avoid filling up /tmp
export APPTAINER_CACHEDIR="${APPTAINER_CACHEDIR:-./apptainer_cache}"
mkdir -p "$APPTAINER_CACHEDIR"

# Build container
apptainer build "$CONTAINER_NAME" "$DEF_FILE"

echo ""
echo "Build complete!"
echo "Container size:"
ls -lh "$CONTAINER_NAME"
echo ""
echo "Test container:"
echo "  apptainer exec $CONTAINER_NAME python3 --version"
echo "  apptainer exec $CONTAINER_NAME R --version"
echo ""
echo "Transfer to HPC:"
echo "  scp $CONTAINER_NAME user@biowulf.nih.gov:/data/\$USER/containers/"
