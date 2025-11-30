#!/bin/bash
# Run optimization test inside the oddpub container

set -e

# Check arguments
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <tar.gz file>"
    echo "Example: $0 /data/NIMH_scratch/licc/pmcoa/files/oa_comm_xml.PMC005xxxxxx.baseline.2025-06-26.tar.gz"
    exit 1
fi

TAR_FILE="$1"
CONTAINER="/data/adamt/containers/oddpub.sif"

# Check if container exists
if [ ! -f "$CONTAINER" ]; then
    echo "ERROR: Container not found: $CONTAINER"
    echo "Please build the container first or adjust the path"
    exit 1
fi

# Check if tar file exists
if [ ! -f "$TAR_FILE" ]; then
    echo "ERROR: Tar file not found: $TAR_FILE"
    exit 1
fi

# Load apptainer module
echo "Loading apptainer module..."
module load apptainer

# Source bind paths
. /usr/local/current/apptainer/app_conf/sing_binds

# Get the directory containing this script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "Running optimization test in container..."
echo "Container: $CONTAINER"
echo "Test file: $TAR_FILE"
echo

# Run the test inside the container
# Mount the scripts directory and data directory
apptainer exec \
    --bind "$SCRIPT_DIR:/scripts_host" \
    --bind "/data/NIMH_scratch/licc/pmcoa/files:/data/NIMH_scratch/licc/pmcoa/files" \
    "$CONTAINER" \
    bash -c "
        cd /scripts_host
        python3 test_optimization_container.py '$TAR_FILE'
    "

echo
echo "Test complete!"