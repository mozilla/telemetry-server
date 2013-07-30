#!/bin/bash

if [ -z "$1" ]; then
    echo "This script is used to install (or update) the files required to run"
    echo "a telemetry server to a given location"
    echo "Usage: $0 deploy_dir"
    exit 2
fi

if [ ! -d "$1" ]; then
    echo "Error: '$1' is not a directory"
    exit 3
fi

# script lives in the 'util' subdir
SCRIPT_DIR=$(dirname $0)
cd "$SCRIPT_DIR/.."

if [ ! -f "convert.py" ]; then
    echo "Error: Can't find server script $SCRIPT_DIR/convert.py"
    exit 4
fi

if [ ! -f "histogram_tools.py" ]; then
    echo "Getting histogram_tools.py..."
    bash get_histogram_tools.sh
fi

rsync -av \
    compressor.py \
    convert.py \
    get_compressables.py \
    histogram_tools.py \
    persist.py \
    revision_cache.py \
    server.py \
    telemetry_schema.py \
    telemetry_schema.json \
    "$1"

if [ ! -f "$1/telemetry_server_config.json" ]; then
    echo "There is no telemetry server config. You should install one."
    echo "You can modify the example config if you like:"
    echo "  cp telemetry_server_config.json $1"
    echo "Then edit it to suit your needs"
fi
