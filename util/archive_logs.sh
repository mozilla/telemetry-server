#!/bin/bash

CODE_DIR=$1
DATA_DIR=$2
LOG_FILE=/tmp/telemetry_archive.log

USAGE="Usage: $0 code_dir data_dir"
if [ -z "$CODE_DIR" ] || [ ! -f "$CODE_DIR/get_compressibles.py" ]; then
  echo "Invalid code_dir"
  echo $USAGE
  exit 1
fi
if [ -z "$DATA_DIR" ] || [ ! -d "$DATA_DIR" ]; then
  echo "Invalid data_dir"
  echo $USAGE
  exit 2
fi

if [ ! -f "$CODE_DIR/histogram_tools.py" ]; then
  echo "Fetching histogram_tools.py"
  bash ./get_histogram_tools.sh
fi

PYTHON=/usr/bin/python
cd $CODE_DIR
# Note that this assumes a bunch of stuff, including that all the defaults
# for compression will work relative to $CODE_DIR (histogram_cache location,
# server config, etc)
time $PYTHON ./get_compressibles.py $DATA_DIR | $PYTHON ./compressor.py >> $LOG_FILE 2>&1

exit $?
