#!/bin/bash

CODE_DIR=$1
DATA_DIR=$2
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

PYTHON=/usr/bin/python
cd $CODE_DIR
# Note that this assumes a bunch of stuff, including that all the defaults
# for compression will work relative to $CODE_DIR (histogram_cache location,
# server config, etc)
$PYTHON ./get_compressibles.py $DATA_DIR | $PYTHON ./compressor.py
