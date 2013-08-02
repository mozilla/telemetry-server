#!/bin/bash

DATA_DIR=$1
USAGE="Usage: $0 data_dir"

if [ -z "$DATA_DIR" ]; then
    echo $USAGE
    exit 1
fi

if [ ! -d "$DATA_DIR" ]; then
    echo "Error: $DATA_DIR is not a valid directory"
    echo $USAGE
    exit 2
fi

BATCH_SIZE=8
FILES=()
CURRENT_COUNT=0

cd $DATA_DIR
for f in $(find . -name "*.lzma" -size +50c); do
    CURRENT_COUNT=$(( $CURRENT_COUNT + 1 ))
    FILES[$CURRENT_COUNT]=$f
    if [ $CURRENT_COUNT -ge $BATCH_SIZE ]; then
        echo "Sending current batch: ${FILES[*]}"
        # if successful, truncate each file.
        FILES=()
    fi
done

if [ $CURRENT_COUNT -gt 0 ]; then
    echo "Sending final batch of $CURRENT_COUNT: ${FILES[*]}"
    # if successful, truncate each file.
fi
