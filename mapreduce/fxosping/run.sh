#!/bin/bash

BASE=$(pwd)
THIS_DIR=$(cd "`dirname "$0"`"; pwd)
TELEMETRY_SERVER_DIR=$(cd "$THIS_DIR/../.."; pwd)
if [ ! -d "$TELEMETRY_SERVER_DIR/mapreduce" ]; then
    TELEMETRY_SERVER_DIR=$HOME/telemetry-server
fi

OUTPUT=${OUTPUT:-output}
TODAY=$(date +%Y%m%d)

if [ ! -d "$OUTPUT" ]; then
    mkdir -p "$OUTPUT"
fi

if [ ! -d "job" ]; then
    mkdir -p "job"
fi

if [ ! -d "work" ]; then
    mkdir -p "work"
fi

if [ ! -d "data" ]; then
    mkdir -p "data"
fi

# If we have an argument, process that day.
TARGET=$1
if [ -z "$TARGET" ]; then
    # Default to processing "yesterday"
    TARGET=$(date -d 'yesterday' +%Y%m%d)
fi

if [ "$TARGET" = "all" ]; then
    TARGET_DATE="\"*\""
else
    TARGET_DATE="[\"$TARGET\"]"
fi

echo "Today is $TODAY, and we're gathering fxosping data for '$TARGET'"

sed -r "s/__TARGET_DATE__/$TARGET_DATE/" \
       "$THIS_DIR/filter_template.json" > "$THIS_DIR/filter.json"

cd "$TELEMETRY_SERVER_DIR"

OUTPUT_FILE=$BASE/$OUTPUT/fxosping_$TARGET.csv

echo "Starting fxosping export for $TARGET"
python -m mapreduce.job "$THIS_DIR/fxosping.py" \
   --input-filter "$THIS_DIR/filter.json" \
   --num-mappers 16 \
   --num-reducers 4 \
   --data-dir "$BASE/data" \
   --work-dir "$BASE/work" \
   --output "$OUTPUT_FILE" \
   --bucket "telemetry-published-v1"

echo "Mapreduce job exited with code: $?"

cd "$BASE"
echo "Compressing output"
gzip "$OUTPUT/fxosping_$TARGET.csv"

echo "Done!"
