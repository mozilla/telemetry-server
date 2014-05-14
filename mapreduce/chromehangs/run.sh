#!/bin/bash

OUTPUT=output
NAME=chromehangs
TODAY=$(date +%Y%m%d)
if [ ! -d "$OUTPUT" ]; then
    mkdir -p "$OUTPUT"
fi

if [ ! -d "temp" ]; then
    mkdir -p "temp"
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

echo "Today is $TODAY, and we're gathering $NAME data for $TARGET"
sed -r "s/__TARGET_DATE__/$TARGET/" filter_template.json > filter.json

BASE=$(pwd)
RAW_DATA_FILE=$BASE/$OUTPUT/chromehangs-raw-$TARGET.txt
FINAL_DATA_FILE=$BASE/$OUTPUT/chromehangs-$TARGET.txt.gz

cd ~/telemetry-server
echo "Starting the $NAME export for $TARGET"
python -u -m mapreduce.job $BASE/chromehangs.py \
  --num-mappers 8 \
  --input-filter $BASE/filter.json \
  --data-dir $BASE/data \
  --work-dir $BASE/work \
  --output $RAW_DATA_FILE \
  --bucket telemetry-published-v1

echo "Mapreduce job exited with code: $?"

cd -
echo "Looking for 'error' lines:"
grep -e "^Error," $RAW_DATA_FILE
echo "End of error lines."

echo "Symbolicating outputs..."
time python symbolicate.py -i $RAW_DATA_FILE -o $FINAL_DATA_FILE -d $TARGET &> symbolicate.out
SYMBOLICATE_CODE=$?

if [ $SYMBOLICATE_CODE -eq 0 ]; then
    echo "Symbolication succeeded (exited with code $SYMBOLICATE_CODE)"
else
    echo "Symbolication failed (exited with code $SYMBOLICATE_CODE). Log:"
    cat symbolicate.out
fi

echo "Compressing raw output..."
gzip $RAW_DATA_FILE

echo "Done!"
