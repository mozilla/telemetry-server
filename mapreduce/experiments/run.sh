#!/bin/bash

OUTPUT=output
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

echo "Today is $TODAY, and we're gathering experiment data for $TARGET"
sed -r "s/__TARGET_DATE__/$TARGET/" filter_template.json > filter.json

BASE=$(pwd)
FINAL_DATA_FILE=$BASE/$OUTPUT/experiments$TARGET
RAW_DATA_FILE=$BASE/data.csv
cd ~/telemetry-server
echo "Starting the experiment export for $TARGET"
python -u -m mapreduce.job $BASE/experiments.py \
  --num-mappers 16 \
  --num-reducers 4 \
  --input-filter $BASE/filter.json \
  --data-dir $BASE/data \
  --work-dir $BASE/work \
  --output $RAW_DATA_FILE \
  --bucket telemetry-published-v1

echo "Mapreduce job exited with code: $?"

cd -

grep -e "^Error," $RAW_DATA_FILE
echo "End of error lines."

echo "Adding header line and removing error lines..."
python experiments-process.py $RAW_DATA_FILE $FINAL_DATA_FILE
echo "Removing temp file"
rm $RAW_DATA_FILE
echo "Compressing output"
gzip $FINAL_DATA_FILE
echo "Done!"

ls -l $BASE/$OUTPUT/
