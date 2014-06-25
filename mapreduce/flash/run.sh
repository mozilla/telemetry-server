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

echo "Today is $TODAY, and we're gathering flash versions for $TARGET"
sed -r "s/__TARGET_DATE__/$TARGET/" filter_template.json > filter_flash.json

BASE=$(pwd)
cd ~/telemetry-server
echo "Starting the flash versions export for $TARGET"
python -u -m mapreduce.job $BASE/flash_versions.py \
  --num-mappers 16 \
  --input-filter $BASE/filter_flash.json \
  --data-dir $BASE/data \
  --work-dir $BASE/work \
  --output $BASE/$OUTPUT/flash_versions$TARGET.csv.tmp \
  --bucket telemetry-published-v2

echo "Mapreduce job exited with code: $?"

cd -
echo "Looking for 'error' lines:"
grep -e "^Error," $OUTPUT/flash_versions$TARGET.csv.tmp
echo "End of error lines."

echo "Adding header line and removing error lines..."
cp csv_header.txt $OUTPUT/flash_versions$TARGET.csv
grep -ve "^Error," $OUTPUT/flash_versions$TARGET.csv.tmp >> $OUTPUT/flash_versions$TARGET.csv
echo "Removing temp file"
rm $OUTPUT/flash_versions$TARGET.csv.tmp
echo "Compressing output"
gzip $OUTPUT/flash_versions$TARGET.csv
echo "Done!"
