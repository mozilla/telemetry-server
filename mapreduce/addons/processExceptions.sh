#!/bin/bash

BASE=$(pwd)
echo "Working in directory $BASE"

WORK="$BASE/work"
OUTPUT="$BASE/output"
TODAY=$(date +%Y%m%d)
mkdir -p "$OUTPUT"
mkdir -p "$WORK/cache"

# If we have an argument, process that day.
TARGET=$1
if [ -z "$TARGET" ]; then
  # Default to processing "yesterday"
  TARGET=$(date -d 'yesterday' +%Y%m%d)
fi

cd telemetry-server
JOB="mapreduce/addons"

FILTER="$WORK/filter.json"
echo "Today is $TODAY, and we're gathering am_exceptions data for $TARGET"
sed -r "s/__TARGET_DATE__/$TARGET/" $JOB/filter_template.json > $FILTER

DATA_FILE="$OUTPUT/am_exceptions${TARGET}.csv"

echo "Starting the am_exceptions export for $TARGET"
python -u -m mapreduce.job $JOB/am_exceptions.py \
  --num-mappers 8 \
  --num-reducers 8 \
  --input-filter $FILTER \
  --data-dir "$WORK/cache" \
  --work-dir $WORK \
  --output $DATA_FILE \
  --bucket telemetry-published-v1

echo "Mapreduce job exited with code: $?"

echo "compressing"
gzip $DATA_FILE
echo "Done!"

echo "Processing weekly data"
cd $BASE
mkdir -p "weekly"
cd weekly

# Monday is day 1
OFFSET=$(( $(date -d $TARGET +%u) - 1 ))
MONDAY=$(date -d "$TARGET - $OFFSET days" +%Y%m%d)
SUNDAY=$(date -d "$MONDAY + 6 days" +%Y%m%d)
echo "For target '$TARGET', week is $MONDAY to $SUNDAY"
for f in $(seq 0 6); do
    DAY=$(date -d "$MONDAY + $f days" +%Y%m%d)
    if [ "$DAY" -eq "$TARGET" ]; then
        echo "Using local file for today ($DAY)"
        cp ${DATA_FILE}.gz .
    else
        echo "Fetching $DAY"
	aws s3 cp s3://telemetry-public-analysis/addons/data/am_exceptions$DAY.csv.gz ./am_exceptions$DAY.csv.gz
    fi
done
echo "Creating weekly data for $MONDAY to $SUNDAY"
python $BASE/telemetry-server/$JOB/combine.py "$OUTPUT" "$MONDAY" *
echo "Created weekly output file:"
ls -l $OUTPUT/
