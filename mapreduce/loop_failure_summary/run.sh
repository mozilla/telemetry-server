#!/bin/bash
# Loop Telemetry

OUTPUT=output
TODAY=$(date +%Y%m%d)

JOB_DIR=$(pwd)
BASE=/mnt/telemetry
cd $BASE
if [ ! -d "$OUTPUT" ]; then
    mkdir -p "$OUTPUT"
fi
if [ ! -d "work" ]; then
    mkdir -p "work"
fi

if [ ! -d "data" ]; then
    mkdir -p "data"
fi

cd $JOB_DIR

TARGET=$1
if [ -z "$TARGET" ]; then
  # Default to processing "yesterday"
  TARGET=$(date -d 'yesterday' +%Y%m%d)
fi

echo "Today is $TODAY | Gathering data for $TARGET"
sed -r "s/__TARGET_DATE__/$TARGET/" filter_template.json > filter.json

FINAL_DATA_FILE=$BASE/$OUTPUT/$TARGET.tsv
RAW_DATA_FILE=${FINAL_DATA_FILE}.tmp
cd ~/telemetry-server
echo "Starting the export for data on $TARGET"
echo "running $BASE/failures_by_type.py"
python -u -m mapreduce.job $JOB_DIR/failures_by_type.py \
  --num-mappers 16 \
  --num-reducers 1 \
  --input-filter $JOB_DIR/filter.json \
  --data-dir $BASE/data \
  --work-dir $BASE/work \
  --output $RAW_DATA_FILE \
  --bucket telemetry-published-v2

cat $JOB_DIR/header.txt > $FINAL_DATA_FILE
cat $RAW_DATA_FILE >> $FINAL_DATA_FILE
rm $RAW_DATA_FILE

aws s3 cp s3://telemetry-private-analysis-2/loop_failures/data/failures_by_type.json $JOB_DIR/failures_by_type.json
if [ -f "$JOB_DIR/failures_by_type.json" ]; then
  # back up the existing one
  cp $JOB_DIR/failures_by_type.json $BASE/$OUTPUT/failures_by_type.json.prev
else
  # create an empty one.
  touch $JOB_DIR/failures_by_type.json
fi
python $JOB_DIR/summarize.py -i $FINAL_DATA_FILE -o $BASE/$OUTPUT/$TARGET.summary.json -c $JOB_DIR/failures_by_type.json -O $BASE/$OUTPUT/failures_by_type.json
gzip $FINAL_DATA_FILE
