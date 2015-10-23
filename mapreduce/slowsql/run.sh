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

echo "Today is $TODAY, and we're gathering slowsql data for $TARGET"
sed -r "s/__TARGET_DATE__/$TARGET/" filter_template.json > filter.json

BASE=$(pwd)
FINAL_DATA_FILE=$BASE/$OUTPUT/slowsql$TARGET.csv
RAW_DATA_FILE=${FINAL_DATA_FILE}.tmp
cd ~/telemetry-server
echo "Starting the slowsql export for $TARGET"
python -u -m mapreduce.hekajob $BASE/slowsql.py \
  --delete-data \
  --num-mappers 16 \
  --num-reducers 4 \
  --input-filter $BASE/filter.json \
  --data-dir $BASE/data \
  --work-dir $BASE/work \
  --output $RAW_DATA_FILE \
  --bucket "net-mozaws-prod-us-west-2-pipeline-data"

echo "Mapreduce job exited with code: $?"

cd -
echo "Looking for 'error' lines:"
grep -e "^Error," $RAW_DATA_FILE
echo "End of error lines."

echo "Adding header line and removing error lines..."
cp csv_header.txt $FINAL_DATA_FILE
grep -ve "^Error," $RAW_DATA_FILE >> $FINAL_DATA_FILE
echo "Removing temp file"
rm $RAW_DATA_FILE
echo "Compressing output"
gzip $FINAL_DATA_FILE
echo "Done!"

echo "Processing weekly data"
cd $BASE
if [ ! -d "weekly" ]; then
    mkdir -p "weekly"
fi
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
        cp $BASE/$OUTPUT/slowsql$DAY.csv.gz ./
    else
        echo "Fetching $DAY"
        aws s3 cp s3://telemetry-public-analysis-2/slowsql/data/slowsql$DAY.csv.gz ./slowsql$DAY.csv.gz
    fi
done
echo "Creating weekly data for $MONDAY to $SUNDAY"
python $BASE/combine.py $BASE/$OUTPUT $MONDAY $SUNDAY
echo "Created weekly output files:"
ls -l $BASE/$OUTPUT/
