#!/bin/bash

cd $(cd -P -- "$(dirname -- "$0")" && pwd -P)
sudo pip install unicodecsv

# Replace the default telemetry-server install with our own
rm -rf telemetry-server
git clone https://github.com/irvingreid/telemetry-server.git
cd telemetry-server/mapreduce/addons

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

echo "Today is $TODAY, and we're gathering am_exceptions data for $TARGET"
sed -r "s/__TARGET_DATE__/$TARGET/" filter_template.json > filter.json

BASE=$(pwd)
FINAL_DATA_FILE=$BASE/$OUTPUT/am_exceptions$TARGET.csv
RAW_DATA_FILE=${FINAL_DATA_FILE}.tmp

cd ../../
echo "Starting the am_exceptions export for $TARGET"
python -u -m mapreduce.job $BASE/am_exceptions.py \
  --num-mappers 16 \
  --num-reducers 4 \
  --input-filter $BASE/filter.json \
  --data-dir $BASE/data \
  --work-dir $BASE/work \
  --output $RAW_DATA_FILE \
  --bucket telemetry-published-v1 #--data-dir $BASE/work/cache --local-only

echo "Mapreduce job exited with code: $?"

echo "Adding header line and compressing"
cat $BASE/csv_header.txt $RAW_DATA_FILE | gzip > $FINAL_DATA_FILE.gz
echo "Removing temp file"
rm $RAW_DATA_FILE
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
        cp $BASE/$OUTPUT/am_exceptions$DAY.csv.gz ./
    else
        echo "Fetching $DAY"
	aws s3 cp s3://telemetry-public-analysis/addons/data/am_exceptions$DAY.csv.gz ./am_exceptions$DAY.csv.gz
    fi
done
echo "Creating weekly data for $MONDAY to $SUNDAY"
python $BASE/combine.py $BASE/$OUTPUT $MONDAY $SUNDAY
echo "Created weekly output files:"
ls -l $BASE/$OUTPUT/
