#!/bin/bash

# If we have a target argument, process that day.
TARGET=$1
if [ -z "$TARGET" ]; then
  # Default to processing "yesterday"
  TARGET=$(date -d 'yesterday' +%Y%m%d)
fi
NAME=$2
if [ -z "$NAME" ]; then
  NAME=chromehangs_weekly
fi

OUTPUT=$3
if [ -z "$OUTPUT" ]; then
  OUTPUT=output
fi

BASE=$(pwd)
DATA_FILE=$BASE/$OUTPUT/chromehangs-common-$TARGET.csv.gz

echo "Processing weekly data"
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
    if [ "$DAY" -eq "$TARGET" -a -f "$DATA_FILE" ]; then
        echo "Using target local file for today ($DAY)"
        cp ${DATA_FILE} ./
    elif [ -f "$BASE/chromehangs-common-$DAY.csv.gz" ]; then
        echo "Already have local file for $DAY"
        cp "$BASE/chromehangs-common-$DAY.csv.gz" ./
    else
        echo "Fetching $DAY"
        aws s3 cp s3://telemetry-public-analysis/$NAME/data/chromehangs-common-$DAY.csv.gz ./
    fi
done
echo "Creating weekly data for $MONDAY to $SUNDAY"
python $BASE/combine.py $BASE/$OUTPUT $MONDAY $SUNDAY
echo "Done!"
