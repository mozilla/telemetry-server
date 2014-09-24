#!/bin/bash

cd $(cd -P -- "$(dirname -- "$0")" && pwd -P)
sudo apt-get --yes install python-numpy git

rm -rf telemetry-server
git clone https://github.com/mozilla/telemetry-server.git
cd telemetry-server/mapreduce/mainthreadio

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

# If we have an argument, process that week.
DAYS=$1
if [ -z "$DAYS" ]; then
  # Default to processing "last week"
  DAYS=0
fi

BEGIN=$(date -d "$TODAY - $DAYS days - 1 weeks" +%Y%m%d)
END=$(date -d "TODAY - $DAYS days" +%Y%m%d)
BID_BEGIN=$BEGIN
BID_END=$BEGIN
TARGET=$BID_BEGIN

echo "Today is $TODAY, and we're gathering mainthreadio data from $BEGIN to $END for build-ids from $BID_BEGIN to $BID_END"
sed -e "s/__BEGIN__/$BEGIN/" -e "s/__END__/$END/" -e "s/__BID_BEGIN__/$BID_BEGIN/" -e "s/__BID_END__/$BID_END/" filter_template.json > filter.json

BASE=$(pwd)
FINAL_DATA_FILE=$BASE/$OUTPUT/buildid_$TARGET.csv
RAW_DATA_FILE=${FINAL_DATA_FILE}.tmp

cd ../../
echo "Starting the mainthreadio export for $TARGET"
python -u -m mapreduce.job $BASE/mainthreadio.py \
  --num-mappers 16 \
  --num-reducers 4 \
  --input-filter $BASE/filter.json \
  --data-dir $BASE/data \
  --work-dir $BASE/work \
  --output $RAW_DATA_FILE \
  --bucket telemetry-published-v2 #--data-dir $BASE/work/cache --local-only

echo "Mapreduce job exited with code: $?"

echo "Adding header line"
cp $BASE/csv_header.txt $FINAL_DATA_FILE

echo "Compute summaries"
python $BASE/summary.py $RAW_DATA_FILE

echo "Copying iacomus configuration"
cp $BASE/iacomus.json $BASE/$OUTPUT

cat $RAW_DATA_FILE >> $FINAL_DATA_FILE
echo "Removing temp file"
rm $RAW_DATA_FILE
echo "Compressing output"
gzip $FINAL_DATA_FILE
echo "Done!"
