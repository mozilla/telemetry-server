#!/bin/bash

cd $(cd -P -- "$(dirname -- "$0")" && pwd -P)
sudo apt-get --yes install python-numpy git

rm -rf telemetry-server
git clone https://github.com/mozilla/telemetry-server.git
cd telemetry-server/mapreduce/addon_analysis

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

BASE=$(pwd)
BEGIN=$(date -d "$TODAY - $DAYS days - 4 weeks" +%Y%m%d)
END=$(date -d "TODAY - $DAYS days" +%Y%m%d)
VERSION=$(python $BASE/last_version.py)

echo "Today is $TODAY, and we're gathering data from $BEGIN to $END"
sed -e "s/__BEGIN__/$BEGIN/" -e "s/__END__/$END/" -e "s/__VERSION__/$VERSION/" filter_template.json > filter.json

FINAL_DATA_FILE=$BASE/$OUTPUT/buildid_$BEGIN.csv
RAW_DATA_FILE=${FINAL_DATA_FILE}.tmp

cd ../../
echo "Starting top addons selection"
python -u -m mapreduce.job $BASE/addons.py \
 --num-mappers 16 \
 --num-reducers 4 \
 --input-filter $BASE/filter.json \
 --data-dir $BASE/data \
 --work-dir $BASE/work \
 --output $BASE/$OUTPUT/addons.csv.tmp \
 --bucket telemetry-published-v2  --data-dir $BASE/work/cache --local-only

sort -t"," -k2 -n  -r $BASE/$OUTPUT/addons.csv.tmp | head -n 200 > $BASE/$OUTPUT/addons.csv
rm $BASE/$OUTPUT/addons.csv.tmp
echo startup,$(cat $BASE/$OUTPUT/addons.csv | cut -d ',' -f 1 | paste -sd ",") > $FINAL_DATA_FILE

echo "Starting addons vector transformation"
python -u -m mapreduce.job $BASE/addon_vector.py \
  --num-mappers 16 \
  --num-reducers 4 \
  --input-filter $BASE/filter.json \
  --data-dir $BASE/data \
  --work-dir $BASE/work \
  --output $RAW_DATA_FILE \
  --bucket telemetry-published-v2  --data-dir $BASE/work/cache --local-only

echo "Mapreduce job exited with code: $?"
cat $RAW_DATA_FILE >> $FINAL_DATA_FILE
rm $RAW_DATA_FILE
