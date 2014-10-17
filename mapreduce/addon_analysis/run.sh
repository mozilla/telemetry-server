#!/bin/bash

cd $(cd -P -- "$(dirname -- "$0")" && pwd -P)
sudo add-apt-repository --yes ppa:marutter/rrutter
sudo apt-get update
sudo apt-get --yes install python-scipy python-numpy git r-base r-base-dev pcregrep

#rm -rf telemetry-server
#git clone https://github.com/mozilla/telemetry-server.git
#cd telemetry-server/mapreduce/addon_analysis

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

DAYS=$1
if [ -z "$DAYS" ]; then
  DAYS=1
fi

BASE=$(pwd)
BEGIN=$(date -d "$TODAY - $DAYS days" +%Y%m%d)
END=$(date -d "TODAY - $DAYS days" +%Y%m%d)
VERSION=$(python last_version.py $BEGIN)

echo "Today is $TODAY, and we're gathering data from $BEGIN to $END"
sed -e "s/__BEGIN__/$BEGIN/" -e "s/__END__/$END/" -e "s/__VERSIONBEGIN__/$(($VERSION - 1))/" \
    -e "s/__VERSIONEND__/$VERSION/" filter_template.json > filter.json

FINAL_DATA_FILE=$BASE/$OUTPUT/addons_startup_$BEGIN.csv
RAW_DATA_FILE=${FINAL_DATA_FILE}.tmp
FINAL_ADDON_FILE=$BASE/$OUTPUT/addons_$BEGIN.csv
RAW_ADDON_FILE=${FINAL_ADDON_FILE}.tmp
SUMMARY_FILE=$BASE/$OUTPUT/addon_summary_$BEGIN

cd ../../
echo "Selecting top addons"
python -u -m mapreduce.job $BASE/addons.py \
 --num-mappers 16 \
 --num-reducers 4 \
 --input-filter $BASE/filter.json \
 --data-dir $BASE/data \
 --work-dir $BASE/work \
 --output $RAW_ADDON_FILE \
 --bucket telemetry-published-v2  # --data-dir $BASE/work/cache --local-only

sort -t"," -k2 -n -r $RAW_ADDON_FILE | head -n 500 > $FINAL_ADDON_FILE && rm $RAW_ADDON_FILE
echo startup,shutdown,cpucount,memsize,$(cat $FINAL_ADDON_FILE | cut -d ',' -f 1 | paste -sd ",") > $FINAL_DATA_FILE

echo "Starting addons vector transformation"
FINAL_ADDON_FILE=$FINAL_ADDON_FILE python -u -m mapreduce.job $BASE/addon_vector.py \
  --num-mappers 16 \
  --num-reducers 4 \
  --input-filter $BASE/filter.json \
  --data-dir $BASE/data \
  --work-dir $BASE/work \
  --output $RAW_DATA_FILE \
  --bucket telemetry-published-v2  --data-dir $BASE/work/cache --local-only

echo "Mapreduce job exited with code: $?"

cat $RAW_DATA_FILE >> $FINAL_DATA_FILE && rm $RAW_DATA_FILE
sudo Rscript $BASE/model.R $FINAL_DATA_FILE $SUMMARY_FILE
gzip $BASE/$OUTPUT/{startup*,shutdown*}.csv
cp $BASE/iacomus/{startup,shutdown}.json $BASE/$OUTPUT

rm $FINAL_DATA_FILE
rm $FINAL_ADDON_FILE
