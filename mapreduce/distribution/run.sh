#!/bin/bash

cd $(cd -P -- "$(dirname -- "$0")" && pwd -P)
sudo add-apt-repository --yes ppa:marutter/rrutter
sudo apt-get update
sudo apt-get --yes install python-scipy python-numpy

#rm -rf telemetry-server
#git clone https://github.com/mozilla/telemetry-server.git
#cd telemetry-server/mapreduce/distribution

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
sed -e "s/__BEGIN__/$BEGIN/" -e "s/__END__/$END/" filter_template.json > filter.json

FINAL_DISTRIBUTION_FILE=$BASE/$OUTPUT/distribution_$BEGIN.csv
FINAL_DISTRIBUTION_FILE_LAST=$BASE/$OUTPUT/distribution.csv
RAW_DISTRIBUTION_FILE=${FINAL_DISTRIBUTION_FILE}.tmp

cd ../../
echo "Running distribution job"
python -u -m mapreduce.job $BASE/distribution.py \
 --num-mappers 16 \
 --num-reducers 4 \
 --input-filter $BASE/filter.json \
 --data-dir $BASE/data \
 --work-dir $BASE/work \
 --output $RAW_DISTRIBUTION_FILE \
 --bucket telemetry-published-v2 # --data-dir $BASE/work/cache --local-only

python $BASE/filter.py $RAW_DISTRIBUTION_FILE > $FINAL_DISTRIBUTION_FILE
cp $FINAL_DISTRIBUTION_FILE $FINAL_DISTRIBUTION_FILE_LAST
rm $RAW_DISTRIBUTION_FILE
