#!/bin/bash

cd "$(dirname "$0")"
DIR=$PWD
cd ../../../
mkdir -p /tmp/telemetry/work/cache

python -m mapreduce.hekajob $DIR/distribution.py \
       --input-filter $DIR/filter.json \
       --num-mappers 16 \
       --num-reducers 4 \
       --data-dir /tmp/telemetry/work \
       --work-dir /tmp/telemetry/work \
       --output /tmp/telemetry/my_mapreduce_results.out \
       --bucket "net-mozaws-prod-us-west-2-pipeline-data"
