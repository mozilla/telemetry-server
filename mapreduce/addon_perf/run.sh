#!/bin/bash

# Install additional python modules used by addon_perf analysis
sudo pip install unicodecsv

# Now run the actually processing job
telemetry-server/mapreduce/addon_perf/processAddonPerf.sh
