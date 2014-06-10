#!/bin/bash

# Install additional python modules used by addons analysis
sudo pip install unicodecsv

# Replace the default telemetry-server install with our own
rm -rf telemetry-server
git clone https://github.com/irvingreid/telemetry-server.git
cd telemetry-server
git checkout addon-nightly

# Now run the actually processing job, using the code from Irving's github
mapreduce/addons/processExceptions.sh
