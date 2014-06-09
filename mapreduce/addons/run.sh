#!/bin/bash

# Install additional python modules used by addons analysis
sudo pip install unicodecsv

# Replace the default telemetry-server install with our own
rm -rf telemetry-server
git clone https://github.com/irvingreid/telemetry-server.git

# Now run the actually processing job, using the code from Irving's github
telemetry-server/mapreduce/addons/processExceptions.sh
