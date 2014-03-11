#!/bin/bash

JOB_CONFIG="/home/ubuntu/telemetry_analysis/jobs/$1.json"
if [ ! -f "$JOB_CONFIG" ]; then
    echo "ERROR: missing config file for job $1"
    exit 1
elif [ ! -d "/home/ubuntu/telemetry-server" ]; then
    echo "ERROR: missing telemetry-server code at /home/ubuntu/telemetry-server"
    exit 2
fi
cd /home/ubuntu/telemetry-server
/usr/bin/python -m provisioning.aws.launch_worker "$JOB_CONFIG"
