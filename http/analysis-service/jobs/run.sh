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
EXIT_CODE=$?

if [ $EXIT_CODE -eq 2 ]; then
    # Job timed out.  Notify owner.
    NOTIFY=monitoring/anomaly_detection/notify.py
    FROM="telemetry-alerts@mozilla.com"
    TO=$(/usr/bin/jq -r '.job_owner' < "$JOB_CONFIG")
    JOB_NAME=$(/usr/bin/jq -r '.job_name' < "$JOB_CONFIG")
    SUBJECT="Your scheduled Telemetry job '$JOB_NAME' timed out"
    JOB_TIMEOUT=$(/usr/bin/jq -r '.job_timeout_minutes' < "$JOB_CONFIG")
    if [ -z "$TO" ]; then
        # Send to a default address if the owner name is missing from the config.
        TO=$FROM
        SUBJECT="Scheduled Telemetry job '$JOB_NAME' timed out (and had no owner)"
    fi
    /usr/bin/python $NOTIFY -f "$FROM" -t "$TO" -s "$SUBJECT" <<END
Scheduled Telemetry job "$JOB_NAME" was forcibly terminated after the configured
timeout ($JOB_TIMEOUT minutes).

You can review the job's details at http://telemetry-dash.mozilla.org
END
