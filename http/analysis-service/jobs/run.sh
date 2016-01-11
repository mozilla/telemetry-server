#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
JOB_CONFIG="$DIR/$1.json"

export PATH=/usr/bin/:/usr/local/bin/:$PATH

if [ ! -f "$JOB_CONFIG" ]; then
    echo "ERROR: missing config file for job $1"
    exit 1
fi

if [ "$(jq -r '.num_workers|type' < $JOB_CONFIG)" == "number" ]; then # Spark cluster
    REGION=$(jq -r '.region' < "$JOB_CONFIG")
    EMR_RELEASE=$(jq -r '.emr_release' < "$JOB_CONFIG")
    N_WORKERS=$(jq -r '.num_workers' < "$JOB_CONFIG")
    MASTER_TYPE=$(jq -r '.master_instance_type' < "$JOB_CONFIG")
    SLAVE_TYPE=$(jq -r '.slave_instance_type' < "$JOB_CONFIG")
    CLUSTER_NAME=$(jq -r '.cluster_name' < "$JOB_CONFIG")
    JOB_NAME=$(jq -r '.job_name' < "$JOB_CONFIG")
    CODE=$(jq -r '.code_uri' < "$JOB_CONFIG")
    TIMEOUT=$(jq -r '.timeout_minutes' < "$JOB_CONFIG")
    DATA_BUCKET=$(jq -r '.data_bucket' < "$JOB_CONFIG")
    SSH_KEY=$(jq -r '.ssl_key_name' < "$JOB_CONFIG")
    OWNER=$(jq -r '.owner' < "$JOB_CONFIG")
    APP_TAG=$(jq -r '.application_tag' < "$JOB_CONFIG")
    INSTANCE_PROFILE=$(jq -r '.spark_instance_profile' < "$JOB_CONFIG")
    EMR_BUCKET=$(jq -r '.spark_emr_bucket' < "$JOB_CONFIG")
    SUBMIT_ARGS=$(jq -r '.commandline' < "$JOB_CONFIG")

    if [ "${CODE##*.}" == "jar" ]; then
        STEP_ARGS=\["s3://${EMR_BUCKET}/steps/batch.sh","--job-name","$JOB_NAME","--jar","$CODE","--spark-submit-args","$SUBMIT_ARGS","--data-bucket","$DATA_BUCKET"\]
    else
        STEP_ARGS=\["s3://${EMR_BUCKET}/steps/batch.sh","--job-name","$JOB_NAME","--notebook","$CODE","--data-bucket","$DATA_BUCKET"\]
    fi

    aws emr create-cluster \
        --auto-terminate \
        --region $REGION \
        --name "$CLUSTER_NAME" \
        --release-label $EMR_RELEASE \
        --instance-type $SLAVE_TYPE \
        --instance-count $N_WORKERS \
        --service-role EMR_DefaultRole \
        --ec2-attributes KeyName=$SSH_KEY,InstanceProfile=$INSTANCE_PROFILE \
        --tags "Owner=$OWNER Application=$APP_TAG" \
        --applications Name=Spark \
        --bootstrap-actions Path=s3://${EMR_BUCKET}/bootstrap/telemetry.sh,Args=\["--timeout","$TIMEOUT"\] \
        --configurations https://s3-${REGION}.amazonaws.com/${EMR_BUCKET}/configuration/configuration.json \
        --steps Type=CUSTOM_JAR,Name=CustomJAR,ActionOnFailure=TERMINATE_JOB_FLOW,Jar=s3://${REGION}.elasticmapreduce/libs/script-runner/script-runner.jar,Args="$STEP_ARGS"
else
    cd ~/telemetry-server
    python -m provisioning.aws.launch_worker "$JOB_CONFIG"
    EXIT_CODE=$?

    if [ $EXIT_CODE -eq 2 ]; then
        # Job timed out.  Notify owner.
        NOTIFY=monitoring/anomaly_detection/notify.py
        FROM="telemetry-alerts@mozilla.com"
        TO=$(jq -r '.job_owner' < "$JOB_CONFIG")
        JOB_NAME=$(jq -r '.job_name' < "$JOB_CONFIG")
        SUBJECT="Your scheduled Telemetry job '$JOB_NAME' timed out"
        JOB_TIMEOUT=$(jq -r '.job_timeout_minutes' < "$JOB_CONFIG")
        if [ -z "$TO" ]; then
            # Send to a default address if the owner name is missing from the config.
            TO=$FROM
            SUBJECT="Scheduled Telemetry job '$JOB_NAME' timed out (and had no owner)"
        fi
        python $NOTIFY -f "$FROM" -t "$TO" -s "$SUBJECT" <<END
Scheduled Telemetry job "$JOB_NAME" was forcibly terminated after the configured
timeout ($JOB_TIMEOUT minutes).

You can review the job's details at http://analysis.telemetry.mozilla.org
END
    fi
fi
