#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
JOB_CONFIG="$DIR/$1.json"

export PATH=/usr/bin/:/usr/local/bin/:$PATH

if [ ! -f "$JOB_CONFIG" ]; then
    echo "ERROR: missing config file for job $1"
    exit 1
fi

if [ $(jq 'has("num_workers")' < $JOB_CONFIG) = true ]; then # Spark cluster
    AMI_VERSION=$(jq -r '.ami_version' < "$JOB_CONFIG")
    SPARK_VERSION=$(jq -r '.spark_version' < "$JOB_CONFIG")
    N_WORKERS=$(jq -r '."num_workers"' < "$JOB_CONFIG")
    MASTER_TYPE=$(jq -r '.master_instance_type' < "$JOB_CONFIG")
    SLAVE_TYPE=$(jq -r '.slave_instance_type' < "$JOB_CONFIG")
    CLUSTER_NAME=$(jq -r '.cluster_name' < "$JOB_CONFIG")
    JOB_NAME=$(jq -r '.job_name' < "$JOB_CONFIG")
    NOTEBOOK=$(jq -r '.code_uri' < "$JOB_CONFIG")
    TIMEOUT=$(jq -r '.timeout_minutes' < "$JOB_CONFIG")
    DATA_BUCKET=$(jq -r '.data_bucket' < "$JOB_CONFIG")
    SSH_KEY=$(jq -r '.ssl_key_name' < "$JOB_CONFIG")
    OWNER=$(jq -r '.owner' < "$JOB_CONFIG")

    aws emr create-cluster --auto-terminate --name $CLUSTER_NAME --ami-version $AMI_VERSION --instance-type $SLAVE_TYPE --instance-count $N_WORKERS --service-role EMR_DefaultRole --ec2-attributes KeyName=$SSH_KEY,InstanceProfile=telemetry-spark-emr --tags "Owner=$OWNER Application=telemetry-analysis-worker-instance" --bootstrap-actions Path=s3://support.elasticmapreduce/spark/install-spark,Args=\["-v","$SPARK_VERSION"\] Path=s3://telemetry-spark-emr/telemetry.sh,Args=\["--timeout","$TIMEOUT"\] --steps Type=CUSTOM_JAR,Name=CustomJAR,ActionOnFailure=TERMINATE_JOB_FLOW,Jar=s3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar,Args=\["s3://telemetry-spark-emr/batch.sh","--job-name","$JOB_NAME","--notebook","$NOTEBOOK","--data-bucket","$DATA_BUCKET"\]
else
    cd "$DIR/../../../"
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

You can review the job's details at http://telemetry-dash.mozilla.org
END
    fi
fi
