#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
JOB_CONFIG="$DIR/$1.json"

REGION=$(jq -r '.region' < "$JOB_CONFIG")
JOB_NAME=$(jq -r '.job_name' < "$JOB_CONFIG")

export PATH=/usr/bin/:/usr/local/bin/:$PATH

if [ ! -f "$JOB_CONFIG" ]; then
    echo "ERROR: missing config file for job $1"
    exit 1
fi

if [ "$(jq -r '.num_workers|type' < $JOB_CONFIG)" == "number" ]; then # Spark cluster
    EMR_RELEASE=$(jq -r '.emr_release' < "$JOB_CONFIG")
    N_WORKERS=$(jq -r '.num_workers' < "$JOB_CONFIG")
    MASTER_TYPE=$(jq -r '.master_instance_type' < "$JOB_CONFIG")
    SLAVE_TYPE=$(jq -r '.slave_instance_type' < "$JOB_CONFIG")
    CLUSTER_NAME=$(jq -r '.cluster_name' < "$JOB_CONFIG")
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
        --applications Name=Spark Name=Hive \
        --bootstrap-actions Path=s3://${EMR_BUCKET}/bootstrap/telemetry.sh,Args=\["--timeout","$TIMEOUT"\] \
        --configurations https://s3-${REGION}.amazonaws.com/${EMR_BUCKET}/configuration/configuration.json \
        --steps Type=CUSTOM_JAR,Name=CustomJAR,ActionOnFailure=TERMINATE_JOB_FLOW,Jar=s3://${REGION}.elasticmapreduce/libs/script-runner/script-runner.jar,Args="$STEP_ARGS"
    EXIT_CODE=$?
    if [ $EXIT_CODE -ne 0 ]; then
        # Error creating emr cluster. Notify owner.
        NOTIFY_SUBJECT="Scheduled Spark job '$JOB_NAME' encountered an error"
        NOTIFY_BODY=<<END
Scheduled Telemetry Spark job '$JOB_NAME' exited with a code of $EXIT_CODE which
indicates it probably encountered an error.
END
    fi
else
    cd ~/telemetry-server
    python -m provisioning.aws.launch_worker "$JOB_CONFIG"
    EXIT_CODE=$?

    if [ $EXIT_CODE -eq 2 ]; then
        # Job timed out.  Notify owner.
        JOB_TIMEOUT=$(jq -r '.job_timeout_minutes' < "$JOB_CONFIG")
        NOTIFY_SUBJECT="Scheduled Telemetry job '$JOB_NAME' timed out"
        NOTIFY_BODY=<<END
Scheduled Telemetry job '$JOB_NAME' was forcibly terminated after the configured
timeout ($JOB_TIMEOUT minutes).
END
    elif [ $EXIT_CODE -ne 0 ]; then
        # Error running job. Notify owner.
        NOTIFY_SUBJECT="Scheduled Telemetry job '$JOB_NAME' encountered an error"
        NOTIFY_BODY=<<END
Scheduled Telemetry job '$JOB_NAME' exited with code $EXIT_CODE which indicates
it probably encountered an error.
END
    fi
fi

if [ ! -z "$NOTIFY_SUBJECT" ]; then
    FROM="telemetry-alerts@mozilla.com"
    TO=$(jq -r '.job_owner' < "$JOB_CONFIG")
    if [ -z "$TO" ]; then
        # Send to a default address if the owner name is missing from the config.
        TO=$FROM
        NOTIFY_SUBJECT="$NOTIFY_SUBJECT (and had no owner)"
    fi
    NOTIFY_BODY=<<END
$NOTIFY_BODY

You can review the job's details at http://analysis.telemetry.mozilla.org
END
    aws ses send-email \
        --region $REGION \
        --from "$FROM" \
        --to "$TO" \
        --subject "$NOTIFY_SUBJECT" \
        --text "$NOTIFY_BODY"
fi
