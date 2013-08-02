#!/bin/bash

DATA_DIR=$1
USAGE="Usage: $0 data_dir aws_key aws_secret_key [ s3_bucket_name ]"
S3F=/usr/local/bin/s3funnel
VERBOSE=" "
MIN_SIZE="50c"
AWS_KEY=$2
AWS_SECRET_KEY=$3

if [ -z "$DEBUG" ]; then
    DEBUG=0
fi

if [ ! -f "$S3F" ] && [ $DEBUG -eq 0 ]; then
    echo "ERROR: s3funnel not found at '$S3F'"
    echo "You can get it from github: https://github.com/sstoiana/s3funnel"
    exit -1
fi

BUCKET=$4
if [ -z "$BUCKET" ]; then
    BUCKET="mreid-telemetry-dev"
fi

if [ -z "$DATA_DIR" ]; then
    echo $USAGE
    exit 1
fi

if [ ! -d "$DATA_DIR" ]; then
    echo "ERROR: $DATA_DIR is not a valid directory"
    echo $USAGE
    exit 2
fi

if [ -z "$AWS_KEY" ] || [ -z "$AWS_SECRET_KEY" ]; then
    echo "ERROR: you must specify aws_key and aws_secret key"
    echo "$USAGE"
    exit 3
fi

BATCH_SIZE=8
FILES=()
CURRENT_COUNT=0

upload () {
    MY_FILES=$@
    echo $1
    if [ $DEBUG -ne 0 ]; then
        echo "$S3F $BUCKET put -a \"$AWS_KEY\" -s \"$AWS_SECRET_KEY\" -t $BATCH_SIZE $VERBOSE --put-only-new --del-prefix=\"./\" --put-full-path ${MY_FILES[*]}"
        S3F_RETURN=$?
    else
        $S3F $BUCKET put -a "$AWS_KEY" -s "$AWS_SECRET_KEY" -t $BATCH_SIZE $VERBOSE --put-only-new --del-prefix="./" --put-full-path ${MY_FILES[*]}
        S3F_RETURN=$?
    fi
    if [ $S3F_RETURN -eq 0 ]; then
        # Success! truncate each file.
        for uploaded in $MY_FILES; do
            echo "Successfully uploaded $uploaded, time to truncate."
            mv "$uploaded" "${uploaded}.uploaded"
            > "$uploaded"
        done
    else
        # Error :(
        echo "Failed to upload one or more files in the current batch. Error code was $S3F_RETURN."
        # Note that since we don't truncate, we'll try them again next time.
        # The '--put-only-new' flag should save some duplicated uploads.
    fi
}

cd $DATA_DIR
for f in $(find . -name "*.lzma" -size +${MIN_SIZE} | head -n 100); do
    CURRENT_COUNT=$(( $CURRENT_COUNT + 1 ))
    FILES[$CURRENT_COUNT]=$f
    if [ $CURRENT_COUNT -ge $BATCH_SIZE ]; then
        echo "Sending current batch: ${FILES[*]}"
        upload "${FILES[@]}"
        FILES=()
        CURRENT_COUNT=0
    fi
done

if [ $CURRENT_COUNT -gt 0 ]; then
    echo "Sending final batch of $CURRENT_COUNT: ${FILES[*]}"
    # if successful, truncate each file.
    upload "${FILES[@]}"
fi
