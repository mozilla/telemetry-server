#!/bin/bash
VERSION=0.4
NAME=chromehangs
TARBALL=${NAME}-$VERSION.tar.gz

if [ -f "$TARBALL" ]; then
    rm -v "$TARBALL"
fi
tar czvf "$TARBALL" \
        filter_template.json \
        run.sh \
        run_public.sh \
        symbolicate.py \
        extract_common_stacks.py \
        combine.py \
        combine_week.sh \
        chromehangs.py

echo "Packaged $NAME code as $TARBALL"
if [ ! -z "$(which aws)" ]; then
    # Private analysis:
    aws s3 cp $TARBALL s3://telemetry-analysis-code/jobs/ChromeHangs/$TARBALL
    # Public analysis:
    aws s3 cp $TARBALL s3://telemetry-analysis-code/jobs/ChromeHangsWeekly/$TARBALL
    echo "Code successfully uploaded to S3"
else
    echo "AWS CLI not found - you should manually upload to s3 via http://telemetry-dash.mozilla.org"
fi
