#!/bin/bash
VERSION=0.1
NAME=chromehangs
TARBALL=${NAME}-$VERSION.tar.gz
DEP=symbolicate.py
# for some reason, plain wget doesn't work anymore (as of 2014/04/09).
WGET_OPT="--secure-protocol=SSLv3"
DEPURL=https://github.com/mozilla-metrics/telemetry-toolbox/raw/master/src/main/python/symbolicate.py
if [ ! -d "$DEP" ]; then
    echo "Fetching '$DEP' from github..."
    wget $WGET_OPT $DEPURL
else
    echo "Using existing '$DEP'"
fi

if [ -f "$TARBALL" ]; then
    rm -v "$TARBALL"
fi
tar czvf "$TARBALL" \
        $DEP \
        filter_template.json \
        run.sh \
        chromehangs.py

S3PATH=s3://telemetry-analysis-code/$NAME/$TARBALL

echo "Packaged $NAME code as $TARBALL"
if [ ! -z "$(which aws)" ]; then
    aws s3 cp $TARBALL $S3PATH
    echo "Code successfully uploaded to S3"
else
    echo "AWS CLI not found - you should manually upload to $S3PATH"
fi
