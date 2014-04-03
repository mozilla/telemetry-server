#!/bin/bash
VERSION=0.1
NAME=fxosping
TARBALL=${NAME}-$VERSION.tar.gz
BASE=$(pwd)
THIS_DIR=$(cd "`dirname "$0"`"; pwd)

if [ -f "$TARBALL" ]; then
    rm -v "$TARBALL"
fi

cd "$THIS_DIR"
tar czvf "$BASE/$TARBALL" \
        fxosping.py \
        filter_template.json \
        run.sh

S3PATH=s3://telemetry-analysis-code/$NAME/$TARBALL

echo "Packaged $NAME code as $TARBALL"
if [ ! -z "$(which aws)" ]; then
    aws s3 cp $TARBALL $S3PATH
    echo "Code successfully uploaded to S3"
else
    echo "AWS CLI not found - you should manually upload to $S3PATH"
fi
