#!/bin/bash
VERSION=0.3
NAME=slowsql
TARBALL=${NAME}-$VERSION.tar.gz
if [ ! -d "combine.py" ]; then
    echo "Fetching 'combine.py' from github..."
    wget https://github.com/mreid-moz/slowsql-dashboard/raw/master/data/combine.py
else
    echo "Using existing 'combine.py'"
fi

if [ -f "$TARBALL" ]; then
    rm -v "$TARBALL"
fi
tar czvf "$TARBALL" \
        combine.py \
        csv_header.txt \
        filter_template.json \
        run.sh \
        slowsql.py

S3PATH=s3://telemetry-analysis-code/$NAME/$TARBALL

echo "Packaged $NAME code as $TARBALL"
if [ ! -z "$(which aws)" ]; then
    aws s3 cp $TARBALL $S3PATH
    echo "Code successfully uploaded to S3"
else
    echo "AWS CLI not found - you should manually upload to $S3PATH"
fi
