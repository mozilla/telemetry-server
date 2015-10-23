#!/bin/bash
VERSION=0.4
NAME=SlowSQL
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

echo "Packaged $NAME code as $TARBALL"
