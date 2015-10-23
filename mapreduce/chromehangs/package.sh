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
