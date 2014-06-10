#!/bin/bash
VERSION=0.2
NAME=addon_perf
TARBALL=${NAME}-$VERSION.tar.gz

if [ -f "$TARBALL" ]; then
    rm -v "$TARBALL"
fi
tar czvf "$TARBALL" \
        run.sh \
        README.md
