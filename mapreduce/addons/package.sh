#!/bin/bash
VERSION=0.1
NAME=addons
TARBALL=${NAME}-$VERSION.tar.gz

if [ -f "$TARBALL" ]; then
    rm -v "$TARBALL"
fi
tar czvf "$TARBALL" \
        run.sh \
        README.md
