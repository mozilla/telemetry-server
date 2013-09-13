#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import hashlib

# might as well return the size too...
def md5file(filename, chunksize=8192):
    md5 = hashlib.md5()
    size = 0
    with open(filename, "rb") as data:
        while True:
            chunk = data.read(chunksize)
            if not chunk:
                break
            md5.update(chunk)
            size += len(chunk)
    return md5.hexdigest(), size
