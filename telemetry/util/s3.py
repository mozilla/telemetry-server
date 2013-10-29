#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

def list_partitions(bucket, prefix='', level=0, schema=None, include_keys=False):
    #print "Listing...", prefix, level
    if schema is not None:
        allowed_values = schema.sanitize_allowed_values()
    delimiter = '/'
    if level > 3:
        delimiter = '.'
    for k in bucket.list(prefix=prefix, delimiter=delimiter):
        partitions = k.name.split("/")
        if level > 3:
            # split the last couple of partition components by "." instead of "/"
            partitions.extend(partitions.pop().split(".", 2))
        if schema is None or schema.is_allowed(partitions[level], allowed_values[level]):
            if level >= 5:
                if include_keys:
                    for f in bucket.list(prefix=k.name):
                        yield f
                else:
                    yield k.name
            else:
                for prefix in list_partitions(bucket, k.name, level + 1, schema, include_keys):
                    yield prefix
