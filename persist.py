#!/usr/bin/env python
# encoding: utf-8
"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""

import os
import sys
import json
import re

import urllib2


class StorageLayout:
    """A class for encapsulating the on-disk data layout for Telemetry"""

    def __init__(self, schema, basedir):
        self._schema = schema
        self._dimensions = self._schema["dimensions"]
        self._basedir = basedir
        #if !self._schema:
        #    schema = json.load(os.path.join(self._basedir, "telemetry_schema.json"))

    def write(self, uuid, obj, dimensions):
        filename = self.get_filename(dimensions)
        self.write_filename(uuid, obj, filename)

    def write_invalid(self, uuid, obj, dimensions, err):
        # TODO: put 'err' into file?
        filename = self.get_filename_invalid(dimensions)
        self.write_filename(uuid, obj, filename, err)

    def write_filename(self, uuid, obj, filename, err=None):
        sys.stderr.write("Writing %s to %s\n" % (uuid, filename))
        try:
            fout = open(filename, "a")
        except IOError:
            os.makedirs(os.path.dirname(filename))
            fout = open(filename, "a")
        fout.write(uuid)
        fout.write("\t")
        if err is not None:
            fout.write(str(err).replace("\t", " "))
            fout.write("\t")
        if isinstance(obj, basestring):
            fout.write(obj)
        else:
            fout.write(json.dumps(obj))
        fout.write("\n")
        fout.close()

    def get_allowed_value(self, value, allowed_values):
        if allowed_values == "*":
            return str(value)
        elif isinstance(allowed_values, list):
            if value in allowed_values:
                return value
        # elif it's a regex, apply the regex.
        # elif it's a special case (date-in-past, uuid, etc)
        return "OTHER"

    def apply_schema(self, dimensions):
        cleaned = ["OTHER"] * len(self._dimensions)
        for i, v in enumerate(dimensions):
            cleaned[i] = self.get_allowed_value(v, self._dimensions[i]["allowed_values"])

        return cleaned

    def get_filename(self, dimensions):
        dirname = os.path.join(*self.apply_schema(dimensions))
        # TODO: get files in order, find newest non-full one
        return os.path.join(self._basedir, re.sub(r'[^a-zA-Z0-9_/.]', "_", dirname)) + ".000"

    def get_filename_invalid(self, dimensions):
        dirname = os.path.join(*self.apply_schema(dimensions))
        # TODO: get files in order, find newest non-full one
        return os.path.join(self._basedir, "invalid", re.sub(r'[^a-zA-Z0-9_/.]', "_", dirname)) + ".000"
