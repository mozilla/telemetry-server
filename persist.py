#!/usr/bin/env python
# encoding: utf-8
"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""

import os
import sys
try:
    import simplejson as json
except ImportError:
    import json
import re

from telemetry_schema import TelemetrySchema
import urllib2


class StorageLayout:
    """A class for encapsulating the on-disk data layout for Telemetry"""

    def __init__(self, schema, basedir):
        self._schema = TelemetrySchema(schema)
        self._basedir = basedir
        #if !self._schema:
        #    schema = json.load(os.path.join(self._basedir, "telemetry_schema.json"))

    def write(self, uuid, obj, dimensions):
        filename = self._schema.get_filename(self._basedir, dimensions)
        self.write_filename(uuid, obj, filename)

    def write_invalid(self, uuid, obj, dimensions, err):
        # TODO: put 'err' into file?
        filename = self._schema.get_filename_invalid(self._basedir, dimensions)
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
            # Use minimal json (without excess spaces)
            fout.write(json.dumps(obj, separators=(',', ':')))
        fout.write("\n")
        fout.close()
