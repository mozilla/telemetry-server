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
import logging
import logging.handlers

from telemetry_schema import TelemetrySchema
import urllib2


class StorageLayout:
    """A class for encapsulating the on-disk data layout for Telemetry"""

    def __init__(self, schema, basedir):
        self._schema = schema
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
        # TODO: logging?
        #sys.stderr.write("Writing %s to %s\n" % (uuid, filename))
        # TODO: keep a cache of these loggers
        if not os.path.isfile(filename):
            # if this fails, I want the whole thing to fail so don't try/except.
            dirname = os.path.dirname(filename)
            if not os.path.isdir(dirname):
                os.makedirs(dirname)
        logger = logging.getLogger(filename)
        handler = logging.handlers.RotatingFileHandler(filename, maxBytes=500000000, backupCount=1000)
        formatter = logging.Formatter("%(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        if isinstance(obj, basestring):
            payload = obj
        else:
            payload = json.dumps(obj, separators=(',', ':'))
        logger.critical("%s\t%s", uuid, payload)
