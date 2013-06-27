#!/usr/bin/env python
# encoding: utf-8
"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""

import re
import os

class TelemetrySchema:
    INVALID_DIR = "invalid"
    DISALLOWED_VALUE = "OTHER"

    def __init__(self, spec):
        self._spec = spec
        self._dimensions = self._spec["dimensions"]

    def safe_filename(self, value):
        return re.sub(r'[^a-zA-Z0-9_/.]', "_", value)

    def apply_schema(self, dimensions):
        num_dims = len(self._dimensions)
        cleaned = [TelemetrySchema.DISALLOWED_VALUE] * num_dims
        if dimensions is not None:
            for i, v in enumerate(dimensions):
                # Don't enumerate past the max number of 'allowed' dimensions
                # ie. if someone passed in 100 dims.
                if i >= num_dims:
                    break
                cleaned[i] = self.get_allowed_value(v, self._dimensions[i]["allowed_values"])
        return cleaned

    def is_allowed(self, value, allowed_values):
        if allowed_values == "*":
            return True
        elif isinstance(allowed_values, list):
            if value in allowed_values:
                return True
        # elif it's a regex, apply the regex.
        # elif it's a special case (date-in-past, uuid, etc)
        return False

    def get_allowed_value(self, value, allowed_values):
        if self.is_allowed(value, allowed_values):
            return str(value)
        return TelemetrySchema.DISALLOWED_VALUE

    def get_filename(self, basedir, dimensions):
        dirname = os.path.join(*self.apply_schema(dimensions))
        # TODO: get files in order, find newest non-full one
        return os.path.join(basedir, self.safe_filename(dirname)) + ".000"

    def get_filename_invalid(self, basedir, dimensions):
        dirname = os.path.join(*self.apply_schema(dimensions))
        # TODO: get files in order, find newest non-full one
        return os.path.join(basedir, TelemetrySchema.INVALID_DIR, self.safe_filename(dirname)) + ".000"
