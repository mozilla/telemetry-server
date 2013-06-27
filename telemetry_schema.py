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
        clean_dims = self.apply_schema(dimensions)
        submission_date = clean_dims.pop()
        return self.get_current_file(basedir, clean_dims, submission_date)

    def get_filename_invalid(self, basedir, dimensions):
        clean_dims = self.apply_schema(dimensions)
        submission_date = clean_dims.pop()
        # prepend invalid dir name.
        clean_dims.insert(0, TelemetrySchema.INVALID_DIR)
        return self.get_current_file(basedir, clean_dims, submission_date)

    def get_current_file(self, basedir, dims, submission_date):
        # TODO: get files in order, find newest non-full one
        # use hex digits for seqnum
        dirname = os.path.join(*dims)
        return ".".join((os.path.join(basedir, self.safe_filename(dirname)), submission_date, "000", "log"))

    def dimensions_from(self, info, submission_date):
        dimensions = []
        for dim in self._dimensions:
            if dim["field_name"] == "submission_date":
                dimensions.append(submission_date)
            else:
                dimensions.append(info.get(dim["field_name"], "UNKNOWN"))
        return dimensions
