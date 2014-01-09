#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import re
import os

class TelemetrySchema:
    DISALLOWED_VALUE = "OTHER"

    def __init__(self, spec):
        self._spec = spec
        self._dimensions = self._spec["dimensions"]

    def safe_filename(self, value):
        return re.sub(r'[^a-zA-Z0-9_/.]', "_", value)

    def sanitize_allowed_values(self):
        dims = []
        for d in self._dimensions:
            allowed = d["allowed_values"]
            if isinstance(allowed, list):
                allowed = [self.safe_filename(a) for a in allowed]
            dims.append(allowed)
        return dims

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
        elif isinstance(allowed_values, dict):
            if "min" in allowed_values and value < allowed_values["min"]:
                return False
            if "max" in allowed_values and value > allowed_values["max"]:
                return False
            return True
        # Treat a string the same as a single-element array:
        elif isinstance(allowed_values, basestring):
            return value == allowed_values
        # elif it's a regex, apply the regex.
        # elif it's a special case (date-in-past, uuid, etc)
        return False

    def get_allowed_value(self, value, allowed_values):
        if self.is_allowed(value, allowed_values):
            return str(value)
        return TelemetrySchema.DISALLOWED_VALUE

    def get_dimensions(self, basedir, filename):
        canonical_base = os.path.realpath(basedir)
        canonical_file = os.path.realpath(filename)

        if not canonical_file.startswith(canonical_base):
            raise ValueError("Error: file '%s' is not under base dir '%s'" % (filename, basedir))

        # Chop off the base dir and one path separator
        dimfile = canonical_file[len(canonical_base)+1:]
        dims = dimfile.split(os.path.sep)
        filename = dims.pop()
        file_dims = filename.split(".")

        # Last two dimensions are in the filename, separated by dots:
        dims.append(file_dims.pop(0))
        dims.append(file_dims.pop(0))
        return dims

    def get_dimension_map(self, dims):
        dim_map = {}
        for i in range(len(self._dimensions)):
            dim_map[self._dimensions[i]["field_name"]] = dims[i]
        return dim_map

    def get_filename(self, basedir, dimensions, version=1):
        clean_dims = self.apply_schema(dimensions)
        submission_date = clean_dims.pop()
        return self.get_current_file(basedir, clean_dims, submission_date, version)

    def get_current_file(self, basedir, dims, submission_date, version=1):
        dirname = os.path.join(*dims)
        return ".".join((os.path.join(basedir, self.safe_filename(dirname)), submission_date, "v" + str(version), "log"))

    def dimensions_from(self, info, submission_date):
        dimensions = []
        for dim in self._dimensions:
            if dim["field_name"] == "submission_date":
                dimensions.append(submission_date)
            else:
                dimensions.append(info.get(dim["field_name"], "UNKNOWN"))
        return dimensions
