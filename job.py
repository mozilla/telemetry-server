#!/usr/bin/env python
# encoding: utf-8
"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""

import argparse
import sys
import os
import json
from telemetry_schema import TelemetrySchema

class Job:
    """A class for orchestrating a Telemetry MapReduce job"""
    # 1. read input filter
    # 2. generate filtered list of input files
    # 3. load mapper
    # 4. spawn N processes
    # 5. distribute files among processes
    # 6. map(key, value, dims) each line in the file
    # 7. combine map output for each file
    # 8. reduce combine output overall

    def __init__(self, input_dir, output_file, input_filter=None):
        self._input_dir = input_dir
        self._input_filter = TelemetrySchema(json.load(open(input_filter)))
        self._allowed_values = self._input_filter.sanitize_allowed_values()
        self._output_file = output_file

    def files(self):
        out_files = self.get_filtered_files(self._input_dir)
        if self._input_filter._include_invalid:
            invalid_dir = os.path.join(self._input_dir, TelemetrySchema.INVALID_DIR)
            print "Looking for invalid data in", invalid_dir
            out_files += self.get_filtered_files(invalid_dir)
        return out_files

    def get_filtered_files(self, searchdir):
        level_offset = searchdir.count(os.path.sep)
        out_files = []
        for root, dirs, files in os.walk(searchdir):
            level = root.count(os.path.sep) - level_offset
            dirs[:] = [i for i in dirs if self.filter_includes(level, i)]
            for f in files:
                full_filename = os.path.join(root, f)
                dims = self._input_filter.get_dimensions(searchdir, full_filename)
                include = True
                for l in range(level, len(self._allowed_values)):
                    if not self.filter_includes(l, dims[l]):
                        include = False
                        break
                if include:
                    out_files.append(full_filename)
        return out_files

    def filter_includes(self, level, value):
        # Filter out 'invalid' data.  It is included explicitly if needed.
        if level == 0 and value == TelemetrySchema.INVALID_DIR:
            return False
        allowed_values = self._allowed_values[level]
        return self._input_filter.is_allowed(value, allowed_values)

def main(argv=None):
    parser = argparse.ArgumentParser(description='Run a MapReduce Job.')
    parser.add_argument("job_script", help="The MapReduce script to run")
    parser.add_argument("-m", "--num-mappers", metavar="N", help="Start N mapper processes", type=int, default=4)
    parser.add_argument("-r", "--num-reducers", metavar="N", help="Start N reducer processes", type=int, default=1)
    parser.add_argument("-d", "--data-dir", help="Base data directory")
    parser.add_argument("-o", "--output", help="Filename to use for final job output")
    parser.add_argument("-f", "--input-filter", help="File containing filter spec")
    args = parser.parse_args()

    job = Job(args.data_dir, args.output, args.input_filter)

    files = job.files()
    print "Found ", len(files), "files:"
    print files


if __name__ == "__main__":
    sys.exit(main())
