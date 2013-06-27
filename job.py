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
        self._input_filter = json.load(input_filter)
        self._include_invalid = False
        if "include_invalid" in self._input_filter:
            self._include_invalid = self._input_filter["include_invalid"]
        self._output_file = output_file

    def files(self):
        out_files = self.get_filtered_files(self._input_dir)
        if self._include_invalid:
            out_files += self.get_filtered_files(os.path.join(self._input_dir, "invalid"))
        return out_files

    def get_filtered_files(self, searchdir):
        level_offset = self._input_dir.count(os.path.sep)
        out_files = []
        for root, dirs, files in os.walk(self._input_dir):
            level = root.count(os.path.sep) - level_offset
            dirs[:] = [i for i in dirs if self.filter_includes(level, i)]
            out_files += [os.path.join(root, f) for f in files]
        return out_files

    def filter_includes(self, level, value):
        if self._input_filter is None:
            # by default, filter out 'invalid' data
            if level == 0 and value == 'invalid':
                return False
            return True

        dims = self._input_filter.get("dimensions", [])
        if dims and len(dims) > level:
            allowed_values = dims[level]["allowed_values"]
            if allowed_values == "*":

        #print "Checking filter for ", level, value
        if level == 0 and value == "20130620":
            return False
        if level == 1 and value == "idle_daily":
            return False
        if level == 3 and value == "beta":
            return False
        return True

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
    print files


if __name__ == "__main__":
    sys.exit(main())
