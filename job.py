#!/usr/bin/env python
# encoding: utf-8
"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""

import argparse
import imp
import sys
import os
import json
import marshal
from datetime import datetime
from multiprocessing import Process
from telemetry_schema import TelemetrySchema
from persist import StorageLayout
from subprocess import Popen, PIPE
from boto.s3.connection import S3Connection

class Job:
    """A class for orchestrating a Telemetry MapReduce job"""
    # 1. read input filter
    # 2. generate filtered list of local input files
    # 2a. generate filtered list of remote input files
    # 3. load mapper
    # 4. spawn N processes
    # 5. distribute files among processes
    # 6. map(key, value, dims) each line in the file
    # 7. combine map output for each file
    # 8. reduce combine output overall

    def __init__(self, config):
        # Sanity check args.
        if config.num_mappers <= 0:
            raise ValueError("Number of mappers must be greater than zero")
        if config.num_reducers <= 0:
            raise ValueError("Number of reducers must be greater than zero")
        if not os.path.isdir(config.data_dir):
            raise ValueError("Data dir must be a valid directory")
        if not os.path.isdir(config.work_dir):
            raise ValueError("Work dir must be a valid directory")
        if not os.path.isfile(config.job_script):
            raise ValueError("Job script must be a valid python file")
        if not os.path.isfile(config.input_filter):
            raise ValueError("Input filter must be a valid json file")

        self._input_dir = config.data_dir
        self._work_dir = config.work_dir
        self._input_filter = TelemetrySchema(json.load(open(config.input_filter)))
        self._allowed_values = self._input_filter.sanitize_allowed_values()
        self._output_file = config.output
        self._num_mappers = config.num_mappers
        self._num_reducers = config.num_reducers
        self._local_only = config.local_only
        self._bucket_name = config.bucket
        self._aws_key = config.aws_key
        self._aws_secret_key = config.aws_secret_key
        modulefd = open(config.job_script)
        ## Lifted from FileDriver.py in jydoop.
        self._job_module = imp.load_module("telemetry_job", modulefd, config.job_script, ('.py', 'U', 1))

    def mapreduce(self):
        # Find files matching specified input filter
        files = self.local_files()
        remote_files = self.get_filtered_files_s3()

        file_count = len(files) + len(remote_files)
        # Not useful to have more mappers than files.
        if file_count < self._num_mappers:
            print "There are only", file_count, "input files. Reducing number of mappers accordingly."
            self._num_mappers = file_count

        # Partition files into reasonably equal groups for use by mappers
        partitions = self.partition(files, remote_files)

        # Partitions are ready. Map.
        mappers = []
        for i in range(self._num_mappers):
            if len(partitions[i]) > 0:
                p = Process(
                        target=Mapper,
                        args=(i, partitions[i], self._work_dir, self._job_module, self._num_reducers))
                mappers.append(p)
                p.start()
            else:
                print "Skipping mapper", i, "- no input files to process"
        for m in mappers:
            m.join()

        # Mappers are done. Reduce.
        reducers = []
        for i in range(self._num_reducers):
            p = Process(
                    target=Reducer,
                    args=(i, self._work_dir, self._job_module, self._num_mappers))
            reducers.append(p)
            p.start()
        for r in reducers:
            r.join()

        # Reducers are done.  Output results.
        os.rename(os.path.join(self._work_dir, "reducer_0"), self._output_file)
        if self._num_reducers > 1:
            out = open(self._output_file, "a")
            for i in range(1, self._num_reducers):
                # FIXME: this reads the entire reducer output into memory
                reducer_filename = os.path.join(self._work_dir, "reducer_" + str(i))
                reducer_output = open(reducer_filename, "r")
                out.write(reducer_output.read())
                reducer_output.close()
                os.remove(reducer_filename)

        # Clean up mapper outputs
        for m in range(self._num_mappers):
            for r in range(self._num_reducers):
                mfile = os.path.join(self._work_dir, "mapper_%d_%d" % (m, r))
                if os.path.exists(mfile):
                    os.remove(mfile)
                else:
                    print "Warning: Could not find", mfile

    def local_files(self):
        out_files = self.get_filtered_files(self._input_dir)
        if self._input_filter._include_invalid:
            invalid_dir = os.path.join(self._input_dir, TelemetrySchema.INVALID_DIR)
            #print "Looking for invalid data in", invalid_dir
            out_files += self.get_filtered_files(invalid_dir)
        return out_files

    # Split up the input files into groups of approximately-equal on-disk size.
    def partition(self, files, remote_files):
        namesize = [ { "name": files[i], "size": os.stat(files[i]).st_size, "dimensions": self._input_filter.get_dimensions(self._input_dir, files[i]) } for i in range(0, len(files)) ]
        partitions = []
        sums = []
        for p in range(self._num_mappers):
            partitions.append([])
            sums.append(0)
        min_idx = 0

        # Greedily assign the largest file to the smallest partition
        while len(namesize) > 0:
            current = namesize.pop()
            #print "putting", current, "into partition", min_idx
            partitions[min_idx].append(current)
            sums[min_idx] += current["size"]
            for m in range(0, len(sums)):
                if sums[m] < sums[min_idx]:
                    min_idx = m
        return partitions

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

    def get_filtered_files_s3(self):
        # Plain boto should be fast enough to list bucket contents.
        conn = S3Connection(self._aws_key, self._aws_secret_key)
        bucket = conn.get_bucket(self._bucket_name)

        out_files = []
        if not self._local_only:
            # TODO: potential optimization - if our input filter is reasonably
            #       restrictive an/or our list of keys is very long, it may be
            #       a win to use the "prefix" and "delimiter" params.
            for f in bucket.list():
                dims = self._input_filter.get_dimensions(".", f.name)
                print f.name, "->", ",".join(dims)
                include = True
                for i in range(len(self._allowed_values)):
                    if not self.filter_includes(i, dims[i]):
                        include = False
                        break
                if include:
                    out_files.append(f.name)
        return out_files

    def filter_includes(self, level, value):
        # Filter out 'invalid' data.  It is included explicitly if needed.
        if level == 0 and value == TelemetrySchema.INVALID_DIR:
            return False
        allowed_values = self._allowed_values[level]
        return self._input_filter.is_allowed(value, allowed_values)


class Context:
    def __init__(self, out, partition_count):
        self._basename = out
        self._partition_count = partition_count
        self._sinks = {} # = open(out, "wb")

    def partition(self, key):
        #print "hash of", key, "is", hash(key) % self._partition_count
        return hash(key) % self._partition_count

    def write(self, key, value):
        p = self.partition(key)
        if p not in self._sinks:
            out = open("%s_%d" % (self._basename, p), "wb")
            self._sinks[p] = out
        else:
            out = self._sinks[p]
        marshal.dump((key, value), out)

    def finish(self):
        for s in self._sinks.itervalues():
            s.close()


class TextContext(Context):
    def __init__(self, out):
        self._sink = open(out, "w")
        self._sinks = {0: self._sink}

    def write(self, key, value):
        self._sink.write(str(key))
        self._sink.write("\t")
        self._sink.write(str(value))
        self._sink.write("\n")


class Mapper:
    def __init__(self, mapper_id, inputs, work_dir, module, partition_count):
        print "I am mapper", mapper_id, ", and I'm mapping", len(inputs), "inputs:", inputs
        output_file = os.path.join(work_dir, "mapper_" + str(mapper_id))
        mapfunc = getattr(module, 'map', None)
        # TODO: pre-create all the files to avoid the situation where we don't
        #       get a key value hashing to each bucket.
        context = Context(output_file, partition_count)
        decompress_cmd = [StorageLayout.COMPRESS_PATH] + StorageLayout.DECOMPRESSION_ARGS
        if mapfunc is None or not callable(mapfunc):
            print "No map function!!!"
            sys.exit(1)

        # Pre-open all the files.  This should protect against the case where a
        # ".compressme" file disappears during processing.
        for input_file in inputs:
            try:
                if input_file["name"].endswith(StorageLayout.COMPRESSED_SUFFIX):
                    # TODO: Popen the decompress version of StorageLayout.COMPRESS_PATH
                    raw_handle = open(input_file["name"], "rb")
                    p_decompress = Popen(decompress_cmd, bufsize=65536, stdin=raw_handle, stdout=PIPE, stderr=sys.stderr)
                    input_file["handle"] = p_decompress.stdout
                else:
                    input_file["handle"] = open(input_file["name"], "r")
            except:
                print "Error opening", input_file["name"], "(skipping)"

        # now do another pass to actually process the files.
        for input_file in inputs:
            line_num = 0
            for line in input_file["handle"]:
                line_num += 1
                try:
                    key, value = line.split("\t", 1)
                    mapfunc(key, input_file["dimensions"], value, context)
                except ValueError:
                    # TODO: increment "bad line" metrics.
                    print "Bad line:", input_file["name"], ":", line_num
            input_file["handle"].close()
            # TODO: close raw_handle too?
        context.finish()


class Reducer:
    def __init__(self, reducer_id, work_dir, module, mapper_count):
        #print "I am reducer", reducer_id, ", and I'm reducing", mapper_count, "mapped files"
        output_file = os.path.join(work_dir, "reducer_" + str(reducer_id))
        context = TextContext(output_file)
        reducefunc = getattr(module, 'reduce', None)
        if reducefunc is None or not callable(reducefunc):
            print "No reduce function (that's ok)"
        else:
            collected = {}
            for i in range(mapper_count):
                mapper_file = os.path.join(work_dir, "mapper_%d_%d" % (i, reducer_id))
                # read, group by key, call reducefunc, output
                input_fd = open(mapper_file, "rb")
                while True:
                    try:
                        key, value = marshal.load(input_fd)
                        if key not in collected:
                            collected[key] = []
                        collected[key].append(value)
                    except EOFError:
                        break

            for k,v in collected.iteritems():
                reducefunc(k, v, context)
        context.finish()


def main(argv=None):
    parser = argparse.ArgumentParser(description='Run a MapReduce Job.', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("job_script", help="The MapReduce script to run")
    parser.add_argument("-l", "--local-only", help="Only process local files (exclude S3 data)", action="store_true")
    parser.add_argument("-m", "--num-mappers", metavar="N", help="Start N mapper processes", type=int, default=4)
    parser.add_argument("-r", "--num-reducers", metavar="N", help="Start N reducer processes", type=int, default=1)
    parser.add_argument("-d", "--data-dir", help="Base data directory", required=True)
    parser.add_argument("-b", "--bucket", help="S3 Bucket name")
    parser.add_argument("-k", "--aws-key", help="AWS Key")
    parser.add_argument("-s", "--aws-secret-key", help="AWS Secret Key")
    parser.add_argument("-w", "--work-dir", help="Location to put temporary work files", default="/tmp/telemetry_mr")
    parser.add_argument("-o", "--output", help="Filename to use for final job output", required=True)
    #TODO: make the input filter optional, default to "everything valid" and generate dims intelligently.
    parser.add_argument("-f", "--input-filter", help="File containing filter spec", required=True)
    args = parser.parse_args()

    if not args.local_only:
        # if we want to process remote data, 3 arguments are required.
        for remote_req in ["bucket", "aws_key", "aws_secret_key"]:
            if not hasattr(args, remote_req) or getattr(args, remote_req) is None:
                print "ERROR:", remote_req, "is a required option"
                parser.print_help()
                sys.exit(-1)

    job = Job(args)
    start = datetime.now()
    job.mapreduce()
    delta = (datetime.now() - start)
    print "All done in %dm %ds %dms" % (delta.seconds / 60, delta.seconds % 60, delta.microseconds / 1000)

if __name__ == "__main__":
    sys.exit(main())
