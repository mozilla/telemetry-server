#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import argparse
import imp
import sys
import os
import json
import marshal
import traceback
from datetime import datetime
from multiprocessing import Process
from telemetry.telemetry_schema import TelemetrySchema
from telemetry.persist import StorageLayout
import telemetry.util.s3 as s3util
import telemetry.util.timer as timer
import subprocess
from subprocess import Popen, PIPE
from boto.s3.connection import S3Connection

def find_min_idx(stuff):
    min_idx = 0
    for m in range(1, len(stuff)):
        if stuff[m] < stuff[min_idx]:
            min_idx = m
    return min_idx


class Job:
    """A class for orchestrating a Telemetry MapReduce job"""
    DOWNLOAD_BATCH_SIZE = 100
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
        if self._input_dir[-1] == os.path.sep:
            self._input_dir = self._input_dir[0:-1]
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

    def dump_stats(self, partitions):
        total = sum(partitions)
        avg = total / len(partitions)
        for i in range(len(partitions)):
            print "Partition %d contained %d (%+d)" % (i, partitions[i], float(partitions[i]) - avg)

    def fetch_remotes(self, remotes):
        # TODO: download remotes in groups of size DOWNLOAD_BATCH_SIZE
        remote_names = [ r["name"] for r in remotes if r["type"] == "remote" ]

        # TODO: check cache first.
        result = 0

        fetch_cwd = os.path.join(self._work_dir, "cache")
        if len(remote_names) > 0:
            if not os.path.isdir(fetch_cwd):
                os.makedirs(fetch_cwd)
            fetch_cmd = ["/usr/local/bin/s3funnel"]
            fetch_cmd.append(self._bucket_name)
            fetch_cmd.append("get")
            fetch_cmd.append("-a")
            fetch_cmd.append(self._aws_key)
            fetch_cmd.append("-s")
            fetch_cmd.append(self._aws_secret_key)
            fetch_cmd.append("-t")
            fetch_cmd.append("8")
            start = datetime.now()
            result = subprocess.call(fetch_cmd + remote_names, cwd=fetch_cwd)
            duration_sec = timer.delta_sec(start)
            downloaded_bytes = sum([ r["size"] for r in remotes if r["type"] == "remote" ])
            downloaded_mb = float(downloaded_bytes) / 1024.0 / 1024.0
            print "Downloaded %.2fMB in %.2fs (%.2fMB/s)" % (downloaded_mb, duration_sec, downloaded_mb / duration_sec)
        return result

    def mapreduce(self):
        # Find files matching specified input filter
        files = self.get_filtered_files(self._input_dir)
        remote_files = self.get_filtered_files_s3()

        file_count = len(files) + len(remote_files)
        # Not useful to have more mappers than input files.
        if file_count < self._num_mappers:
            print "Filter matched only %s input files (%s local in %s and %s " \
                  "remote from %s). Reducing number of mappers accordingly." \
                  % (file_count, len(files), self._input_dir, len(remote_files),
                      self._bucket_name)
            self._num_mappers = file_count

        # Partition files into reasonably equal groups for use by mappers
        print "Partitioning input data..."
        partitions = self.partition(files, remote_files)
        print "Done"

        # Partitions are ready. Map.
        mappers = []
        for i in range(self._num_mappers):
            if len(partitions[i]) > 0:
                # Fetch the files we need for each mapper
                print "Fetching remotes for partition", i
                self.fetch_remotes(partitions[i])
                print "Done"
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

        # TODO: clean up downloaded files?

        # Clean up mapper outputs
        for m in range(self._num_mappers):
            for r in range(self._num_reducers):
                mfile = os.path.join(self._work_dir, "mapper_%d_%d" % (m, r))
                if os.path.exists(mfile):
                    os.remove(mfile)
                else:
                    print "Warning: Could not find", mfile

    # Split up the input files into groups of approximately-equal on-disk size.
    def partition(self, files, remote_files):
        namesize = [ { "type": "local", "name": files[i], "size": os.stat(files[i]).st_size, "dimensions": self._input_filter.get_dimensions(self._input_dir, files[i]) } for i in range(0, len(files)) ]
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
            min_idx = find_min_idx(sums)

        # And now do the same with the remote files.
        # TODO: if this is too slow, just distribute remote files round-robin.
        if len(remote_files) > 0:
            conn = S3Connection(self._aws_key, self._aws_secret_key)
            bucket = conn.get_bucket(self._bucket_name)
            for r in remote_files:
                size = r.size
                dims = self._input_filter.get_dimensions(".", r.name)
                remote = {"type": "remote", "name": r.name, "size": size, "dimensions": dims}
                #print "putting", remote, "into partition", min_idx
                partitions[min_idx].append(remote)
                sums[min_idx] += size
                min_idx = find_min_idx(sums)

        # Print out some info to see how balanced the partitions were:
        self.dump_stats(sums)

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
        out_files = []
        if not self._local_only:
            print "Fetching file list from S3..."
            # Plain boto should be fast enough to list bucket contents.
            conn = S3Connection(self._aws_key, self._aws_secret_key)
            bucket = conn.get_bucket(self._bucket_name)
            start = datetime.now()
            count = 0
            # Filter input files by partition. If the filter is reasonably
            # selective, this can be much faster than listing all files in the
            # bucket.
            for f in s3util.list_partitions(bucket, schema=self._input_filter, include_keys=True):
                count += 1
                out_files.append(f)
                if count % 1000 == 0:
                    print "Listed", count, "so far"
            conn.close()
            duration = timer.delta_sec(start)
            print "Listed", len(out_files), "files in", duration, "seconds"
        return out_files

    def filter_includes(self, level, value):
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
        self.work_dir = work_dir

        print "I am mapper", mapper_id, ", and I'm mapping", len(inputs), "inputs:", inputs
        output_file = os.path.join(work_dir, "mapper_" + str(mapper_id))
        mapfunc = getattr(module, 'map', None)
        # TODO: pre-create all the files to avoid the situation where we don't
        #       get a key value hashing to each bucket.
        context = Context(output_file, partition_count)
        if mapfunc is None or not callable(mapfunc):
            print "No map function!!!"
            sys.exit(1)

        # TODO: Stream/decompress the files directly.
        for input_file in inputs:
            try:
                self.open_input_file(input_file)
            except:
                print "Error opening", input_file["name"], "(skipping)"
                traceback.print_exc(file=sys.stderr)
                continue
            line_num = 0
            for line in input_file["handle"]:
                line_num += 1
                try:
                    key, value = line.split("\t", 1)
                    mapfunc(key, input_file["dimensions"], value, context)
                except ValueError, e:
                    # TODO: increment "bad line" metrics.
                    print "Bad line:", input_file["name"], ":", line_num, e
            input_file["handle"].close()
            if "raw_handle" in input_file:
                input_file["raw_handle"].close()
        context.finish()

    def open_input_file(self, input_file):
        filename = input_file["name"]
        if input_file["type"] == "remote":
            # Read so-called remote files from the local cache. Go on the
            # assumption that they have already been downloaded.
            filename = os.path.join(self.work_dir, "cache", input_file["name"])

        if filename.endswith(StorageLayout.COMPRESSED_SUFFIX):
            decompress_cmd = [StorageLayout.COMPRESS_PATH] + StorageLayout.DECOMPRESSION_ARGS
            raw_handle = open(filename, "rb")
            input_file["raw_handle"] = raw_handle
            # Popen the decompressing version of StorageLayout.COMPRESS_PATH
            p_decompress = Popen(decompress_cmd, bufsize=65536, stdin=raw_handle, stdout=PIPE, stderr=sys.stderr)
            input_file["handle"] = p_decompress.stdout
        else:
            input_file["handle"] = open(filename, "r")

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


def main():
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
    duration = timer.delta_sec(start)
    print "All done in %.2fs" % (duration)

if __name__ == "__main__":
    sys.exit(main())
