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
import errno
from datetime import datetime
from multiprocessing import Process
from telemetry.telemetry_schema import TelemetrySchema
from telemetry.util.compress import CompressedFile
import telemetry.util.s3 as s3util
import telemetry.util.timer as timer
import subprocess
import csv
import signal
import cProfile
import collections
import gc
try:
    from boto.s3.connection import S3Connection
    BOTO_AVAILABLE=True
except ImportError:
    BOTO_AVAILABLE=False


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
        if config.get("num_mappers") <= 0:
            raise ValueError("Number of mappers must be greater than zero")
        if config.get("num_reducers") <= 0:
            raise ValueError("Number of reducers must be greater than zero")
        if not os.path.isdir(config.get("data_dir")):
            raise ValueError("Data dir must be a valid directory")
        if not os.path.isdir(config.get("work_dir")):
            raise ValueError("Work dir must be a valid directory")
        if not os.path.isfile(config.get("job_script", "")):
            raise ValueError("Job script must be a valid python file")
        if not os.path.isfile(config.get("input_filter")):
            raise ValueError("Input filter must be a valid json file")

        self._input_dir = config.get("data_dir")
        if self._input_dir[-1] == os.path.sep:
            self._input_dir = self._input_dir[0:-1]
        self._work_dir = config.get("work_dir")
        with open(config.get("input_filter")) as filter_file:
            self._input_filter = TelemetrySchema(json.load(filter_file))
        self._allowed_values = self._input_filter.sanitize_allowed_values()
        self._output_file = config.get("output")
        self._num_mappers = config.get("num_mappers")
        self._num_reducers = config.get("num_reducers")
        self._local_only = config.get("local_only")
        self._bucket_name = config.get("bucket")
        self._aws_key = config.get("aws_key")
        self._aws_secret_key = config.get("aws_secret_key")
        self._profile = config.get("profile")
        self._delete_data = config.get("delete_data")
        with open(config.get("job_script")) as modulefd:
            # let the job script import additional modules under its path
            sys.path.append(os.path.dirname(config.get("job_script")))
            ## Lifted from FileDriver.py in jydoop.
            self._job_module = imp.load_module(
                "telemetry_job", modulefd, config.get("job_script"), ('.py', 'U', 1))

    def dump_stats(self, partitions):
        total = sum(partitions)
        avg = total / len(partitions)
        for i in range(len(partitions)):
            print "Partition %d contained %d (%+d)" % (i, partitions[i], float(partitions[i]) - avg)

    def fetch_remotes(self, remotes):
        # TODO: fetch remotes inside Mappers, and process each one as it becomes available.
        remote_names = ( r.name for r in remotes if r.remote )

        # TODO: check cache first.
        result = 0

        fetch_cwd = os.path.join(self._work_dir, "cache")
        if not os.path.isdir(fetch_cwd):
            os.makedirs(fetch_cwd)
        loader = s3util.Loader(fetch_cwd, self._bucket_name, aws_key=self._aws_key, aws_secret_key=self._aws_secret_key)
        start = datetime.now()
        downloaded_bytes = 0
        for local, remote, err in loader.get_list(remote_names):
            if err is None:
                # print "Downloaded", remote
                downloaded_bytes += os.path.getsize(local)
            else:
                print "Failed to download", remote
                result += 1
        duration_sec = timer.delta_sec(start)
        downloaded_mb = float(downloaded_bytes) / 1024.0 / 1024.0
        print "Downloaded %.2fMB in %.2fs (%.2fMB/s)" % (downloaded_mb, duration_sec, downloaded_mb / duration_sec)
        return result

    def dedupe_remotes(self, remote_files, local_files):
        return ( r for r in remote_files
                   if os.path.join(self._input_dir, r.name) not in local_files )

    def mapreduce(self):
        # Find files matching specified input filter
        files = set(self.get_filtered_files(self._input_dir))
        remote_files = self.get_filtered_files_s3()

        # If we're using the cache dir as the data dir, we will end up reading
        # each already-downloaded file twice, so we should skip any remote files
        # that exist in the data dir.
        remote_files = self.dedupe_remotes(remote_files, files)

        # Partition files into reasonably equal groups for use by mappers
        print "Partitioning input data..."
        partitions = self.partition(files, remote_files)
        print "Done"

        if not any(part for part in partitions):
             print "Filter didn't match any files... nothing to do"
             return

        partitions = [part for part in partitions if part]

        # Not useful to have more mappers than partitions.
        if len(partitions) < self._num_mappers:
            print "Filter matched only %d input files (%d local in %s and %d " \
                  "remote from %s). Reducing number of mappers accordingly." % (
                  len(partitions), len(files), self._input_dir,
                  sum(len(part) for part in partitions) - len(files),
                  self._bucket_name)
            self._num_mappers = len(partitions)

        # Free up our set of names. We want to minimize
        # our memory usage prior to forking map jobs.
        files = None
        gc.collect()

        def checkExitCode(proc):
            # If process was terminated by a signal, exitcode is the negative signal value
            if proc.exitcode == -signal.SIGKILL:
                # SIGKILL is most likely an OOM kill
                raise MemoryError("%s ran out of memory" % proc.name)
            elif proc.exitcode:
                raise OSError("%s exited with code %d" % (proc.name, proc.exitcode))

        # Partitions are ready. Map.
        mappers = []
        for i in range(self._num_mappers):
            if len(partitions[i]) > 0:
                # Fetch the files we need for each mapper
                if not self._local_only:
                    print "Fetching remotes for partition", i
                    fetch_result = self.fetch_remotes(partitions[i])
                    if fetch_result == 0:
                        print "Remote files fetched successfully"
                    else:
                        print "ERROR: Failed to fetch", fetch_result, "files."
                        # TODO: Bail, since results will be unreliable?
                p = Process(
                        target=Mapper,
                        name=("Mapper-%d" % i),
                        args=(i, self._profile, partitions[i], self._work_dir, self._job_module, self._num_reducers, self._delete_data))
                mappers.append(p)
                p.start()
            else:
                print "Skipping mapper", i, "- no input files to process"
        for m in mappers:
            m.join()
            checkExitCode(m)

        # Mappers are done. Reduce.
        reducers = []
        for i in range(self._num_reducers):
            p = Process(
                    target=Reducer,
                    name=("Reducer-%d" % i),
                    args=(i, self._profile, self._work_dir, self._job_module, self._num_mappers))
            reducers.append(p)
            p.start()
        for r in reducers:
            r.join()
            checkExitCode(r)

        # Reducers are done.  Output results.
        to_combine = 1
        try:
            os.rename(os.path.join(self._work_dir, "reducer_0"), self._output_file)
        except OSError, e:
            if e.errno != errno.EXDEV:
                raise
            else:
                # OSError: [Errno 18] Invalid cross-device link (EXDEV == 18)
                # We can't rename across devices :( Copy / delete instead.
                to_combine = 0

        # TODO: If _output_file ends with a compressed suffix (.gz, .xz, .bz2, etc),
        #       try to compress it after writing.
        if self._num_reducers > to_combine:
            with open(self._output_file, "a") as out:
                for i in range(to_combine, self._num_reducers):
                    # FIXME: this reads the entire reducer output into memory
                    reducer_filename = os.path.join(self._work_dir, "reducer_" + str(i))
                    with open(reducer_filename, "r") as reducer_output:
                        out.write(reducer_output.read())
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

    MapperInput = collections.namedtuple('MapperInput',
        ('remote', 'name', 'size', 'dimensions'))

    # Split up the input files into groups of approximately-equal on-disk size.
    def partition(self, files, remote_files):
        namesize = ( self.MapperInput(
            remote=False,
            name=fn,
            size=os.stat(fn).st_size,
            dimensions=self._input_filter.get_dimensions(self._input_dir, fn)
        ) for fn in files )

        partitions = [[] for i in range(self._num_mappers)]
        sums = [0 for i in range(self._num_mappers)]
        min_idx = 0

        def find_min_idx(stuff):
            return min(enumerate(stuff), key=lambda x: x[1])[0]

        # Greedily assign the largest file to the smallest partition
        for current in namesize:
            #print "putting", current, "into partition", min_idx
            partitions[min_idx].append(current)
            sums[min_idx] += current.size
            min_idx = find_min_idx(sums)

        # And now do the same with the remote files.
        for r in remote_files:
            size = r.size
            dims = self._input_filter.get_dimensions(".", r.name)
            remote = self.MapperInput(
                remote=True,
                name=r.name,
                size=size,
                dimensions=dims
            )
            #print "putting", remote, "into partition", min_idx
            partitions[min_idx].append(remote)
            sums[min_idx] += size
            min_idx = find_min_idx(sums)

        # Print out some info to see how balanced the partitions were:
        self.dump_stats(sums)
        return partitions

    def get_filtered_files(self, searchdir):
        level_offset = searchdir.count(os.path.sep)
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
                    yield full_filename

    def get_filtered_files_s3(self):
        if not self._local_only:
            print "Fetching file list from S3..."
            # Plain boto should be fast enough to list bucket contents.
            if self._aws_key is not None:
                conn = S3Connection(self._aws_key, self._aws_secret_key)
            else:
                conn = S3Connection()
            bucket = conn.get_bucket(self._bucket_name)
            start = datetime.now()
            count = 0
            # Filter input files by partition. If the filter is reasonably
            # selective, this can be much faster than listing all files in the
            # bucket.
            for f in s3util.list_partitions(bucket, schema=self._input_filter, include_keys=True):
                count += 1
                if count == 1 or count % 1000 == 0:
                    print "Listed", count, "so far"
                yield f
            conn.close()
            duration = timer.delta_sec(start)
            print "Listed", count, "files in", duration, "seconds"

    def filter_includes(self, level, value):
        allowed_values = self._allowed_values[level]
        return self._input_filter.is_allowed(value, allowed_values)


class Context:
    def __init__(self, out, partition_count):
        self._basename = out
        self._partition_count = partition_count
        self._sinks = {}
        # Pre-open all the files to make sure they exist for the reducer. This
        # takes care of the situation where we don't get a key value hashing
        # to a particular partition.
        for i in range(partition_count):
            self._sinks[i] = open("%s_%d" % (self._basename, i), "wb")

    def partition(self, key):
        #print "hash of", key, "is", hash(key) % self._partition_count
        return hash(key) % self._partition_count

    def write(self, key, value):
        p = self.partition(key)
        out = self._sinks[p]
        marshal.dump((key, value), out)

    def finish(self):
        for s in self._sinks.itervalues():
            s.close()


class TextContext(Context):
    def __init__(self, out, field_separator="\t", record_separator="\n"):
        self._sink = open(out, "w")
        self._sinks = {0: self._sink}
        self.field_separator = field_separator
        self.record_separator = record_separator

    def write(self, key, value):
        self._sink.write(str(key))
        self._sink.write(self.field_separator)
        self._sink.write(str(value))
        self._sink.write(self.record_separator)

    def writecsv(self, values):
        w = csv.writer(self._sink)
        w.writerow(values)

    def writeline(self, value):
        self._sink.write(value)
        self._sink.write(self.record_separator)

class Mapper:
    def __init__(self, mapper_id, do_profile, inputs, work_dir, module, partition_count, delete_files):
        if do_profile:
            profile_out = os.path.join(work_dir, "profile_mapper_" + str(mapper_id))
            pr = cProfile.Profile()
            pr.enable()

        self.run_mapper(mapper_id, inputs, work_dir, module, partition_count, delete_files)

        if do_profile:
            pr.disable()
            pr.dump_stats(profile_out)

    def run_mapper(self, mapper_id, inputs, work_dir, module, partition_count, delete_files):
        self.work_dir = work_dir

        print "I am mapper", mapper_id, ", and I'm mapping", len(inputs), "inputs"
        output_file = os.path.join(work_dir, "mapper_" + str(mapper_id))
        mapfunc = getattr(module, 'map', None)
        context = Context(output_file, partition_count)
        if not callable(mapfunc):
            print "No map function!!!"
            sys.exit(1)

        # TODO: Stream/decompress the files directly.
        for input_file in inputs:
            try:
                handle = self.open_input_file(input_file)
            except:
                print "Error opening", input_file.name, "(skipping)"
                traceback.print_exc(file=sys.stderr)
                continue
            line_num = 0
            for line in handle:
                line_num += 1
                try:
                    # Remove the trailing EOL character(s) before passing to
                    # the map function.
                    key, value = line.rstrip('\r\n').split("\t", 1)
                    mapfunc(key, input_file.dimensions, value, context)
                except ValueError, e:
                    # TODO: increment "bad line" metrics.
                    print "Bad line:", input_file.name, ":", line_num, e
            handle.close()
            if delete_files:
                print "Removing", input_file.name
                os.remove(handle.filename)
        context.finish()

    def open_input_file(self, input_file):
        filename = input_file.name
        if input_file.remote:
            # Read so-called remote files from the local cache. Go on the
            # assumption that they have already been downloaded.
            filename = os.path.join(self.work_dir, "cache", input_file.name)
        return CompressedFile(filename)


class Collector(dict):
    def __init__(self, combine_func=None, combine_size=50):
        if callable(combine_func):
            self.combine = combine_func
        else:
            self.combine = self.dummy_combine
        self.combine_size = combine_size

    def write(self, key, value):
        self.__setitem__(key, [value])

    def collect(self, key, value):
        values = []
        if key in self:
            values = self.__getitem__(key)
        values.append(value)
        self.__setitem__(key, values)
        if len(values) >= self.combine_size:
            self.combine(key, values, self)

    def dummy_combine(self, k, v, cx):
        pass


class Reducer:
    def __init__(self, reducer_id, do_profile, work_dir, module, mapper_count):
        if do_profile:
            profile_out = os.path.join(work_dir, "profile_reducer_" + str(reducer_id))
            pr = cProfile.Profile()
            pr.enable()

        self.run_reducer(reducer_id, work_dir, module, mapper_count)

        if do_profile:
            pr.disable()
            pr.dump_stats(profile_out)

    COMBINE_SIZE = 50
    def run_reducer(self, reducer_id, work_dir, module, mapper_count):
        #print "I am reducer", reducer_id, ", and I'm reducing", mapper_count, "mapped files"
        output_file = os.path.join(work_dir, "reducer_" + str(reducer_id))
        context = TextContext(output_file)
        reducefunc = getattr(module, 'reduce', None)
        combinefunc = getattr(module, 'combine', None)
        setupreducefunc = getattr(module, 'setup_reduce', None)
        if callable(setupreducefunc):
            setupreducefunc(context)

        map_only = False
        if not callable(reducefunc):
            print "No reduce function (that's ok). Writing out all the data."
            map_only = True

        collected = Collector(combinefunc, Reducer.COMBINE_SIZE)
        for i in range(mapper_count):
            mapper_file = os.path.join(work_dir, "mapper_%d_%d" % (i, reducer_id))
            # read, group by key, call reducefunc, output
            input_fd = open(mapper_file, "rb")
            try:
                while True:
                    key, value = marshal.load(input_fd)
                    if map_only:
                        # Just write out each row as we see it
                        context.write(key, value)
                    else:
                        collected.collect(key, value)
            except EOFError:
                input_fd.close()
        if not map_only:
            # invoke the reduce function on each combined output.
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
    parser.add_argument("-k", "--aws-key", help="AWS Key", default=None)
    parser.add_argument("-s", "--aws-secret-key", help="AWS Secret Key", default=None)
    parser.add_argument("-w", "--work-dir", help="Location to put temporary work files", default="/tmp/telemetry_mr")
    parser.add_argument("-o", "--output", help="Filename to use for final job output", required=True)
    #TODO: make the input filter optional, default to "everything valid" and generate dims intelligently.
    parser.add_argument("-f", "--input-filter", help="File containing filter spec", required=True)
    parser.add_argument("-v", "--verbose", help="Print verbose output", action="store_true")
    parser.add_argument("-X", "--delete-data", help="Delete raw data files after mapping", action="store_true")
    parser.add_argument("-p", "--profile", help="Profile mappers and reducers using cProfile", action="store_true")
    args = parser.parse_args()

    if not args.local_only:
        if not BOTO_AVAILABLE:
            print "ERROR: The 'boto' library is required except in 'local-only' mode."
            print "       You can install it using `sudo pip install boto`"
            parser.print_help()
            return -2
        # If we want to process remote data, some more arguments are required.
        for remote_req in ["bucket"]:
            if not hasattr(args, remote_req) or getattr(args, remote_req) is None:
                print "ERROR:", remote_req, "is a required option"
                parser.print_help()
                return -1

    args = args.__dict__
    job = Job(args)
    start = datetime.now()
    exit_code = 0
    try:
        job.mapreduce()
    except:
        traceback.print_exc(file=sys.stderr)
        exit_code = 2
    duration = timer.delta_sec(start)
    print "All done in %.2fs" % (duration)
    return exit_code

if __name__ == "__main__":
    sys.exit(main())
