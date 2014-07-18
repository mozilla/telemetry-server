#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import argparse
import logging
import multiprocessing
import io
import os
import Queue as Q
import re
import signal
import simplejson as json
import subprocess
import sys
import time
import traceback
import uuid

from collections import defaultdict
from datetime import date, datetime
from multiprocessing import Process, Queue
from subprocess import Popen

import boto.sqs
from boto.exception import S3ResponseError
from boto.s3.connection import S3Connection

from telemetry.convert import Converter, BadPayloadError
from telemetry.persist import StorageLayout
from telemetry.revision_cache import RevisionCache
from telemetry.telemetry_schema import TelemetrySchema
import telemetry.util.timer as timer
import telemetry.util.files as fileutil
import telemetry.util.s3 as s3util

# Wait for processes to complete on their own
def wait_for(logger, processes, label):
    logger.log("Waiting for {0}...".format(label))
    for p in processes:
        logger.log("Joining pid {0} (alive: {1})".format(p.pid, p.is_alive()))
        p.join()
    logger.log("{0} Done.".format(label))

# Force processes to stop immediately
def terminate(logger, processes, label):
    logger.log("Terminating {0}...".format(label))
    for p in processes:
        p.terminate()
    logger.log("{0} Done.".format(label))

# Start the requested number of worker processes
def start_workers(logger, count, name, clazz, q_in, more_args):
    logger.log("Starting {0}s...".format(name))
    workers = []
    for i in range(count):
        w = Process(
                target=clazz,
                name="{0}-{1}".format(name, i),
                args=(i, name, q_in) + more_args)
        workers.append(w)
        w.start()
        logger.log("{0} {1} pid: {2}".format(name, i, w.pid))
    logger.log("{0}s all started".format(name))
    return workers

# Insert the required number of sentinel values to signal subprocesses that
# there is no more data.
def finish_queue(queue, num_procs):
    for i in range(num_procs):
        queue.put(PipeStep.SENTINEL)

# Convert a datetime object to a standard JSON date string.
def datetime_to_json(d):
    return d.isoformat() + "Z"

# Use UTC dates throughout for consistency.
now = datetime.utcnow

# Convert a timestamp (in milliseconds) to a YYYYMMDD string
def ts_to_yyyymmdd(ts):
    # Incoming timestamps are in milliseconds, so convert to POSIX first
    # (ie. seconds)
    return date.fromtimestamp(ts / 1000).strftime("%Y%m%d")

class InterruptProcessingError(Exception):
    def __init__(self, msg):
        self.msg = msg

# Convert OS Signal to an exception
def handle_sigint(signum, frame):
    print "Caught signal", str(signum), "in pid", os.getpid()
    if signum == signal.SIGINT:
        raise InterruptProcessingError("It's quittin' time")

# Output labeled log messages
class Log(object):
    def __init__(self, log_file, label=None):
        self.log_file = log_file
        if label is None:
            self.label = "PID {0}".format(os.getpid())
        else:
            self.label = "{0} (PID {1})".format(label, os.getpid())

    def log(self, message):
        if self.log_file is None:
            # log to stdout
            print self.label, message
        else:
            with io.open(self.log_file, "a") as fout:
                fout.write(u"{0}: {1}\n".format(self.label, message))

# Keep track of a task's progress and save stats for external monitoring
class Stats(object):
    def __init__(self, task, stats_file, logger=None):
        self.task = task
        self.stats_file = stats_file
        self.logger = logger
        self.reset()

    def save_map(self, channel_name, chan_stats):
        if self.stats_file is None:
            return;

        chan_stats["task"] = self.task
        chan_stats["channel"] = channel_name
        chan_stats["start_time"] = datetime_to_json(self.start_time)
        chan_stats["end_time"] = datetime_to_json(self.end_time)
        chan_stats["duration"] = timer.delta_sec(self.start_time, self.end_time)
        try:
            with io.open(self.stats_file, "a") as fout:
                fout.write(unicode(json.dumps(chan_stats) + u"\n"))
        except:
            self.logger.log("Error writing '{}' stats".format(channel_name))
            self.logger.log(traceback.format_exc())

    def save(self):
        self.save_map("ALL", self.overall)
        for channel, channel_stats in self.by_channel.iteritems():
            self.save_map(channel, channel_stats)

    def reset(self):
        self.overall = defaultdict(int)
        self.by_channel = {}
        self.start_time = now()
        self.end_time = now()

    def update_end_time(self):
        self.end_time = now()

    def get_summary(self):
        duration = timer.delta_sec(self.start_time, self.end_time)
        read_rate = self.overall["records_read"] / duration
        mb_read = self.overall["bytes_read"] / 1024.0 / 1024.0
        mb_read_rate = mb_read / duration
        write_rate = self.overall["records_written"] / duration
        mb_written = self.overall["bytes_written"] / 1024.0 / 1024.0
        mb_write_rate = mb_written / duration
        summary = "Read %d records or %.2fMB (%.2fr/s, %.2fMB/s), " \
                  "wrote %d or %.2f MB (%.2fr/s, %.2fMB/s). " \
                  "Found %d bad records" % (self.overall["records_read"],
                    mb_read, read_rate, mb_read_rate,
                    self.overall["records_written"], mb_written, write_rate,
                    mb_write_rate, self.overall["bad_records"])
        return summary

    def increment_map(self, the_map, records_read=0, records_written=0,
            bytes_read=0, bytes_uncompressed=0, bytes_written=0, bad_records=0,
            bad_record_type=None):
        the_map["records_read"] += records_read
        the_map["records_written"] += records_written
        the_map["bytes_read"] += bytes_read
        the_map["bytes_uncompressed"] += bytes_uncompressed
        the_map["bytes_written"] += bytes_written
        the_map["bad_records"] += bad_records
        if bad_record_type is not None and bad_records > 0:
            br_key = "bad_records.{}".format(bad_record_type)
            the_map[br_key] += bad_records

    def increment(self, channel=None, records_read=0, records_written=0,
            bytes_read=0, bytes_uncompressed=0, bytes_written=0, bad_records=0,
            bad_record_type=None, update_end_time=True):
        self.increment_map(self.overall, records_read, records_written,
                bytes_read, bytes_uncompressed, bytes_written, bad_records,
                bad_record_type)
        if channel is not None:
            # also record the stats for the channel
            cs = self.by_channel.get(channel, defaultdict(int))
            self.increment_map(cs, records_read, records_written, bytes_read,
                    bytes_uncompressed, bytes_written, bad_records,
                    bad_record_type)
            self.by_channel[channel] = cs
        if update_end_time:
            self.update_end_time()


# Base class for pipline workers
class PipeStep(object):
    SENTINEL = 'STOP'
    def __init__(self, num, name, q_in, q_out=None, log_file=None,
            stats_file=None):
        self.print_stats = True
        self.num = num
        self.label = " ".join((name, str(num)))
        self.q_in = q_in
        self.q_out = q_out
        self.log_file = log_file
        self.logger = Log(log_file, self.label)
        self.stats = Stats(name, stats_file, self.logger)
        self.last_update = now()
        self.log = self.logger.log

        # Do stuff.
        self.setup()
        self.work()
        self.finish()

    def setup(self):
        pass

    def finish(self):
        self.log("All done")
        self.log(self.stats.get_summary())

    def handle(self, record):
        pass

    def work(self):
        self.log("Starting up")
        while True:
            try:
                raw = self.q_in.get()
                if raw == PipeStep.SENTINEL:
                    break
                self.stats.reset()
                self.handle(raw)
                self.stats.update_end_time()
                self.stats.save()
                if self.print_stats:
                    this_update = now()
                    if timer.delta_sec(self.last_update, this_update) > 10.0:
                        self.last_update = this_update
                        self.log(self.stats.get_summary())
            except Q.Empty:
                break
        self.log("Received stop message... work done")


# Read from raw input files, validate and convert data, save output to disk
class ReadRawStep(PipeStep):
    def __init__(self, num, name, raw_files, completed_files, log_file,
            stats_file, schema, converter, storage, bad_filename):
        self.schema = schema
        self.converter = converter
        self.storage = storage
        self.bad_filename = bad_filename
        PipeStep.__init__(self, num, name, raw_files, completed_files,
                log_file, stats_file)

    def setup(self):
        self.expected_dim_count = len(self.schema._dimensions)

    def handle(self, raw_file):
        self.log("Reading " + raw_file)
        try:
            record_count = 0
            bytes_read = 0
            start = now()
            file_version = fileutil.detect_file_version(raw_file, simple_detection=True)
            self.log("Detected version {0} for file {1}".format(file_version,
                     raw_file))
            for unpacked in fileutil.unpack(raw_file, file_version=file_version):
                record_count += 1
                common_bytes = unpacked.len_path + fileutil.RECORD_PREAMBLE_LENGTH[file_version]
                current_bytes = common_bytes + unpacked.len_data
                current_bytes_uncompressed = common_bytes + len(unpacked.data)
                bytes_read += current_bytes
                if unpacked.error:
                    self.log("ERROR: Found corrupted data for record {0} in " \
                             "{1} path: {2} Error: {3}".format(record_count,
                                 raw_file, unpacked.path, unpacked.error))
                    self.stats.increment(records_read=1,
                            bytes_read=current_bytes,
                            bytes_uncompressed=current_bytes_uncompressed,
                            bad_records=1, bad_record_type="corrupted_data")
                    continue
                if len(unpacked.data) == 0:
                    self.log("WARN: Found empty data for record {0} in " \
                             "{2} path: {2}".format(record_count, raw_file,
                                 unpacked.path))
                    self.stats.increment(records_read=1,
                            bytes_read=current_bytes,
                            bytes_uncompressed=current_bytes_uncompressed,
                            bad_records=1, bad_record_type="empty_data")
                    continue

                submission_date = ts_to_yyyymmdd(unpacked.timestamp)
                path = unicode(unpacked.path, errors="replace")
                #self.log("Path was: {}".format(path))

                if unpacked.data[0] != "{":
                    # Data looks weird, should be JSON.
                    self.log(u"Warning: Found unexpected data for record {0}" \
                             u" in {1} path: {2} data:\n{3}".format(record_count,
                                 raw_file, path, unicode(unpacked.data, errors="replace")))
                else:
                    # Raw JSON, make sure we treat it as unicode.
                    unpacked.data = unicode(unpacked.data, errors="replace")

                dims = [submission_date]
                data_version = 1
                serialized_data = unpacked.data

                self.stats.increment(records_read=1, bytes_read=current_bytes,
                        bytes_uncompressed=current_bytes_uncompressed)

                # TODO: Validate JSON here if need be
                try:
                    # Write to persistent storage
                    n = self.storage.write(path, serialized_data, dims,
                                           data_version)
                    self.stats.increment(records_written=1,
                        bytes_written=len(path) + len(serialized_data) + 2)
                    # Compress rotated files as we generate them
                    if n.endswith(StorageLayout.PENDING_COMPRESSION_SUFFIX):
                        self.q_out.put(n)
                except Exception, e:
                    self.write_bad_record(path, dims, serialized_data,
                            str(e), "ERROR Writing to output file:",
                            "write_failed")
                    self.log(traceback.format_exc())

                if self.print_stats:
                    this_update = now()
                    sec = timer.delta_sec(self.last_update, this_update)
                    if sec > 10.0:
                        self.last_update = this_update
                        self.log(self.stats.get_summary())

            duration = timer.delta_sec(start, now())
            mb_read = bytes_read / 1024.0 / 1024.0
            # Stats for the current file:
            self.log("Read %d records %.2fMB in %.2fs (%.2fMB/s)" % (
                    record_count, mb_read, duration, mb_read / duration))
        except Exception, e:
            # Corrupted data, let's skip this record.
            self.log("Error reading raw data from {0} {1}\n{2}".format(
                    raw_file, e, traceback.format_exc()))


    def write_bad_record(self, key, dims, data, error, message=None,
            bad_record_type=None):
        try:
            channel = self.schema.get_field(dims, "appUpdateChannel", True,
                    True)
        except ValueError, e:
            channel = "UNKNOWN"
        self.stats.increment(channel=channel, bad_records=1,
                bad_record_type=bad_record_type)
        if message is not None:
            self.log("{0} - {1}".format(message, error))
        if self.bad_filename is not None:
            try:
                path = u"/".join([key] + dims)
                self.storage.write_filename(path, data, self.bad_filename)
            except Exception, e:
                self.log("ERROR: {0}".format(e))


# Compress completed output files from ReadRawStep
class CompressCompletedStep(PipeStep):
    def setup(self):
        self.compress_cmd = [StorageLayout.COMPRESS_PATH] + StorageLayout.COMPRESSION_ARGS

    def handle(self, record):
        filename = record
        base_ends = filename.find(".log") + 4
        if base_ends < 4:
            self.log("Bad filename encountered, skipping: " + filename)
            self.stats.increment(records_read=1, bad_records=1,
                    bad_record_type="bad_filename")
            return
        basename = filename[0:base_ends]
        # Get a unique name for the compressed file:
        comp_name = basename + "." + uuid.uuid4().hex + StorageLayout.COMPRESSED_SUFFIX

        # reserve it!
        f_comp = open(comp_name, "wb")
        # TODO: open f_comp with same buffer size as below?

        # Rename uncompressed file to a temp name
        tmp_name = comp_name + ".compressing"
        os.rename(filename, tmp_name)

        # Read input file as text (line-buffered)
        f_raw = open(tmp_name, "r", 1)
        start = now()

        # Now set up our processing pipe:
        # - read from f_raw, compress, write to comp_name
        p_compress = Popen(self.compress_cmd, bufsize=65536, stdin=f_raw,
                stdout=f_comp, stderr=sys.stderr)

        # Note: it looks like p_compress.wait() is what we want, but the docs
        #       warn of a deadlock, so we use communicate() instead.
        p_compress.communicate()

        raw_bytes = f_raw.tell()
        comp_bytes = f_comp.tell()
        raw_mb = float(raw_bytes) / 1024.0 / 1024.0
        comp_mb = float(comp_bytes) / 1024.0 / 1024.0
        f_raw.close()
        f_comp.close()

        self.stats.increment(records_read=1, records_written=1,
                bytes_read=raw_bytes, bytes_written=comp_bytes)

        # Remove raw file
        os.remove(tmp_name)
        sec = timer.delta_sec(start, now())
        self.log("Compressed %s as %s in %.2fs. Size before: %.2fMB, after:" \
                 " %.2fMB (r: %.2fMB/s, w: %.2fMB/s)" % (filename, comp_name,
                    sec, raw_mb, comp_mb, (raw_mb/sec), (comp_mb/sec)))


# Export compressed output files to S3 for long-term storage and analysis
class ExportCompressedStep(PipeStep):
    def __init__(self, num, name, q_in, log_file, stats_file,
            base_dir, config, dry_run):
        self.dry_run = dry_run
        self.base_dir = base_dir
        self.aws_key = config.get("aws_key", None)
        self.aws_secret_key = config.get("aws_secret_key", None)
        self.aws_bucket_name = config["publish_bucket"]
        PipeStep.__init__(self, num, name, q_in, log_file=log_file,
                stats_file=stats_file)

    def setup(self):
        if self.dry_run:
            self.conn = None
            self.bucket = None
            return
        self.conn = S3Connection(self.aws_key, self.aws_secret_key)
        self.bucket = self.conn.get_bucket(self.aws_bucket_name)

    def strip_data_dir(self, data_dir, full_file):
        if full_file.startswith(data_dir):
            chopped = full_file[len(data_dir):]
            if chopped[0] == "/":
                chopped = chopped[1:]
            return chopped
        else:
            raise ValueError("Invalid full filename: " + str(full_file))

    def handle(self, record):
        try:
            # Remove the output dir prefix from filenames
            stripped_name = self.strip_data_dir(self.base_dir, record)
        except ValueError, e:
            self.log("Warning: couldn't strip base dir from '{0}' " \
                     "{1}".format(record, e))
            stripped_name = record

        self.log("Uploading {0}".format(stripped_name))
        start = now()
        if self.dry_run:
            local_filename = record
            remote_filename = stripped_name
            err = None
        else:
            local_filename, remote_filename, err = s3util.upload_one([
                    self.base_dir, self.bucket, stripped_name])
        sec = timer.delta_sec(start, now())
        current_size = os.path.getsize(record)
        self.stats.increment(records_read=1, bytes_read=current_size)
        if err is None:
            # Everything went well.
            self.stats.increment(records_written=1, bytes_written=current_size)
            # Delete local files once they've been uploaded successfully.
            if not self.dry_run:
                try:
                    os.remove(record)
                    self.log("Removed uploaded file {0}".format(record))
                except Exception, e:
                    self.log("Failed to remove uploaded file {0}: " \
                             "{1}".format(record, e))
        else:
            self.log("ERROR: failed to upload a file {0}: {1}".format(record,
                    err))
            self.stats.increment(bad_records=1)
            # TODO: add to a "failures" queue, save them or something?


def main():
    signal.signal(signal.SIGINT, handle_sigint)
    parser = argparse.ArgumentParser(
            description='Process incoming Telemetry data',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-c", "--config", required=True, type=file,
            help="AWS Configuration file (json)")
    parser.add_argument("-w", "--work-dir", required=True,
            help="Location to cache downloaded files")
    parser.add_argument("-o", "--output-dir", required=True,
            help="Base dir to store processed data")
    parser.add_argument("-i", "--input-files", type=file,
            help="File containing a list of keys to process")
    parser.add_argument("-b", "--bad-data-log",
            help="Save bad records to this file")
    parser.add_argument("-l", "--log-file",
            help="Log output to this file")
    parser.add_argument("-s", "--stats-file",
            help="Log statistics to this file")
    parser.add_argument("-t", "--telemetry-schema", required=True,
            help="Location of the desired telemetry schema")
    parser.add_argument("-m", "--max-output-size", metavar="N", type=int,
            default=500000000, help="Rotate output files after N bytes")
    parser.add_argument("-D", "--dry-run", action="store_true",
            help="Don't modify remote files")
    parser.add_argument("-n", "--no-clean", action="store_true",
            help="Don't clean out the output-dir before beginning")
    parser.add_argument("-v", "--verbose", action="store_true",
            help="Print more detailed output")
    args = parser.parse_args()

    if args.verbose:
        # Turn on mp logging
        multiprocessing.log_to_stderr(logging.DEBUG)

    config = json.load(args.config)
    # TODO: allow commandline args to override config values.

    if not os.path.isdir(args.output_dir):
        os.makedirs(args.output_dir)

    schema_data = open(args.telemetry_schema)
    schema = TelemetrySchema(json.load(schema_data))
    schema_data.close()
    storage = StorageLayout(schema, args.output_dir, args.max_output_size)
    logger = Log(args.log_file, "Master")
    num_cpus = multiprocessing.cpu_count()
    conn = None
    incoming_bucket = None
    incoming_queue = None
    s3downloader = None
    raw_readers = None
    compressors = None
    exporters = None
    done = False

    if args.no_clean:
        logger.log("Not removing log files in {}".format(args.output_dir))
    else:
        # Remove existing log files from output_dir (to clean up after an
        # incomplete previous run, for example).
        logger.log("Removing log files in {}".format(args.output_dir))
        for root, dirs, files in os.walk(args.output_dir):
            for f in files:
                if f.endswith(".log"):
                    full = os.path.join(root, f)
                    if args.dry_run:
                        logger.log("Would be deleting {}, except it's a " \
                                   "dry run".format(full))
                    else:
                        try:
                            logger.log("Removing existing file: " + full)
                            os.remove(full)
                        except Exception, e:
                            logger.log("Error removing existing " \
                                       " file {}: {}".format(full, e))

    if not args.dry_run:
        # Set up AWS connections
        conn = S3Connection(config.get("aws_key", None), config.get(
                "aws_secret_key", None))
        incoming_bucket = conn.get_bucket(config["incoming_bucket"])
        q_conn = boto.sqs.connect_to_region(config.get("aws_region", None),
                aws_access_key_id=config.get("aws_key", None),
                aws_secret_access_key=config.get("aws_secret_key", None))
        incoming_queue = q_conn.get_queue(config["incoming_queue"])
        if incoming_queue is None:
            logger.log("Error: could not get queue " + config["incoming_queue"])
            return -2

        logger.log("Verifying that we can write to " + config["publish_bucket"])
        try:
            publish_bucket = conn.get_bucket(config["publish_bucket"])
            logger.log("Looks good!")
        except S3ResponseError:
            logger.log("Bucket {0} not found. Attempting to create it.".format(
                    config["publish_bucket"]))
            publish_bucket = conn.create_bucket(config["publish_bucket"])
        s3downloader = s3util.Loader(args.work_dir, config["incoming_bucket"],
                poolsize=num_cpus, aws_key=config.get("aws_key", None),
                aws_secret_key=config.get("aws_secret_key", None))

    while not done:
        if args.dry_run:
            done = True
        try:
            start = now()
            incoming_filenames = []
            incoming_queue_messages = []
            logger.log("Fetching file list from queue " + config["incoming_queue"])
            if args.dry_run:
                logger.log("Dry run mode... can't read from the queue " \
                           "without messing things up...")
                if args.input_files:
                    logger.log("Fetching file list from file {}".format(
                            args.input_files))
                    incoming_filenames = [ l.strip() for l in args.input_files.readlines() ]
            else:
                # Sometimes we don't get all the messages, even if more are
                # available, so keep trying until we have enough (or there
                # aren't any left)
                for i in range(num_cpus):
                    messages = incoming_queue.get_messages(num_cpus - len(incoming_filenames))
                    for m in messages:
                        # Make sure this file exists in S3 first
                        possible_filename = m.get_body()
                        key = incoming_bucket.get_key(possible_filename)
                        if key is None:
                            logger.log("Could not find queued filename in" \
                                       " bucket {0}: {1}".format(
                                            config["incoming_bucket"],
                                            possible_filename))
                            # try to delete it:
                            incoming_queue.delete_message(m)
                        else:
                            incoming_filenames.append(possible_filename)
                            incoming_queue_messages.append(m)
                    if len(messages) == 0 or len(incoming_filenames) >= num_cpus:
                        break
            logger.log("Done")

            if len(incoming_filenames) == 0:
                logger.log("Nothing to do! Sleeping...")
                time.sleep(5)
                continue

            for f in incoming_filenames:
                logger.log("  " + f)

            before_download = now()
            logger.log("Downloading {0} files...".format(len(incoming_filenames)))
            local_filenames = []
            download_stats = Stats("Downloader", args.stats_file, logger)
            if args.dry_run:
                logger.log("Dry run mode: skipping download from S3")
                local_filenames = [ os.path.join(args.work_dir, f) for f in incoming_filenames ]
            else:
                for local_filename, remote_filename, err in s3downloader.get_list(incoming_filenames):
                    if err is None:
                        local_filenames.append(local_filename)
                    else:
                        # s3downloader already retries 3 times.
                        logger.log("Error downloading {0} Error: {1}".format(
                                local_filename, err))
                        download_stats.increment(
                                records_read=len(incoming_filenames),
                                records_written=len(local_filenames),
                                bad_records=1)
                        download_stats.save()
                        return 2
            downloaded_bytes = sum([os.path.getsize(f) for f in local_filenames])
            download_stats.increment(records_read=len(incoming_filenames),
                    records_written=len(local_filenames),
                    bytes_read=downloaded_bytes,
                    bytes_written=downloaded_bytes)
            logger.log(download_stats.get_summary())
            download_stats.save()
            after_download = now()

            raw_files = Queue()
            for l in local_filenames:
                raw_files.put(l)

            completed_files = Queue()

            # Begin reading raw input
            raw_readers = start_workers(logger, num_cpus, "Reader", ReadRawStep,
                    raw_files, (completed_files, args.log_file, args.stats_file,
                    schema, None, storage, args.bad_data_log))

            # Tell readers to stop when they get to the end:
            finish_queue(raw_files, num_cpus)

            # Compress completed files.
            compressors = start_workers(logger, num_cpus, "Compressor",
                    CompressCompletedStep, completed_files, (None,
                    args.log_file, args.stats_file))
            wait_for(logger, raw_readers, "Raw Readers")

            # `find <out_dir> -type f -not -name ".compressme"`
            # Add them to completed_files
            for root, dirs, files in os.walk(args.output_dir):
                for f in files:
                    if f.endswith(".log"):
                        completed_files.put(os.path.join(root, f))

            # Tell compressors to stop:
            finish_queue(completed_files, num_cpus)
            wait_for(logger, compressors, "Compressors")

            shutdown_requested = False
            try:
                # Export compressed files to S3.
                compressed_files = Queue()
                exporters = start_workers(logger, num_cpus, "Exporter",
                        ExportCompressedStep, compressed_files, (args.log_file,
                        args.stats_file, args.output_dir, config, args.dry_run))
                for root, dirs, files in os.walk(args.output_dir):
                    for f in files:
                        if f.endswith(StorageLayout.COMPRESSED_SUFFIX):
                            compressed_files.put(os.path.join(root, f))
                finish_queue(compressed_files, num_cpus)
                wait_for(logger, exporters, "Exporters")
            except InterruptProcessingError, e:
                logger.log("Received shutdown request... waiting for " \
                           "exporters to finish")
                shutdown_requested = True
                shutdown_stats = Stats("ShutdownDuringExport", args.stats_file,
                    logger)
                shutdown_stats.increment(records_read=1)
                shutdown_stats.save()
                done = True
                wait_for(logger, exporters, "Exporters")
                logger.log("OK, cleaning up")

            logger.log("Removing processed logs from S3...")
            for f in incoming_filenames:
                if args.dry_run:
                    logger.log("  Dry run, so not really deleting " + f)
                else:
                    logger.log("  Deleting " + f)
                    incoming_bucket.delete_key(f)
                    # Delete file locally too.
                    os.remove(os.path.join(args.work_dir, f))
            logger.log("Done")

            if len(incoming_queue_messages) > 0:
                logger.log("Removing processed messages from SQS...")
                for m in incoming_queue_messages:
                    if args.dry_run:
                        logger.log("  Dry run, so not really deleting " \
                                   "{0}".format(m.get_body()))
                    else:
                        logger.log("  Deleting {0}".format(m.get_body()))
                        if incoming_queue.delete_message(m):
                            logger.log("  Message deleted successfully")
                        else:
                            logger.log("  Failed to delete message :(")
                logger.log("Done")

            if shutdown_requested:
                shutdown_stats.increment(records_written=1)
                shutdown_stats.save()
            all_done = now()
            duration = timer.delta_sec(start, all_done)
            logger.log("All done in %.2fs (%.2fs excluding download time)" % (
                duration, timer.delta_sec(after_download, all_done)))
        except InterruptProcessingError, e:
            logger.log("Received normal shutdown request... quittin' time!")
            if raw_readers is not None:
                terminate(logger, raw_readers, "Readers")
            if compressors is not None:
                terminate(logger, compressors, "Compressors")
            if exporters is not None:
                terminate(logger, exporters, "Exporters")

            done = True
    shutdown_stats = Stats("ShutdownComplete", args.stats_file, logger)
    shutdown_stats.increment(records_read=1, records_written=1)
    shutdown_stats.save()
    logger.log("All done.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
