#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import argparse
import uuid
import multiprocessing
import logging
from multiprocessing import Process, Queue
import Queue as Q
import simplejson as json
import sys
import io
import os
import time
from datetime import date, datetime
from telemetry.telemetry_schema import TelemetrySchema
import subprocess
from subprocess import Popen
from boto.s3.connection import S3Connection
from boto.exception import S3ResponseError
import telemetry.util.timer as timer
import telemetry.util.files as fileutil
import telemetry.util.s3 as s3util
from telemetry.convert import Converter, BadPayloadError
from telemetry.revision_cache import RevisionCache
from telemetry.persist import StorageLayout
import boto.sqs
import traceback
import signal

def wait_for(logger, processes, label):
    logger.log("Waiting for {0}...".format(label))
    for p in processes:
        logger.log("Joining pid {0} (alive: {1})...".format(p.pid, p.is_alive()))
        p.join()
    logger.log("{0} Done.".format(label))

def terminate(logger, processes, label):
    logger.log("Terminating {0}...".format(label))
    for p in processes:
        p.terminate()
    logger.log("{0} Done.".format(label))

def finish_queue(queue, num_procs):
    for i in range(num_procs):
        queue.put(PipeStep.SENTINEL)
    queue.close()

class InterruptProcessingError(Exception):
    def __init__(self, msg):
        self.msg = msg

def handle_sigint(signum, frame):
    print "Caught signal", str(signum), "in pid", os.getpid()
    if signum == signal.SIGINT:
        raise InterruptProcessingError("It's quittin' time")

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


class PipeStep(object):
    SENTINEL = 'STOP'
    def __init__(self, num, name, q_in, q_out=None, log_file=None):
        self.print_stats = True
        self.num = num
        self.label = " ".join((name, str(num)))
        self.q_in = q_in
        self.q_out = q_out
        self.log_file = log_file
        self.start_time = datetime.now()
        self.end_time = datetime.now()
        self.last_update = datetime.now()
        self.bad_records = 0
        self.records_read = 0
        self.records_written = 0
        self.bytes_read = 0
        self.bytes_written = 0
        self.logger = Log(log_file, self.label)
        self.log = self.logger.log

        # Do stuff.
        self.setup()
        self.work()
        self.finish()

    def setup(self):
        pass

    def dump_stats(self):
        duration = timer.delta_sec(self.start_time, self.end_time)
        read_rate = self.records_read / duration
        mb_read = self.bytes_read / 1024.0 / 1024.0
        mb_read_rate = mb_read / duration
        write_rate = self.records_written / duration
        mb_written = self.bytes_written / 1024.0 / 1024.0
        mb_write_rate = mb_written / duration
        self.log("Read %d records or %.2fMB (%.2fr/s, %.2fMB/s), wrote %d or %.2f MB (%.2fr/s, %.2fMB/s). Found %d bad records" % (self.records_read, mb_read, read_rate, mb_read_rate, self.records_written, mb_written, write_rate, mb_write_rate, self.bad_records))

    def finish(self):
        self.log("All done")
        self.dump_stats()

    def handle(self, record):
        pass

    def work(self):
        self.log("Starting up")
        while True:
            try:
                raw = self.q_in.get()
                if raw == PipeStep.SENTINEL:
                    break
                self.handle(raw)
                self.records_read += 1
                if self.print_stats:
                    this_update = datetime.now()
                    if timer.delta_sec(self.last_update, this_update) > 10.0:
                        self.last_update = this_update
                        self.dump_stats()
                self.end_time = datetime.now()
            except Q.Empty:
                break
        self.log("Received stop message... work done")

class ReadRawStep(PipeStep):
    def __init__(self, num, name, raw_files, completed_files, log_file, schema, converter, storage, bad_filename):
        self.schema = schema
        self.converter = converter
        self.storage = storage
        self.bad_filename = bad_filename
        PipeStep.__init__(self, num, name, raw_files, completed_files, log_file)

    def setup(self):
        self.expected_dim_count = len(self.schema._dimensions)

    def handle(self, raw_file):
        self.log("Reading" + raw_file)
        try:
            record_count = 0
            bytes_read = 0
            start = datetime.now()
            for len_path, len_data, timestamp, path, data, err in fileutil.unpack(raw_file):
                record_count += 1
                self.records_read += 1
                if err:
                    self.log("ERROR: Found corrupted data for record {0} in {1} path: {2} Error: {3}".format(record_count, raw_file, path, err))
                    self.bad_records += 1
                    continue
                if len(data) == 0:
                    self.log("WARN: Found empty data for record {0} in {2} path: {2}".format(record_count, raw_file, path))
                    self.bad_records += 1
                    continue

                # Incoming timestamps are in milliseconds, so convert to POSIX first
                # (ie. seconds)
                submission_date = date.fromtimestamp(timestamp / 1000).strftime("%Y%m%d")
                path = unicode(path, errors="replace")

                if data[0] != "{":
                    # Data looks weird, should be JSON.
                    self.log("Warning: Found unexpected data for record {0} in {1} path: {2} data:\n{3}".format(record_count, raw_file, path, data))
                else:
                    # Raw JSON, make sure we treat it as unicode.
                    data = unicode(data, errors="replace")

                current_bytes = len_path + len_data + fileutil.RECORD_PREAMBLE_LENGTH
                bytes_read += current_bytes
                self.bytes_read += current_bytes
                path_components = path.split("/")
                if len(path_components) != self.expected_dim_count:
                    # We're going to pop the ID off, but we'll also add the
                    # submission date, so it evens out.
                    self.log("Found an invalid path in record {0}: {1}".format(record_count, path))
                    continue

                key = path_components.pop(0)
                info = {}
                info["reason"] = path_components.pop(0)
                info["appName"] = path_components.pop(0)
                info["appVersion"] = path_components.pop(0)
                info["appUpdateChannel"] = path_components.pop(0)
                info["appBuildID"] = path_components.pop(0)
                dims = self.schema.dimensions_from(info, submission_date)

                try:
                    # Convert data:
                    if self.converter is None:
                        serialized_data = data
                        data_version = 1
                    else:
                        parsed_data, parsed_dims = self.converter.convert_json(data, dims[-1])
                        # TODO: take this out if it's too slow
                        for i in range(len(dims)):
                            if dims[i] != parsed_dims[i]:
                                self.log("Record {0} mismatched dimension {1}: '{2}' != '{3}'".format(self.records_read, i, dims[1], parsed_dims[i]))
                        serialized_data = self.converter.serialize(parsed_data)
                        dims = parsed_dims
                        data_version = 2
                    try:
                        # Write to persistent storage
                        n = self.storage.write(key, serialized_data, dims, data_version)
                        self.bytes_written += len(key) + len(serialized_data) + 1
                        self.records_written += 1
                        # Compress rotated files as we generate them
                        if n.endswith(StorageLayout.PENDING_COMPRESSION_SUFFIX):
                            self.q_out.put(n)
                    except Exception, e:
                        self.write_bad_record(key, dims, serialized_data, str(e), "ERROR Writing to output file:")
                except BadPayloadError, e:
                    self.write_bad_record(key, dims, data, e.msg, "Bad Payload:")
                except Exception, e:
                    err_message = str(e)

                    if err_message == "Missing in payload: info.revision":
                        # We don't need to write these bad records out - we know
                        # why they are being skipped.
                        self.bad_records += 1
                    elif err_message == "Invalid revision URL: /rev/":
                        # We do want to log these payloads, but we don't want
                        # the full stack trace.
                        self.write_bad_record(key, dims, data, err_message, "Conversion Error")
                    elif err_message.startswith("JSONDecodeError: Invalid control character"):
                        self.write_bad_record(key, dims, data, err_message, "Conversion Error")
                    else:
                        # TODO: recognize other common failure modes and handle them gracefully.
                        self.write_bad_record(key, dims, data, err_message, "Conversion Error")
                        self.log(traceback.format_exc())

                if self.print_stats:
                    this_update = datetime.now()
                    sec = timer.delta_sec(self.last_update, this_update)
                    if sec > 10.0:
                        self.last_update = this_update
                        self.end_time = datetime.now()
                        self.dump_stats()

            duration = timer.delta_sec(start)
            mb_read = bytes_read / 1024.0 / 1024.0
            # Stats for the current file:
            self.log("Read %d records %.2fMB in %.2fs (%.2fMB/s)" % (record_count, mb_read, duration, mb_read / duration))
        except Exception, e:
            # Corrupted data, let's skip this record.
            self.log("Error reading raw data from {0} {1}\n{2}".format(raw_file, e, traceback.format_exc()))

    def write_bad_record(self, key, dims, data, error, message=None):
        self.bad_records += 1
        if message is not None:
            self.log("{0} - {1}".format(message, error))
        if self.bad_filename is not None:
            try:
                path = u"/".join([key] + dims)
                self.storage.write_filename(path, data, self.bad_filename)
            except Exception, e:
                self.log("ERROR: {0}".format(e))


class CompressCompletedStep(PipeStep):
    def setup(self):
        self.compress_cmd = [StorageLayout.COMPRESS_PATH] + StorageLayout.COMPRESSION_ARGS

    def handle(self, record):
        filename = record
        base_ends = filename.find(".log") + 4
        if base_ends < 4:
            self.log("Bad filename encountered, skipping: " + filename)
            self.bad_records += 1
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
        start = datetime.now()

        # Now set up our processing pipe:
        # - read from f_raw, compress, write to comp_name
        p_compress = Popen(self.compress_cmd, bufsize=65536, stdin=f_raw, stdout=f_comp, stderr=sys.stderr)

        # Note: it looks like p_compress.wait() is what we want, but the docs
        #       warn of a deadlock, so we use communicate() instead.
        p_compress.communicate()

        raw_bytes = f_raw.tell()
        comp_bytes = f_comp.tell()
        raw_mb = float(raw_bytes) / 1024.0 / 1024.0
        comp_mb = float(comp_bytes) / 1024.0 / 1024.0
        f_raw.close()
        f_comp.close()

        self.bytes_read += raw_bytes
        self.bytes_written += comp_bytes

        # Remove raw file
        os.remove(tmp_name)
        sec = timer.delta_sec(start)
        self.log("Compressed %s as %s in %.2fs. Size before: %.2fMB, after: %.2fMB (r: %.2fMB/s, w: %.2fMB/s)" % (filename, comp_name, sec, raw_mb, comp_mb, (raw_mb/sec), (comp_mb/sec)))


class ExportCompressedStep(PipeStep):
    def __init__(self, num, name, q_in, log_file, base_dir, config, dry_run):
        self.dry_run = dry_run
        self.base_dir = base_dir
        self.aws_key = config.get("aws_key", None)
        self.aws_secret_key = config.get("aws_secret_key", None)
        self.aws_bucket_name = config["publish_bucket"]
        PipeStep.__init__(self, num, name, q_in, log_file=log_file)

    def setup(self):
        self.batch = []
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
            self.log("Warning: couldn't strip base dir from '{0}' {1}".format(record, e))
            stripped_name = record

        self.log("Uploading {0}".format(stripped_name))
        start = datetime.now()
        if self.dry_run:
            local_filename = record
            remote_filename = stripped_name
            err = None
        else:
            local_filename, remote_filename, err = s3util.upload_one([self.base_dir, self.bucket, stripped_name])
        sec = timer.delta_sec(start)
        current_size = os.path.getsize(record)
        self.bytes_read += current_size
        if err is None:
            # Everything went well.
            self.records_written += 1
            self.bytes_written += current_size
            # Delete local files once they've been uploaded successfully.
            if not self.dry_run:
                try:
                    os.remove(record)
                    self.log("Removed uploaded file {0}".format(record))
                except Exception, e:
                    self.log("Failed to remove uploaded file {0}: {1}".format(record, e))
        else:
            self.log("ERROR: failed to upload a file {0}: {1}".format(record, err))
            self.bad_records += 1
            # TODO: add to a "failures" queue, save them or something?

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


def main():
    signal.signal(signal.SIGINT, handle_sigint)
    # Turn on mp logging
    multiprocessing.log_to_stderr(logging.DEBUG)
    parser = argparse.ArgumentParser(description='Process incoming Telemetry data', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-c", "--config", help="AWS Configuration file (json)", required=True, type=file)

    parser.add_argument("-w", "--work-dir", help="Location to cache downloaded files", required=True)
    parser.add_argument("-o", "--output-dir", help="Base dir to store processed data", required=True)
    parser.add_argument("-i", "--input-files", help="File containing a list of keys to process", type=file)
    parser.add_argument("-b", "--bad-data-log", help="Save bad records to this file")
    parser.add_argument("-l", "--log-file", help="Log output to this file")
    parser.add_argument("--histogram-cache-path", help="Path to store a local cache of histograms", default="./histogram_cache")
    parser.add_argument("-t", "--telemetry-schema", help="Location of the desired telemetry schema", required=True)
    parser.add_argument("-m", "--max-output-size", metavar="N", help="Rotate output files after N bytes", type=int, default=500000000)
    parser.add_argument("-D", "--dry-run", help="Don't modify remote files", action="store_true")
    args = parser.parse_args()

    config = json.load(args.config)
    # TODO: allow commandline args to override config values.

    if not os.path.isdir(args.output_dir):
        os.makedirs(args.output_dir)

    schema_data = open(args.telemetry_schema)
    schema = TelemetrySchema(json.load(schema_data))
    schema_data.close()
    cache = RevisionCache(args.histogram_cache_path, "hg.mozilla.org")
    converter = Converter(cache, schema)
    storage = StorageLayout(schema, args.output_dir, args.max_output_size)

    logger = Log(args.log_file, "Master")

    num_cpus = multiprocessing.cpu_count()

    conn = None
    incoming_bucket = None
    incoming_queue = None
    s3downloader = None

    if not args.dry_run:
        # Set up AWS connections
        conn = S3Connection(config.get("aws_key", None), config.get("aws_secret_key", None))
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
            logger.log("Bucket {0} not found.  Attempting to create it.".format(config["publish_bucket"]))
            publish_bucket = conn.create_bucket(config["publish_bucket"])
        s3downloader = s3util.Loader(args.work_dir, config["incoming_bucket"], poolsize=num_cpus, aws_key=config.get("aws_key", None), aws_secret_key=config.get("aws_secret_key", None))

    raw_readers = None
    compressors = None
    exporters = None
    done = False

    while not done:
        try:
            start = datetime.now()
            incoming_filenames = []
            incoming_queue_messages = []
            logger.log("Fetching file list from queue " + config["incoming_queue"])
            if args.dry_run:
                logger.log("Dry run mode... can't read from the queue without messing things up...")
            else:
                # Sometimes we don't get all the messages, even if more are
                # available, so keep trying until we have enough (or there aren't
                # any left)
                for i in range(num_cpus):
                    messages = incoming_queue.get_messages(num_cpus - len(incoming_filenames))
                    for m in messages:
                        # Make sure this file exists in S3 first
                        possible_filename = m.get_body()
                        key = incoming_bucket.get_key(possible_filename)
                        if key is None:
                            logger.log("Could not find queued filename in bucket {0}: {1}".format(config["incoming_bucket"], possible_filename))
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

            before_download = datetime.now()
            logger.log("Downloading {0} files...".format(len(incoming_filenames)))
            local_filenames = []
            if args.dry_run:
                logger.log("Dry run mode: skipping download from S3")
            else:
                for local_filename, remote_filename, err in s3downloader.get_list(incoming_filenames):
                    if err is None:
                        local_filenames.append(local_filename)
                    else:
                        # s3downloader already retries 3 times.
                        logger.log("Error downloading {0} Error: {1}".format(local_filename, err))
                        return 2

            after_download = datetime.now()
            duration_sec = timer.delta_sec(before_download, after_download)
            downloaded_bytes = sum([ os.path.getsize(f) for f in local_filenames ])
            downloaded_mb = downloaded_bytes / 1024.0 / 1024.0
            downloaded_mbps = downloaded_mb / duration_sec
            logger.log("Downloaded %.2fMB in %.2fs (%.2fMB/s)" % (downloaded_mb, duration_sec, downloaded_mbps))
            # TODO: log downloaded_mb, duration_sec
            # statsd.increment("process_incoming.bytes_downloaded", downloaded_bytes)
            # statsd.histogram("process_incoming.download_speed_mbps", downloaded_mbps)

            raw_files = Queue()
            for l in local_filenames:
                raw_files.put(l)

            completed_files = Queue()

            # Begin reading raw input
            raw_readers = start_workers(logger, num_cpus, "Reader", ReadRawStep, raw_files,
                    (completed_files, args.log_file, schema, converter, storage, args.bad_data_log))

            # Tell readers to stop when they get to the end:
            finish_queue(raw_files, num_cpus)

            # Compress completed files.
            compressors = start_workers(logger, num_cpus, "Compressor", CompressCompletedStep,
                    completed_files, (None, args.log_file))

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

            try:
                # Export compressed files to S3.
                compressed_files = Queue()
                exporters = start_workers(logger, num_cpus, "Exporter", ExportCompressedStep,
                        compressed_files, (args.log_file, args.output_dir, config, args.dry_run))
                for root, dirs, files in os.walk(args.output_dir):
                    for f in files:
                        if f.endswith(StorageLayout.COMPRESSED_SUFFIX):
                            compressed_files.put(os.path.join(root, f))
                finish_queue(compressed_files, num_cpus)
                wait_for(logger, exporters, "Exporters")
            except InterruptProcessingError, e:
                logger.log("Received shutdown request... waiting for exporters to finish")
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
                        logger.log("  Dry run, so not really deleting {0}".format(m.get_body()))
                    else:
                        logger.log("  Deleting {0}".format(m.get_body()))
                        if incoming_queue.delete_message(m):
                            logger.log("  Message deleted successfully")
                        else:
                            logger.log("  Failed to delete message :(")
                logger.log("Done")

            duration = timer.delta_sec(start)
            logger.log("All done in %.2fs (%.2fs excluding download time)" % (duration, timer.delta_sec(after_download)))
        except InterruptProcessingError, e:
            logger.log("Received normal shutdown request... quittin' time!")
            if raw_readers is not None:
                terminate(logger, raw_readers, "Readers")
            if compressors is not None:
                terminate(logger, compressors, "Compressors")
            if exporters is not None:
                terminate(logger, exporters, "Exporters")

            done = True
    logger.log("All done.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
