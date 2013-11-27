#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import argparse
import uuid
import multiprocessing
from multiprocessing import Process, Queue
import Queue as Q
import simplejson as json
import sys
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

S3FUNNEL_PATH = "/usr/local/bin/s3funnel"

def wait_for(processes, label):
    print "Waiting for", label, "..."
    for p in processes:
        p.join()
    print label, "Done."

def terminate(processes, label):
    print "Terminating", label, "..."
    for p in processes:
        p.terminate()
    print label, "Done."

class InterruptProcessingError(Exception):
    def __init__(self, msg):
        self.msg = msg

def handle_sigint(signum, frame):
    print "Caught signal " + str(signum)
    if signum == signal.SIGINT:
        raise InterruptProcessingError("It's quittin' time")


class PipeStep(object):
    SENTINEL = 'STOP'
    def __init__(self, num, name, q_in, q_out=None):
        self.print_stats = True
        self.num = num
        self.label = " ".join((name, str(num)))
        self.q_in = q_in
        self.q_out = q_out
        self.start_time = datetime.now()
        self.end_time = datetime.now()
        self.last_update = datetime.now()
        self.bad_records = 0
        self.records_read = 0
        self.records_written = 0
        self.bytes_read = 0
        self.bytes_written = 0

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
        print "%s: Read %d records or %.2fMB (%.2fr/s, %.2fMB/s), wrote %d or %.2f MB (%.2fr/s, %.2fMB/s). Found %d bad records" % (self.label, self.records_read, mb_read, read_rate, mb_read_rate, self.records_written, mb_written, write_rate, mb_write_rate, self.bad_records)

    def finish(self):
        print self.label, "All done"
        self.dump_stats()

    def handle(self, record):
        pass

    def work(self):
        print self.label, "Starting up"
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
        print self.label, "Received stop message... all done"

class ReadRawStep(PipeStep):
    def __init__(self, num, name, raw_files, completed_files, schema, converter, storage, bad_filename):
        self.schema = schema
        self.converter = converter
        self.storage = storage
        self.bad_filename = bad_filename
        PipeStep.__init__(self, num, name, raw_files, completed_files)

    def setup(self):
        self.expected_dim_count = len(self.schema._dimensions)

    def handle(self, raw_file):
        print self.label, "reading", raw_file
        try:
            record_count = 0
            bytes_read = 0
            start = datetime.now()
            for len_path, len_data, timestamp, path, data, err in fileutil.unpack(raw_file):
                record_count += 1
                self.records_read += 1
                if err:
                    print self.label, "ERROR: Found corrupted data for record", record_count, "in", raw_file, "path:", path, "Error:", err
                    self.bad_records += 1
                    continue
                if len(data) == 0:
                    print self.label, "ERROR: Found empty data for record", record_count, "in", raw_file, "path:", path
                    self.bad_records += 1
                    continue

                # Incoming timestamps are in milliseconds, so convert to POSIX first
                # (ie. seconds)
                submission_date = date.fromtimestamp(timestamp / 1000).strftime("%Y%m%d")
                path = unicode(path, errors="replace")
                #print "Path for record", record_count, path, "length of data:", len_data

                if data[0] != "{":
                    # Data looks weird, should be JSON.
                    print self.label, "Warning: Found unexpected data for record", record_count, "in", raw_file, "path:", path, "data:"
                    print data
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
                    print self.label, "Found an invalid path in record", record_count, path
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
                                print self.label, "Record", self.records_read, "mismatched dimension", i, dims[i], "!=", parsed_dims[i]
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

                    # We don't need to write these bad records out - we know
                    # why they are being skipped.
                    if err_message != "Missing in payload: info.revision":
                        # TODO: recognize other common failure modes and handle them gracefully.
                        self.write_bad_record(key, dims, data, err_message, "Conversion Error:")
                        traceback.print_exc()

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
            print self.label, "- Read %d records %.2fMB in %.2fs (%.2fMB/s)" % (record_count, mb_read, duration, mb_read / duration)
        except Exception, e:
            # Corrupted data, let's skip this record.
            print self.label, "- Error reading raw data from ", raw_file, e
            traceback.print_exc()

    def write_bad_record(self, key, dims, data, error, message=None):
        self.bad_records += 1
        if message is not None:
            print self.label, message, error
        if self.bad_filename is not None:
            try:
                path = u"/".join([key] + dims)
                self.storage.write_filename(path, data, self.bad_filename)
            except Exception, e:
                print self.label, "ERROR:", e


class CompressCompletedStep(PipeStep):
    def setup(self):
        self.compress_cmd = [StorageLayout.COMPRESS_PATH] + StorageLayout.COMPRESSION_ARGS

    def handle(self, record):
        filename = record
        base_ends = filename.find(".log") + 4
        if base_ends < 4:
            logging.warn("Bad filename encountered, skipping: " + filename)
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
        print self.label, "Compressed %s as %s in %.2fs. Size before: %.2fMB, after: %.2fMB (r: %.2fMB/s, w: %.2fMB/s)" % (filename, comp_name, sec, raw_mb, comp_mb, (raw_mb/sec), (comp_mb/sec))
        self.q_out.put(comp_name)


class ExportCompressedStep(PipeStep):
    def __init__(self, num, name, q_in, base_dir, config, dry_run):
        self.dry_run = dry_run
        self.base_dir = base_dir
        self.aws_key = config.get("aws_key", None)
        self.aws_secret_key = config.get("aws_secret_key", None)
        self.aws_bucket_name = config["publish_bucket"]
        PipeStep.__init__(self, num, name, q_in)

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
            print self.label, "Warning: couldn't strip base dir from", record, e
            stripped_name = record

        print self.label, "Uploading", stripped_name
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
                    print self.label, "Removed uploaded file", record
                except Exception, e:
                    print self.label, "Failed to remove uploaded file", record, e
        else:
            print self.label, "ERROR: failed to upload a file:", record, err
            self.bad_records += 1
            # TODO: add to a "failures" queue, save them or something?

def start_workers(count, name, clazz, q_in, more_args):
    workers = []
    for i in range(count):
        w = Process(
                target=clazz,
                args=(i, name, q_in) + more_args)
        workers.append(w)
        w.start()
        print name, i, "pid:", w.pid
    print name + "s", "all started"
    return workers


def main():
    signal.signal(signal.SIGINT, handle_sigint)
    parser = argparse.ArgumentParser(description='Process incoming Telemetry data', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-c", "--config", help="AWS Configuration file (json)", required=True, type=file)

    parser.add_argument("-w", "--work-dir", help="Location to cache downloaded files", required=True)
    parser.add_argument("-o", "--output-dir", help="Base dir to store processed data", required=True)
    parser.add_argument("-i", "--input-files", help="File containing a list of keys to process", type=file)
    parser.add_argument("-b", "--bad-data-log", help="Save bad records to this file")
    parser.add_argument("--histogram-cache-path", help="Path to store a local cache of histograms", default="./histogram_cache")
    parser.add_argument("-t", "--telemetry-schema", help="Location of the desired telemetry schema", required=True)
    parser.add_argument("-m", "--max-output-size", metavar="N", help="Rotate output files after N bytes", type=int, default=500000000)
    parser.add_argument("-D", "--dry-run", help="Don't modify remote files", action="store_true")
    args = parser.parse_args()

    config = json.load(args.config)
    # TODO: allow commandline args to override config values.

    if not os.path.isfile(S3FUNNEL_PATH):
        print "ERROR: s3funnel not found at", S3FUNNEL_PATH
        print "You can get it from github: https://github.com/sstoiana/s3funnel"
        return -1

    if not os.path.isdir(args.output_dir):
        os.makedirs(args.output_dir)

    schema_data = open(args.telemetry_schema)
    schema = TelemetrySchema(json.load(schema_data))
    schema_data.close()
    cache = RevisionCache(args.histogram_cache_path, "hg.mozilla.org")
    converter = Converter(cache, schema)
    storage = StorageLayout(schema, args.output_dir, args.max_output_size)

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
            print "Error: could not get queue", config["incoming_queue"]
            return -2

        print "Verifying that we can write to", config["publish_bucket"]
        try:
            publish_bucket = conn.get_bucket(config["publish_bucket"])
            print "Looks good!"
        except S3ResponseError:
            print "Bucket", config["publish_bucket"], "not found.  Attempting to create it."
            publish_bucket = conn.create_bucket(config["publish_bucket"])
        s3downloader = s3util.Loader(args.work_dir, config["incoming_bucket"], poolsize=num_cpus)

    raw_readers = None
    compressors = None
    exporters = None
    done = False

    while not done:
        try:
            start = datetime.now()
            incoming_filenames = []
            incoming_queue_messages = []
            print "Fetching file list from queue", config["incoming_queue"]
            if args.dry_run:
                print "Dry run mode... can't read from the queue without messing things up..."
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
                            print "Could not find queued filename in bucket", config["incoming_bucket"], ":", possible_filename
                            # try to delete it:
                            incoming_queue.delete_message(m)
                        else:
                            incoming_filenames.append(possible_filename)
                            incoming_queue_messages.append(m)
                    if len(messages) == 0 or len(incoming_filenames) >= num_cpus:
                        break
            print "Done"

            if len(incoming_filenames) == 0:
                print "Nothing to do! Sleeping..."
                time.sleep(5)
                continue

            for f in incoming_filenames:
                print "  ", f

            before_download = datetime.now()
            print "Downloading", len(incoming_filenames), "files..."
            local_filenames = []
            if args.dry_run:
                print "Dry run mode: skipping download from S3"
            else:
                for local_filename, remote_filename, err in s3downloader.get_list(incoming_filenames):
                    if err is None:
                        local_filenames.append(local_filename)
                    else:
                        # s3downloader already retries 3 times.
                        print "Error downloading", local_filename, "Error:", err
                        return 2

            after_download = datetime.now()
            duration_sec = timer.delta_sec(before_download, after_download)
            downloaded_bytes = sum([ os.path.getsize(f) for f in local_filenames ])
            downloaded_mb = downloaded_bytes / 1024.0 / 1024.0
            downloaded_mbps = downloaded_mb / duration_sec
            print "Downloaded %.2fMB in %.2fs (%.2fMB/s)" % (downloaded_mb, duration_sec, downloaded_mbps)
            # TODO: log downloaded_mb, duration_sec
            # statsd.increment("process_incoming.bytes_downloaded", downloaded_bytes)
            # statsd.histogram("process_incoming.download_speed_mbps", downloaded_mbps)

            raw_files = Queue()
            for l in local_filenames:
                raw_files.put(l)

            completed_files = Queue()
            compressed_files = Queue()

            # Begin reading raw input
            raw_readers = start_workers(num_cpus, "Reader", ReadRawStep, raw_files,
                    (completed_files, schema, converter, storage, args.bad_data_log))

            # Tell readers when to stop:
            for i in range(num_cpus):
                raw_files.put(PipeStep.SENTINEL)

            # Compress completed files.
            compressors = start_workers(num_cpus, "Compressor", CompressCompletedStep,
                    completed_files, (compressed_files,))

            wait_for(raw_readers, "Raw Readers")

            # `find <out_dir> -type f -not -name ".compressme"`
            # Add them to completed_files
            for root, dirs, files in os.walk(args.output_dir):
                for f in files:
                    if f.endswith(".log"):
                        completed_files.put(os.path.join(root, f))

            # Tell compressors when to stop:
            for i in range(num_cpus):
                completed_files.put(PipeStep.SENTINEL)

            wait_for(compressors, "Compressors")

            # Tell exporters when to stop:
            for i in range(num_cpus):
                compressed_files.put(PipeStep.SENTINEL)

            try:
                # Export compressed files to S3.
                exporters = start_workers(num_cpus, "Exporter", ExportCompressedStep,
                        compressed_files, (args.output_dir, config, args.dry_run))
                wait_for(exporters, "Exporters")
            except InterruptProcessingError, e:
                print "Received shutdown request... waiting for exporters to finish"
                done = True
                wait_for(exporters, "Exporters")
                print "OK, cleaning up"

            print "Removing processed logs from S3..."
            for f in incoming_filenames:
                if args.dry_run:
                    print "  Dry run, so not really deleting", f
                else:
                    print "  Deleting", f
                    incoming_bucket.delete_key(f)
                    # Delete file locally too.
                    os.remove(os.path.join(args.work_dir, f))
            print "Done"

            if len(incoming_queue_messages) > 0:
                print "Removing processed messages from SQS..."
                for m in incoming_queue_messages:
                    if args.dry_run:
                        print "  Dry run, so not really deleting", m.get_body()
                    else:
                        print "  Deleting", m.get_body()
                        if incoming_queue.delete_message(m):
                            print "  Message deleted successfully"
                        else:
                            print "  Failed to delete message :("
                print "Done"

            duration = timer.delta_sec(start)
            print "All done in %.2fs (%.2fs excluding download time)" % (duration, timer.delta_sec(after_download))
        except InterruptProcessingError, e:
            print "Received normal shutdown request... quittin' time!"
            if raw_readers is not None:
                terminate(raw_readers, "Readers")
            if compressors is not None:
                terminate(compressors, "Compressors")
            if exporters is not None:
                terminate(exporters, "Exporters")

            done = True
    print "All done."
    return 0

if __name__ == "__main__":
    sys.exit(main())
