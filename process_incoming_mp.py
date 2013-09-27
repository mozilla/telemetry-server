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
from datetime import date, datetime
from telemetry_schema import TelemetrySchema
import subprocess
from subprocess import Popen
from boto.s3.connection import S3Connection
import util.timer as timer
import util.files as fileutil
from convert import Converter, BadPayloadError
from revision_cache import RevisionCache
from persist import StorageLayout
import boto.sqs

S3FUNNEL_PATH = "/usr/local/bin/s3funnel"
def fetch_s3_files(incoming_files, fetch_cwd, bucket, aws_key, aws_secret_key):
    result = 0
    if len(incoming_files) > 0:
        if not os.path.isdir(fetch_cwd):
            os.makedirs(fetch_cwd)

        files = []
        for f in incoming_files:
            full_filename = os.path.join(fetch_cwd, f)
            if os.path.isfile(full_filename):
                md5, size = fileutil.md5file(full_filename)
                # f is the key name - it does not include the full path to the
                # data dir.
                key = bucket.get_key(f)
                # Strip quotes from md5
                remote_md5 = key.etag[1:-1]
                if md5 != remote_md5:
                    files.append(f)
                else:
                    print "Already downloaded", f
            else:
                files.append(f)
        fetch_cmd = [S3FUNNEL_PATH]
        fetch_cmd.append(bucket.name)
        fetch_cmd.append("get")
        fetch_cmd.append("-a")
        fetch_cmd.append(aws_key)
        fetch_cmd.append("-s")
        fetch_cmd.append(aws_secret_key)
        fetch_cmd.append("-t")
        fetch_cmd.append("8")
        # Fetch in batches of 8 files at a time
        while len(files) > 0:
            current_files = files[0:8]
            files = files[8:]
            start = datetime.now()
            result = subprocess.call(fetch_cmd + current_files, cwd=fetch_cwd)
            duration_sec = timer.delta_sec(start)
            # TODO: verify MD5s
            downloaded_bytes = sum([ os.path.getsize(os.path.join(fetch_cwd, f)) for f in current_files ])
            downloaded_mb = downloaded_bytes / 1024.0 / 1024.0
            print "Downloaded %.2fMB in %.2fs (%.2fMB/s)" % (downloaded_mb, duration_sec, downloaded_mb / duration_sec)
            if result != 0:
                break
    return result

def wait_for(processes, label):
    print "Waiting for", label, "..."
    for p in processes:
        p.join()
    print label, "Done."

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
    def __init__(self, num, name, q_in, base_dir, key, skey, bucket, dry_run):
        self.dry_run = dry_run
        self.batch_size = 8
        self.retries = 10
        self.base_dir = base_dir
        self.aws_key = key
        self.aws_secret_key = skey
        self.aws_bucket_name = bucket
        PipeStep.__init__(self, num, name, q_in)

    def setup(self):
        self.batch = []
        self.s3f_cmd = [S3FUNNEL_PATH, self.aws_bucket_name, "put",
                "-a", self.aws_key, "-s", self.aws_secret_key, "-t",
                str(self.batch_size), "--put-only-new", "--put-full-path"]
        if self.dry_run:
            self.conn = None
            self.bucket = None
            return
        self.conn = S3Connection(self.aws_key, self.aws_secret_key)
        self.bucket = self.conn.get_bucket(self.aws_bucket_name)

    def export_batch(self, data_dir, conn, bucket, files):
        print self.label, "Uploading", ",".join(files)
        if self.dry_run:
            return 0

        # Time the s3funnel call:
        start = datetime.now()
        result = subprocess.call(self.s3f_cmd + files, cwd=data_dir)
        sec = timer.delta_sec(start)

        total_size = 0
        if result == 0:
            # Success! Verify each file's checksum, then truncate it.
            for f in files:
                # Verify checksum and track cumulative size so we can figure out MB/s
                full_filename = os.path.join(data_dir, f)
                md5, size = fileutil.md5file(full_filename)
                total_size += size
                # f is the key name - it does not include the full path to the
                # data dir.
                key = bucket.get_key(f)
                # Strip quotes from md5
                remote_md5 = key.etag[1:-1]
                if md5 != remote_md5:
                    # TODO: add it to a "failed" queue.
                    print "ERROR: %s failed checksum verification: Local=%s, Remote=%s" % (f, md5, remote_md5)
                    self.bad_records += 1
                    result = -1
                # TODO: else add it to a "succeeded" queue and remove it locally.
        else:
            print "Failed to upload one or more files in the current batch. Error code was", result

        total_mb = float(total_size) / 1024.0 / 1024.0
        print "Transferred %.2fMB in %.2fs (%.2fMB/s)" % (total_mb, sec, total_mb / sec)
        return result

    def retry_export_batch(self, data_dir, conn, bucket, files):
        success = False
        for i in range(self.retries):
            batch_response = self.export_batch(self.base_dir, self.conn, self.bucket, self.batch)
            if batch_response == 0:
                success = True
                break
        return success

    def strip_data_dir(self, data_dir, full_file):
        if full_file.startswith(data_dir):
            chopped = full_file[len(data_dir):]
            if chopped[0] == "/":
                chopped = chopped[1:]
            return chopped
        else:
            print "ERROR: cannot remove", data_dir, "from", full_file
            raise ValueError("Invalid full filename: " + str(full_file))

    def handle(self, record):
        # Remove the output dir prefix from filenames
        try:
            stripped_name = self.strip_data_dir(self.base_dir, record)
        except Exception, e:
            print self.label, "Warning: couldn't strip base dir from", record, e
            stripped_name = record
        self.batch.append(stripped_name)
        if len(self.batch) >= self.batch_size:
            success = self.retry_export_batch(self.base_dir, self.conn, self.bucket, self.batch)
            if success:
                # Delete local files once they've been uploaded successfully.
                if not self.dry_run:
                    for b in self.batch:
                        try:
                            os.remove(os.path.join(self.base_dir, b))
                            print self.label, "Removed uploaded file", b
                        except Exception, e:
                            print self.label, "Failed to remove uploaded file", b
                self.batch = []
            else:
                print self.label, "ERROR: failed to upload a batch:", ",".join(self.batch)
                # TODO: add to a "failures" queue, save them or something?

    def finish(self):
        if len(self.batch) > 0:
            print "Sending last batch of", len(self.batch)
            success = self.retry_export_batch(self.base_dir, self.conn, self.bucket, self.batch)
            if not success:
                print self.label, "ERROR: failed to upload a batch:", ",".join(self.batch)
                # TODO: add to a "failures" queue, save them or something?
        self.dump_stats()

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
    parser = argparse.ArgumentParser(description='Process incoming Telemetry data', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("incoming_bucket", help="The S3 bucket containing incoming files")
    parser.add_argument("publish_bucket", help="The S3 bucket to save processed files")
    parser.add_argument("-k", "--aws-key", help="AWS Key", required=True)
    parser.add_argument("-s", "--aws-secret-key", help="AWS Secret Key", required=True)
    parser.add_argument("-r", "--aws-region", help="AWS Region", default="us-west-2")
    parser.add_argument("-w", "--work-dir", help="Location to cache downloaded files", required=True)
    parser.add_argument("-o", "--output-dir", help="Base dir to store processed data", required=True)
    parser.add_argument("-i", "--input-files", help="File containing a list of keys to process", type=file)
    parser.add_argument("-b", "--bad-data-log", help="Save bad records to this file")
    parser.add_argument("-q", "--queue", help="SQS Queue name to poll for incoming data")
    parser.add_argument("-c", "--histogram-cache-path", help="Path to store a local cache of histograms", default="./histogram_cache")
    parser.add_argument("-t", "--telemetry-schema", help="Location of the desired telemetry schema", required=True)
    parser.add_argument("-m", "--max-output-size", metavar="N", help="Rotate output files after N bytes", type=int, default=500000000)
    parser.add_argument("-D", "--dry-run", help="Don't modify remote files", action="store_true")
    parser.add_argument("-C", "--skip-conversion", help="Skip validation/conversion of payloads", action="store_true")
    args = parser.parse_args()

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
    if args.skip_conversion:
        converter = None
    else:
        converter = Converter(cache, schema)
    storage = StorageLayout(schema, args.output_dir, args.max_output_size)

    num_cpus = multiprocessing.cpu_count()

    start = datetime.now()
    conn = None
    incoming_bucket = None
    incoming_queue = None
    incoming_queue_messages = []

    if not args.dry_run:
        conn = S3Connection(args.aws_key, args.aws_secret_key)
        incoming_bucket = conn.get_bucket(args.incoming_bucket)

    incoming_filenames = []
    if args.queue is not None:
        print "Fetching file list from queue", args.queue
        if args.dry_run:
            print "Dry run mode... can't read from the queue without messing things up..."
        else:
            q_conn = boto.sqs.connect_to_region(args.aws_region,
                    aws_access_key_id=args.aws_key,
                    aws_secret_access_key=args.aws_secret_key)
            incoming_queue = q_conn.get_queue(args.queue)
            if incoming_queue is None:
                print "Error: could not get queue", args.queue
                return -2
            # Sometimes we don't get all the messages, even if more are
            # available, so keep trying until we have enough (or there aren't
            # any left)
            for i in range(num_cpus):
                messages = incoming_queue.get_messages(num_cpus - len(incoming_filenames))
                for m in messages:
                    # TODO: Make sure this file exists in S3 first?
                    possible_filename = m.get_body()
                    key = incoming_bucket.get_key(possible_filename)
                    if key is None:
                        print "Could not find queued filename in bucket", args.incoming_bucket, ":", possible_filename
                        # try to delete it:
                        incoming_queue.delete_message(m)
                    else:
                        incoming_filenames.append(possible_filename)
                        incoming_queue_messages.append(m)
                if len(messages) == 0 or len(incoming_filenames) >= num_cpus:
                    break
    elif args.input_files:
        print "Fetching file list from file", args.input_files
        incoming_filenames = [ l.strip() for l in args.input_files.readlines() ]
    else:
        print "Fetching file list from S3..."
        for f in incoming_bucket.list():
            incoming_filenames.append(f.name)
    print "Done"

    if len(incoming_filenames) == 0:
        print "Nothing to do!"
        return 0

    for f in incoming_filenames:
        print "  ", f

    print "Verifying that we can write to", args.publish_bucket
    if args.dry_run:
        print "Dry run mode: don't care!"
    else:
        try:
            publish_bucket = conn.get_bucket(args.publish_bucket)
            print "Looks good!"
        except S3ResponseError:
            print "Bucket", args.publish_bucket, "not found.  Attempting to create it."
            publish_bucket = conn.create_bucket(publish_bucket)

    result = 0
    print "Downloading", len(incoming_filenames), "files..."
    if args.dry_run:
        print "Dry run mode: skipping download from S3"
    else:
        result = fetch_s3_files(incoming_filenames, args.work_dir,
                incoming_bucket, args.aws_key, args.aws_secret_key)

    if result != 0:
        print "Error downloading files. Return code of s3funnel was", result
        return result
    print "Done"

    after_download = datetime.now()

    local_filenames = [os.path.join(args.work_dir, f) for f in incoming_filenames]

    # TODO: try a SimpleQueue
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

    # Export compressed files to S3.
    exporters = start_workers(num_cpus, "Exporter", ExportCompressedStep,
            compressed_files, (args.output_dir, args.aws_key,
                args.aws_secret_key, args.publish_bucket, args.dry_run))

    wait_for(raw_readers, "Raw Readers")

    # `find <out_dir> -type f -not -name ".compressme"`
    # Add them to completed_files
    for root, dirs, files in os.walk(args.output_dir):
        for f in files:
            if f.endswith(".log"):
                completed_files.put(os.path.join(root, f))

    for i in range(num_cpus):
        completed_files.put(PipeStep.SENTINEL)

    wait_for(compressors, "Compressors")
    for i in range(num_cpus):
        compressed_files.put(PipeStep.SENTINEL)

    wait_for(exporters, "Exporters")

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
    return 0

if __name__ == "__main__":
    sys.exit(main())
