#!/usr/bin/env python
# encoding: utf-8
"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""

import argparse
import time
import multiprocessing
from multiprocessing import Process, Queue
import Queue as Q
import simplejson as json
import imp
import sys
import os
import json
import marshal
import traceback
from datetime import date, datetime
from multiprocessing import Process
from telemetry_schema import TelemetrySchema
from persist import StorageLayout
import subprocess
from subprocess import Popen, PIPE
from boto.s3.connection import S3Connection
import util.timer as timer
import struct, gzip, StringIO


def fetch_s3_files(files, fetch_cwd, bucket_name, aws_key, aws_secret_key):
    result = 0
    if len(files) > 0:
        if not os.path.isdir(fetch_cwd):
            os.makedirs(fetch_cwd)
        fetch_cmd = ["/usr/local/bin/s3funnel"]
        fetch_cmd.append(bucket_name)
        fetch_cmd.append("get")
        fetch_cmd.append("-a")
        fetch_cmd.append(aws_key)
        fetch_cmd.append("-s")
        fetch_cmd.append(aws_secret_key)
        fetch_cmd.append("-t")
        fetch_cmd.append("8")
        start = datetime.now()
        result = subprocess.call(fetch_cmd + files, cwd=fetch_cwd)
        duration_sec = timer.delta_sec(start)
        # TODO: verify MD5s
        downloaded_bytes = sum([ os.path.getsize(os.path.join(fetch_cwd, f)) for f in files ])
        downloaded_mb = downloaded_bytes / 1024.0 / 1024.0
        print "Downloaded %.2fMB in %.2fs (%.2fMB/s)" % (downloaded_mb, duration_sec, downloaded_mb / duration_sec)
    return result

def abort_on_error(err):
    # TODO: make this kill all procs
    while True:
        try:
            error = err.get(True, 3)
            if is_sentinel(error):
                break
            #if isinstance(error, basestring):
            sys.stderr.write(str(error) + "\n")
            #else:
            #    sys.stderr.write(json.dumps(error))
            #sys.exit(1)
        except Q.Empty:
            #print "No errors so far!"
            pass
    
def wait_for(processes, label):
    print "Waiting for", label, "..."
    for p in processes:
        p.join()
    print label, "Done."

def is_sentinel(value):
    return value is None

def get_sentinel():
    return None

def read_raw_records(n, num_cpus, raw_files, schema, q_raw, errors):
    print "Raw Reader #", n, "of", num_cpus, "starting up"

    #time.sleep(10)
    #errors.put("woops")

    expected_dim_count = len(schema._dimensions)
    while True:
        # Wait (potentially forever) for a raw file
        raw_file = raw_files.get()
        if is_sentinel(raw_file):
            print "reader", n, "got exit message"
            # put it back for other processes.
            #raw_files.put('exit')
            # Pass it along to the next stage too.
            q_raw.put(get_sentinel())
            break
        print "Reader", n, "reading", raw_file
        try:
            fin = open(raw_file, "rb")
            bytes_read = 0
            record_count = 0
            start = datetime.now()
            while True:
                record_count += 1
                # Read two 4-byte values and one 8-byte value
                lengths = fin.read(16)
                if lengths == '':
                    break
                len_path, len_data, timestamp = struct.unpack("<IIQ", lengths)

                # Incoming timestamps are in milliseconds, so convert to POSIX first
                # (ie. seconds)
                submission_date = date.fromtimestamp(timestamp / 1000).strftime("%Y%m%d")
                path = unicode(fin.read(len_path), errors="replace")
                #print "Path for record", record_count, path, "length of data:", len_data

                # Detect and handle gzipped data.
                data = fin.read(len_data)
                if ord(data[0]) == 0x1f and ord(data[1]) == 0x8b:
                    # Data is gzipped, uncompress it:
                    try:
                        # Note: from brief testing, cStringIO doesn't appear to be any
                        #       faster. In fact, it seems slightly slower than StringIO.
                        data_reader = StringIO.StringIO(data)
                        uncompressor = gzip.GzipFile(fileobj=data_reader, mode="r")
                        data = unicode(uncompressor.read(), errors="replace")
                        uncompressor.close()
                        data_reader.close()
                    except Exception, e:
                        # Corrupted data, let's skip this record.
                        print "Warning: Found corrupted data for record", record_count, "in", raw_file, "path:", path
                        continue
                elif data[0] != "{":
                    # Data looks weird, should be JSON.
                    print "Warning: Found unexpected data for record", record_count, "in", raw_file, "path:", path, "data:"
                    print data

                bytes_read += 8 + len_path + len_data
                #print "Path for record", record_count, path, "length of data:", len_data, "data:", data[0:5] + "..."

                path_components = path.split("/")
                if len(path_components) != expected_dim_count:
                    # We're going to pop the ID off, but we'll also add the submission,
                    # so it evens out.
                    print "Found an invalid path in record", record_count, path
                    continue

                key = path_components.pop(0)
                info = {}
                info["reason"] = path_components.pop(0)
                info["appName"] = path_components.pop(0)
                info["appVersion"] = path_components.pop(0)
                info["appUpdateChannel"] = path_components.pop(0)
                info["appBuildID"] = path_components.pop(0)
                dimensions = schema.dimensions_from(info, submission_date)
                q_raw.put((key, dimensions, data))
            duration = timer.delta_sec(start)
            mb_read = bytes_read / 1024.0 / 1024.0
            print "Reader", n, "- Read %.2fMB in %.2fs (%.2fMB/s)" % (mb_read, duration, mb_read / duration)
        except Exception, e:
            # Corrupted data, let's skip this record.
            errors.put({"err": e, "message": "Error reading raw data from " + raw_file, "id": n})
            continue
    print "Reader", n, "all done"
    sys.exit(0)

def convert_raw_records(n, num_cpus, q_raw, q_converted, q_bad, errors):
    print "Converter #", n, "of", num_cpus, "starting up"
    #time.sleep(10)
    #errors.put("woops")
    while True:
        raw = q_raw.get()
        if is_sentinel(raw):
            print "converter", n, "got exit message"
            # tell the other processes to exit too
            q_raw.put(get_sentinel())
            # pass it along and clean up
            q_converted.put(get_sentinel())
            break
        key, dims, data = raw
        #print "Converter", n, "got", key
    print "Converter", n, "all done"
    sys.exit(0)

def write_converted_records(n, num_cpus, q_converted, q_completed, errors):
    print "Writer #", n, "of", num_cpus, "starting up"

    while True:
        converted = q_converted.get()
        if is_sentinel(converted):
            print "writer", n, "got exit message"
            q_converted.put(get_sentinel())
            # Note: do NOT pass 'exit' forward to q_completed, since we'll
            #       need to process all the non-rotated files when we are
            #       all finished coverting.
            break
    print "Writer", n, "all done"
    sys.exit(0)

def export_completed_files(n, num_cpus, q_completed, errors):
    print "Exporter #", n, "of", num_cpus, "starting up"

    time.sleep(10)
    errors.put("exporter %d woops" % (n))
    sys.exit(0)

def add_sentinels(queue, count):
    for i in range(count):
        queue.put(get_sentinel())

def main():
    parser = argparse.ArgumentParser(description='Process incoming Telemetry data', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("incoming_bucket", help="The S3 bucket containing incoming files")
    parser.add_argument("publish_bucket", help="The S3 bucket to save processed files")
    parser.add_argument("-k", "--aws-key", help="AWS Key", required=True)
    parser.add_argument("-s", "--aws-secret-key", help="AWS Secret Key", required=True)
    parser.add_argument("-w", "--work-dir", help="Location to cache downloaded files", required=True)
    parser.add_argument("-o", "--output-dir", help="Base dir to store processed data", required=True)
    parser.add_argument("-i", "--input-files", help="File containing a list of keys to process", type=file)
    parser.add_argument("-t", "--telemetry-schema", help="Location of the desired telemetry schema", required=True)
    args = parser.parse_args()

    schema_data = open(args.telemetry_schema)
    schema = TelemetrySchema(json.load(schema_data))
    schema_data.close()

    #num_cpus = multiprocessing.cpu_count()
    num_cpus = 2

    start = datetime.now()
    conn = S3Connection(args.aws_key, args.aws_secret_key)
    incoming_bucket = conn.get_bucket(args.incoming_bucket)
    incoming_filenames = []
    if args.input_files:
        print "Fetching file list from file", args.input_files
        incoming_filenames = [ l.strip() for l in args.input_files.readlines() ]
    else:
        print "Fetching file list from S3..."
        for f in incoming_bucket.list():
            incoming_filenames.append(f.name)
    print "Done"

    for f in incoming_filenames:
        print "  ", f
    

    result = 0
    print "Downloading", len(incoming_filenames), "files..."
    result = 0#fetch_s3_files(incoming_filenames, args.work_dir, args.incoming_bucket, args.aws_key, args.aws_secret_key)
    if result != 0:
        print "Error downloading files. Return code of s3funnel was", result
        return result
    print "Done"


    print "Splitting raw logs..."
    local_filenames = [os.path.join(args.work_dir, f) for f in incoming_filenames]

    # TODO: try a SimpleQueue
    raw_files = Queue(100)
    for l in local_filenames:
        raw_files.put(l)
    add_sentinels(raw_files, num_cpus)

    raw_records = Queue(150000)
    converted_records = Queue(50000)
    bad_records = Queue()
    completed_files = Queue()
    compressed_files = Queue()

    # Fatal processing errors.
    errors = Queue()

    error_watcher = Process(target=abort_on_error, args=(errors))
    error_watcher.start()
    print "error_watcher pid:", error_watcher.pid

    # Begin reading raw input
    raw_readers = []
    for i in range(num_cpus):
        rr = Process(
                target=read_raw_records,
                args=(i, num_cpus, raw_files, schema, raw_records, errors))
        raw_readers.append(rr)
        rr.start()
        print "Reader", i, "pid:", rr.pid
    print "Readers all started"

    # Convert raw input as it becomes available
    converters = []
    for i in range(num_cpus):
        cr = Process(
                target=convert_raw_records,
                args=(i, num_cpus, raw_records, converted_records, bad_records, errors))
        converters.append(cr)
        cr.start()
        print "Converter", i, "pid:", cr.pid
    print "Converters all started"

    # Writer converted data as it becomes available
    writers = []
    for i in range(num_cpus):
        w = Process(
                target=write_converted_records,
                args=(i, num_cpus, converted_records, completed_files, errors))
        writers.append(w)
        w.start()
        print "Writer", i, "pid:", w.pid
    print "Writers all started"

    # Compress and export completed files.
    exporters = []
    for i in range(num_cpus):
        e = Process(
                target=export_completed_files,
                args=(i, num_cpus, completed_files, errors))
        exporters.append(e)
        e.start()
        print "Exporter", i, "pid:", e.pid
    print "Exporters all started"

    # Wait for raw input to complete.
    wait_for(raw_readers, "Raw Readers")

    # Wait for conversion to complete.
    wait_for(converters, "Converters")

    wait_for(writers, "Converted Writers")

    # TODO: find <out_dir> -type f -not -name ".compressme"
    # Add them to completed_files

    wait_for(exporters, "Exporters to S3")

    print "Removing processed logs from S3..."
    for f in incoming_filenames:
        print "  Deleting", f
        #incoming_bucket.delete_key(f)
    print "Done"

    duration = timer.delta_sec(start)
    print "All done in %.2fs" % (duration)

    errors.put(get_sentinel())

    # We haven't had an error by now, terminate the error watcher.
    error_watcher.terminate()
    return 0

if __name__ == "__main__":
    sys.exit(main())
