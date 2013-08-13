#!/usr/bin/env python
# encoding: utf-8
"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""

import argparse
import sys
import httplib, urllib
from urlparse import urlparse
from datetime import datetime

def send(conn, o, data):
    conn.request("POST", o.path, data)
    response = conn.getresponse()
    return response.read()

def delta_ms(start, end=None):
    if end is None:
        end = datetime.now()
    delta = end - start
    ms = delta.seconds * 1000.0 + float(delta.microseconds) / 1000.0
    # prevent division-by-zero errors by cheating:
    if ms == 0.0:
        return 0.0001
    return ms

def delta_sec(start, end=None):
    return delta_ms(start, end) / 1000.0

def run_benchmark(args):
    o = urlparse(args.server_url)
    worst_time = -1.0
    #headers = {"Content-type": "application/x-www-form-urlencoded",
    #           "Accept": "text/plain"}
    total_ms = 0.0
    record_count = 0
    request_count = 0
    total_size = 0
    conn = httplib.HTTPConnection(o.netloc)
    #conn.set_debuglevel(1)
    batch = []
    latencies = []

    while True:
        line = sys.stdin.readline()
        if line == '':
            break
        line = line.strip()
        data = ""
        if args.batch_size == 0:
            data = line
        else:
            # TODO: generate a UUID?
            batch.append("bogusid")
            batch.append(line.replace("\t", ' '))
            if record_count % args.batch_size == 0:
                data = "\t".join(batch)
                batch = []

        record_count += 1
        if not args.verbose and record_count % 100 == 0:
            print "Processed", record_count, "records in", request_count, "requests so far"

        if data:
            start = datetime.now()
            if args.dry_run:
                resp = "created"
            else:
                resp = send(conn, o, data)
            ms = delta_ms(start)
            latencies.append(ms)
            if worst_time < ms:
                worst_time = ms
            total_ms += ms
            request_count += 1
            total_size += len(data)

            if args.verbose:
                print "%s %.1fms, avg %.1f, max %.1f, %.2fMB/s, %.2f reqs/s, %.2f records/s" % (resp, ms, total_ms/request_count, worst_time, (total_size/1000.0/total_ms), (1000.0 * request_count / total_ms), (1000.0 * record_count / total_ms))
    # Send the last (partial) batch
    if len(batch):
        start = datetime.now()
        resp = send(conn, o, "\t".join(batch))
        ms = delta_ms(start)
        latencies.append(ms)
        if worst_time < ms:
            worst_time = ms
        total_ms += ms
        request_count += 1
        total_size += len(line)
        if args.verbose:
            print "%s %.1fms, avg %.1f, max %.1f, %.2fMB/s, %.2f reqs/s, %.2f records/s" % (resp, ms, total_ms/request_count, worst_time, (total_size/1000.0/total_ms), (1000.0 * request_count / total_ms), (1000.0 * record_count / total_ms))

    latencies.sort()
    assert(len(latencies) == request_count)
    print "Min:",   latencies[0]
    print "Max:",   latencies[-1]
    print "Med:",   latencies[int(0.5 * request_count)]
    print "Avg:",   sum(latencies) / request_count
    print "75%:",   latencies[int(0.75 * request_count)]
    print "95%:",   latencies[int(0.95 * request_count)]
    print "99%:",   latencies[int(0.99 * request_count)]
    print "99.9%:", latencies[int(0.999 * request_count)]
    return record_count, request_count, total_size, worst_time

def main():
    parser = argparse.ArgumentParser(description='Run benchmark on a Telemetry Server', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("server_url", help="The URL of the target server to benchmark")
    parser.add_argument("-p", "--num-processes", metavar="N", help="Start N client processes", type=int, default=4)
    parser.add_argument("-b", "--batch-size", metavar="N", help="Send N records per batch (use 0 to send individual requests)", type=int, default=20)
    parser.add_argument("-z", "--gzip-compress", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-d", "--dry-run", action="store_true")
    args = parser.parse_args()
    start = datetime.now()
    record_count, request_count, size_bytes, worst_time = run_benchmark(args)
    duration = delta_sec(start)
    size_mb = size_bytes / 1024.0 / 1024.0
    print "Overall, sent %.2fMB: %d records in %d requests in %.2f seconds: %.2fMB/s, %.2f reqs/s, %.2f records/s" % (size_mb, record_count, request_count, duration, size_mb / duration, request_count / duration, record_count / duration)
    print "Worst time was %.2fms" % worst_time

if __name__ == "__main__":
    sys.exit(main())
