#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import argparse
import random
import math
import uuid
import time
import multiprocessing
from multiprocessing import Process, Queue
import Queue as Q
import simplejson as json
import imp
import sys
import os
import signal
from datetime import date, datetime
from multiprocessing import Process
import subprocess
from subprocess import Popen, PIPE
from boto.s3.connection import S3Connection

class InterruptProcessingError(Exception):
    def __init__(self, msg):
        self.msg = msg

def debug(message):
    print message

def handle_sighup(signum, frame):
    debug("Caught signal " + str(signum))
    if signum == signal.SIGHUP:
        raise InterruptProcessingError("Time to clean up")

def wait_for(processes, label):
    for p in processes:
        p.join()

class InterruptibleWorker():
    def __init__(self, num, num_workers, args):
        signal.signal(signal.SIGHUP, handle_sighup)
        self.pid = os.getpid()
        self.num = num
        self.num_workers = num_workers
        self.args = args
        self.work_dir = self.args.work_dir
        self.incoming_dir = os.path.join(self.work_dir, "incoming")
        self.downloading_dir = os.path.join(self.work_dir, "downloading")
        self.uploading_dir = os.path.join(self.work_dir, "uploading")
        self.upload_pending_dir = os.path.join(self.work_dir, "upload_pending")

        # Add a small delay to encourage workers not to step on each other.
        sleepy = random.uniform(0.0, 0.1)
        self.log("delaying init by %.04f seconds" % (sleepy))
        time.sleep(sleepy)

        self.setup()
        self.run_interruptibly()

    def setup(self):
        pass

    def log(self, message):
        debug("%s %d: %d: %s" % (self.__class__.__name__, self.num, self.pid, message))

    def run_interruptibly(self):
        try:
            self.run()
        except (InterruptProcessingError, KeyboardInterrupt):
            self.interrupt()
            self.shutdown()

    def interrupt(self):
        self.log("Interrupted")

    def shutdown(self):
        pass

class DownloaderStep(InterruptibleWorker):
    def run(self):
        # While there are X < num_workers files in "incoming/", download one
        # to "downloading/" then move it to "incoming/"
        while True:
            num_incoming = self.count_incoming(self.incoming_dir)
            if num_incoming < self.num_workers:
                self.download_one(self.downloading_dir, self.incoming_dir)
            else:
                self.log("There are already " + str(num_incoming) + " incoming... Sleeping")
                time.sleep(10)

    def count_incoming(self, incoming_dir):
        for (path, dirs, files) in os.walk(incoming_dir):
            # We don't need to recurse - files go straight into the base dir.
            return len(files)

    def download_one(self, downloading_dir, incoming_dir):
        available_file = self.get_available_filename()
        if available_file is None:
            self.log("There were no available files... Sleeping")
            time.sleep(5)
            return
        downloadable = os.path.join(downloading_dir, available_file)
        # TODO: get it from S3
        dfile = open(downloadable, "a")
        dfile.write("hello")
        dfile.close()
        self.finalize_downloaded(downloading_dir, incoming_dir, available_file)

    def get_available_filename(self):
        # TODO: consult the "available files" queue for a filename
        return "incoming." + uuid.uuid4().hex

    def finalize_downloaded(self, downloading_dir, incoming_dir, filename):
        source = os.path.join(downloading_dir, filename)
        dest = os.path.join(incoming_dir, filename)
        os.rename(source, dest)

class UploaderStep(InterruptibleWorker):
    def run(self):
        # TODO: while there are files in <work>/upload, move one to uploading/
        #       and upload it.  Delete when finished.
        while True:
            self.log("Sleeping")
            time.sleep(60)

    def shutdown(self):
        # TODO: Upload any lingering files in <work>/upload
        self.log("Shutting down")


class WorkerStep(InterruptibleWorker):
    def setup(self):
        self.worker_base = os.path.join(self.args.work_dir, "workers", str(self.num))
        self.worker_input_dir = os.path.join(self.worker_base, "input")

    def run(self):
        # TODO: while there are files in <work>/incoming, move one of them to
        #       <work>/workers/i/input, process it, then delete it.
        while True:
            filename = self.claim_incoming_file(self.incoming_dir, self.worker_input_dir)
            if filename is None:
                self.log("No files to claim... Sleeping")
                time.sleep(10)
            else:
                self.log("Working on file: " + filename)
                # TODO: process it.
                time.sleep(5)

    def claim_incoming_file(self, source_dir, dest_dir):
        for (path, dirs, files) in os.walk(source_dir):
            for f in files:
                try:
                    # We may fail when another process is competing to claim it
                    os.rename(os.path.join(source_dir, f), os.path.join(dest_dir, f))
                    return f
                except OSError, e:
                    if e.errno == 2:
                        print "Failed to claim", f, "Someone else got it first.", e
                    else:
                        raise
        return None

    def shutdown(self):
        # TODO: close all files and compressor contexts, compress any
        #       in-progress files and move them to upload/
        self.log("Shutting down")

def start_workers(count, num_workers, name, clazz, args):
    workers = []
    for i in range(count):
        w = Process(
                target=clazz,
                args=(i, num_workers, args))
        workers.append(w)
        w.start()
        debug(name + str(i) + " pid: " + str(w.pid))
    debug(name + "s all started")
    return workers

def signal_workers(workers, label, sig=signal.SIGHUP):
    try:
        debug("Signaling " + label + " to stop")
        for w in workers:
            os.kill(w.pid, sig)
        debug("Waiting for " + label + "...")
        wait_for(workers, label)
        debug(label + " complete.")
    except KeyboardInterrupt:
        debug("Keyboard interrupt while signaling " + label + ", shutting down NOW")
        for w in workers:
            w.terminate()

def main():
    parser = argparse.ArgumentParser(description='Process incoming Telemetry data', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("incoming_bucket", help="The S3 bucket containing incoming files")
    parser.add_argument("publish_bucket", help="The S3 bucket to save processed files")
    parser.add_argument("-k", "--aws-key", help="AWS Key", required=True)
    parser.add_argument("-s", "--aws-secret-key", help="AWS Secret Key", required=True)
    parser.add_argument("-w", "--work-dir", help="Location to cache downloaded files", required=True)
    parser.add_argument("-i", "--incoming-queue", help="Queue containing incoming data", required=True)
    args = parser.parse_args()

    # TODO: should we clean up the "workers" dir before getting started?
    if not os.path.isdir(args.work_dir):
        os.makedirs(args.work_dir)

    # Overall disk structure:
    #  * work_dir/downloading/       files being downloaded (incomplete)
    #  * work_dir/upload_pending/    files ready to be uploaded
    #  * work_dir/uploading/         files currently being uploaded
    #  * work_dir/incoming/          files ready to be processed (complete)
    #  * work_dir/workers/           base dir for per-worker files
    #  * work_dir/workers/i/         base dir for worker i's files
    #  * work_dir/workers/i/input/   input files being processed by worker i
    #  * work_dir/workers/i/work/    output files produced by worker i
    #  * work_dir/workers/i/log/     log output produced by worker i
    for d in [ "downloading", "upload_pending", "uploading", "incoming", "workers" ]:
        pd = os.path.join(args.work_dir, d)
        if not os.path.isdir(pd):
            os.makedirs(pd)

    num_cpus = multiprocessing.cpu_count()
    num_workers = num_cpus
    debug("Using " + str(num_workers) + " workers")
    num_downloaders = int(math.ceil(num_cpus / 2))
    debug("Using " + str(num_downloaders) + " downloaders")
    num_uploaders = num_downloaders
    debug("Using " + str(num_uploaders) + " uploaders")

    for i in range(num_cpus):
        wd = os.path.join(args.work_dir, "workers", str(i))
        if not os.path.isdir(wd):
            os.makedirs(wd)
        for d in [ "work", "input", "log" ]:
            wsd = os.path.join(wd, d)
            if not os.path.isdir(wsd):
                os.makedirs(wsd)

    # Handle SIGHUP
    signal.signal(signal.SIGHUP, handle_sighup)

    # Start downloaders
    downloaders = start_workers(num_downloaders, num_workers, "Downloader", DownloaderStep, args)

    # Start uploaders
    uploaders = start_workers(num_uploaders, num_workers, "Uploader", UploaderStep, args)

    # Start workers
    # TODO: workers should be actual processes launched with os.spawnl(os.P_NOWAIT...)
    workers = start_workers(num_workers, num_workers, "Worker", WorkerStep, args)

    # TODO: watch for SIGHUP
    while True:
        try:
            debug("Master process " + str(os.getpid()) + " waiting for SIGHUP...")
            time.sleep(60)
        except InterruptProcessingError, ipe:
            debug("Received SIGHUP, shutting down gracefully")
            break
        except KeyboardInterrupt, ke:
            debug("Received keyboard interrupt, shutting down gracefully")
            break

    # Stop downloaders abruptly:
    for d in downloaders:
        d.terminate()
        d.join()

    # Signal workers and uploaders to stop nicely:
    signal_workers(workers, "workers")
    signal_workers(uploaders, "uploaders")

if __name__ == "__main__":
    sys.exit(main())
