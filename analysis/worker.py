#!/usr/bin/env python
try:
    import simplejson as json
except ImportError:
    import json
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from multiprocessing import Process, Queue
from traceback import print_exc
from subprocess import Popen, PIPE
from utils import mkdirp
from boto.s3.connection import S3Connection
from shutil import rmtree, copyfile, copytree
from pkg_resources import WorkingSet
from time import sleep
import tarfile
import os, sys

class AnalysisWorker(Process):
    """
        Analysis worker that processes files from input_queue and
        adds to output_queue when nb_files have been received
    """
    def __init__(self, job_bundle, nb_files, aws_cred, input_queue, output_queue,
                 work_dir):
        super(AnalysisWorker, self).__init__()
        self.job_bundle_bucket, self.job_bundle_prefix = job_bundle
        self.aws_cred = aws_cred
        self.work_folder = work_dir
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.output_folder = os.path.join(self.work_folder, "output")
        self.nb_files = nb_files

    def setup(self):
        # Remove work folder, no failures allowed
        if os.path.exists(self.work_folder):
            rmtree(self.work_folder, ignore_errors = False)

        # Create work folder
        mkdirp(self.work_folder)
        mkdirp(self.output_folder)

        job_bundle_target = os.path.join(self.work_folder, "job_bundle.tar.gz")
        # If job_bundle_bucket is None then the bundle is stored locally
        if self.job_bundle_bucket == None:
            copyfile(self.job_bundle_prefix, job_bundle_target)
        else:
            s3 = S3Connection(**self.aws_cred)
            bucket = s3.get_bucket(self.job_bundle_bucket, validate = False)
            key = bucket.get_key(self.job_bundle_prefix)
            key.get_contents_to_filename(job_bundle_target)

        # Extract job_bundle
        self.processor_path = os.path.join(self.work_folder, "code")
        mkdirp(self.processor_path)
        tar = tarfile.open(job_bundle_target)
        tar.extractall(path = self.processor_path)
        tar.close()

        # Create processor
        self.processor = Popen(
            ['./processor', os.path.relpath(self.output_folder, self.processor_path)],
            cwd = self.processor_path,
            bufsize = 1,
            stdin = PIPE,
            stdout = sys.stdout,
            stderr = sys.stderr
        )

    def run(self):
        try:
            self.setup()
            while self.nb_files > 0:
                virtual_name, path = self.input_queue.get()
                if path != None:
                    self.process_file(virtual_name, path)
                self.nb_files -= 1
            self.finish()
        except:
            print >> sys.stderr, "Failed job, cleaning up after this:"
            print_exc(file = sys.stderr)
            self.output_queue.put(False)

    def finish(self):
        # Ask processor to write output
        self.processor.stdin.close()
        self.processor.wait()

        # Check return code
        if self.processor.returncode == 0:
            # Put output files to uploaders
            for path, folder, files in os.walk(self.output_folder):
                for f in files:
                    source = os.path.join(path, f)
                    target = os.path.relpath(os.path.join(path, f), self.output_folder)
                    self.output_queue.put((source, target))

            # Put finished message
            self.output_queue.put(True)
        else:
            print >> sys.stderr, "Processor exited non-zero, task failed"
            self.output_queue.put(False)

    def process_file(self, prefix, path):
        path = os.path.relpath(path, self.processor_path)
        self.processor.stdin.write("%s\t%s\n" % (prefix, path))

def main():
    """ Run the worker with a job_bundle on a local input-file for debugging """
    p = ArgumentParser(
        description = 'Debug analysis script',
        formatter_class = ArgumentDefaultsHelpFormatter
    )
    p.add_argument(
        "job_bundle",
        help = "The analysis bundle to run"
    )
    p.add_argument(
        "-f", "--input",
        help = "File with 'prefix <TAB> path' for files to process"
    )
    p.add_argument(
        "-w", "--work-dir",
        help = "Location to put temporary work files"
    )
    cfg = p.parse_args()

    # Get a clean work folder
    rmtree(cfg.work_dir, ignore_errors = False)

    # Create work directories
    work_dir = os.path.join(cfg.work_dir, "work-folder")
    data_dir = os.path.join(cfg.work_dir, "data-folder")
    mkdirp(work_dir)
    mkdirp(data_dir)

    # Setup queues
    input_queue = Queue()
    output_queue = Queue()

    # Put input files in queue
    nb_files = 0
    with open(cfg.input, 'r') as input:
        for line in input:
            prefix, path = line.strip().split('\t')
            source = os.path.join(data_dir, "file-%i" % nb_files)
            copyfile(path, source)
            input_queue.put((prefix, source))
            nb_files += 1

    # The empty set of AWS credentials
    aws_cred = {
        'aws_access_key_id':        None,
        'aws_secret_access_key':    None
    }

    # Job bundle is stored locally
    job_bundle = (None, cfg.job_bundle)

    # Start analysis worker
    worker = AnalysisWorker(job_bundle, nb_files, aws_cred,
                            input_queue, output_queue,
                            work_dir)
    worker.start()

    # Print messages from worker
    while True:
        msg = output_queue.get()
        if msg is True:
            print "Done with success"
            break
        elif msg is False:
            print "Done with failure"
            break
        else:
            print "Upload: %s => %s" % msg

    # Give it time to shutdown correctly
    sleep(0.5)

    # Terminate worker and join
    worker.terminate()
    worker.join()
    print "Worker finished: %s" % worker.exitcode

if __name__ == "__main__":
    sys.exit(main())