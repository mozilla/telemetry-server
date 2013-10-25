import argparse
from multiprocessing import Queue, Process
from traceback import print_exc
from downloader import DownloaderProcess
import os, sys, shutil
from boto import sqs
from boto.sqs.jsonmessage import JSONMessage
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from subprocess import Popen, PIPE
from zipimport import zipimporter
import errno

def mkdirp(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST or not os.path.isdir(path):
            raise

NUMBER_OF_DOWNLOADERS = 4

class AnalysisWorker(Process):
    """ Analysis worker that finishes tasks from SQS """
    def __init__(self, aws_key, aws_secret_key, work_dir):
        super(AnalysisWorker, self).__init__()
        self.aws_key = aws_key
        self.aws_secret_key = aws_secret_key
        self.work_folder = work_dir
        self.download_queue = Queue()
        self.processing_queue = Queue()
        self.input_folder = os.path.join(self.work_folder, "input")
        self.output_folder = os.path.join(self.work_folder, "output")

        # Bucket with intermediate data for this analysis job
        self.analysis_bucket_name = "jonasfj-telemetry-analysis"

        # S3 region of operation
        self.aws_region = "us-west-2"
        self.sqs_input_name = "telemetry-analysis-input"

    def setup(self):
        print "Worker setting up"
        # Remove work folder, no failures allowed
        if os.path.exists(self.work_folder):
            shutil.rmtree(self.work_folder, ignore_errors = False)

        # Create folders as needed
        mkdirp(self.input_folder)
        mkdirp(self.output_folder)

        # Launch two downloader processes
        self.downloaders = []
        for i in xrange(0, NUMBER_OF_DOWNLOADERS):
            d = DownloaderProcess(self.download_queue, self.processing_queue,
                                  self.input_folder,
                                  self.aws_key, self.aws_secret_key)
            self.downloaders.append(d)
            d.start()

        # Connect to SQS
        self.sqs_conn = sqs.connect_to_region(
            self.aws_region,
            aws_access_key_id = self.aws_key,
            aws_secret_access_key = self.aws_secret_key
        )
        self.sqs_input_queue = self.sqs_conn.get_queue(self.sqs_input_name)
        self.sqs_input_queue.set_message_class(JSONMessage)

        # Connect to S3
        self.s3_conn = S3Connection(self.aws_key, self.aws_secret_key)
        self.analysis_bucket = self.s3_conn.get_bucket(self.analysis_bucket_name)

    def run(self):
        try:
            self.setup()
            msgs = self.sqs_input_queue.get_messages(num_messages = 1)
            if len(msgs) > 0:
                self.process_message(msgs[0])
        except:
            print >> sys.stderr, "Failed job, cleaning up after this:"
            print_exc(file = sys.stderr)
        finally:
            self.teardown()

    def teardown(self):
        # Murder downloaders
        for d in self.downloaders:
            d.terminate()

        # Remove work folder, as best possible
        shutil.rmtree(self.work_folder, ignore_errors = True)

        # Close service connections
        self.sqs_conn.close()
        self.s3_conn.close()

    def process_message(self, msg):
        # Start downloading all files
        for f in msg["files"]:
            self.download_queue.put(f)

        print "Processing Message"
        print "id:              %s" % msg["id"]
        print "code:            %s" % msg["code"]
        print "files:           %i" % len(msg["files"])
        print "size:            %s" % msg["size"]
        print "target-queue:    %s" % msg["target-queue"]

        # Download the job bundle
        job_bundle_target = os.path.join(self.work_folder, "job.zip")
        k = Key(self.analysis_bucket)
        k.key = msg["code"]
        k.get_contents_to_filename(job_bundle_target)

        # zipimport job bundle
        proc_module = zipimporter(job_bundle_target).load_module("processor")
        processor = proc_module.Processor()
        processor.set_output_folder(self.output_folder)

        # maintain set of files
        fileset = set(msg["files"])

        # Wait for downloads to finish until, all files are processed
        while len(fileset) != 0:
            (path, fileentry) = self.processing_queue.get()
            print "Processing %s" % fileentry
            if fileentry in fileset and path != None:
                self.process_file(processor, path)
            fileset.remove(fileentry)

        # Ask processor to write output
        processor.write_output()
        processor.clear_state()

        # Upload result to S3
        target_prefix = "output/" + msg["id"] + "/"

        #TODO multi-process uploaders like downloaders
        k = Key(self.analysis_bucket)
        for path, folder, files in os.walk(self.output_folder):
            for f in files:
                k.key = target_prefix + os.path.relpath(os.path.join(path, f), self.output_folder)
                k.set_contents_from_filename(os.path.join(path, f))

        # Delete SQS message
        self.sqs_input_queue.delete_message(msg)

        # Get output SQS queue
        target_queue = self.sqs_conn.get_queue(msg["target-queue"])
        target_queue.set_message_class(JSONMessage)

        m = target_queue.new_message(body = {'id': msg["id"]})
        target_queue.write(m)

        print "Finished task: %s" % msg["id"]

    def process_file(self, processor, path):
        self.open_compressor(path)
        line_nb = 0
        for line in self.decompressor.stdout:
            line_nb += 1
            try:
                key, value = line.split("\t", 1)
                processor.scan(key, value)
            except:
                print >> sys.stderr, ("Bad input line: %i of %s" %
                                      (line_nb, self.filename))
                print_exc(file = sys.stderr)
        self.close_compressor()


    def open_compressor(self, path):
        self.raw_handle = open(path, "rb")
        self.decompressor = Popen(
            ['xz', '-d', '-c'],
            bufsize = 65536,
            stdin = self.raw_handle,
            stdout = PIPE,
            stderr = sys.stderr
        )

    def close_compressor(self):
        self.decompressor.stdout.close()
        self.raw_handle.close()
        #if self.decompressor.poll() != 0:
        #    print >> sys.stderr, "decompressor exited: %s" % self.decompressor.returncode
        #    self.decompressor.kill()
        self.decompressor = None
        self.raw_handle = None


