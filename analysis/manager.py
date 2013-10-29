#!/usr/bin/env python
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from multiprocessing import Queue, cpu_count, active_children
from threading import Thread
from boto.sqs import connect_to_region as sqs_connect
from boto.sqs.jsonmessage import JSONMessage
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from worker import AnalysisWorker
from traceback import print_exc
from downloader import DownloaderProcess
import os, sys

NUMBER_DOWNLOADERS = 2

class Manager(Thread):
    def __init__(self, aws_cred, work_dir, sqs_input_queue, index):
        super(Manager, self).__init__()
        self.aws_cred = aws_cred
        self.work_dir = os.path.join(work_dir, "process-%i" % index)
        self.aws_region = "us-west-2"
        self.analysis_bucket_name = "jonasfj-telemetry-analysis"
        self.sqs_input_queue_name = sqs_input_queue
        self.s3 = S3Connection(**aws_cred)
        self.analysis_bucket = self.s3.get_bucket(
            self.analysis_bucket_name,
            validate = False
        )

    def run(self):
        # Connect to SQS input queue
        self.sqs = sqs_connect(self.aws_region, **self.aws_cred)
        self.sqs_input_queue = self.sqs.get_queue(self.sqs_input_queue_name)
        self.sqs_input_queue.set_message_class(JSONMessage)

        while True:
            msgs = self.sqs_input_queue.get_messages(num_messages = 1)
            if len(msgs) > 0:
                self.process_message(msgs[0])


    def process_message(self, msg):
        # Create queues for communication
        download_queue = Queue()
        processing_queue = Queue()
        upload_queue = Queue()

        # Create downloaders
        downloaders = []
        for i in xrange(0, NUMBER_DOWNLOADERS):
            downloader = DownloaderProcess(
                download_queue, processing_queue,
                os.path.join(self.work_dir, "downloader-%i" % i), self.aws_cred
            )
            downloaders.append(downloader)
            downloader.start()

        # Give files to downloaders
        for f in msg["files"]:
            download_queue.put(f)

        # Create analysis worker
        job_bundle_reference = (self.analysis_bucket_name, msg["code"])
        worker = AnalysisWorker(
            job_bundle_reference, len(msg["files"]), self.aws_cred,
            processing_queue, upload_queue,
            os.path.join(self.work_dir, "analysis-worker")
        )
        worker.start()

        # Upload result to S3
        target_prefix = "output/" + msg["id"] + "/"

        # TODO multi-process uploaders
        success = False
        while True:
            val = upload_queue.get()
            if type(val) is bool:
                success = val
                break
            path, prefix = val
            k = Key(self.analysis_bucket)
            k.key = target_prefix + prefix
            k.set_contents_from_filename(path)

        if success:
            # Delete message
            self.sqs_input_queue.delete_message(msg)

            # Get output SQS queue
            target_queue = self.sqs.get_queue(msg["target-queue"])
            target_queue.set_message_class(JSONMessage)

            m = target_queue.new_message(body = {'id': msg["id"]})
            target_queue.write(m)

        # Kill all downloaders
        for downloader in downloaders:
            downloader.terminate()

def main():
    p = ArgumentParser(
        description = 'Run analysis workers',
        formatter_class = ArgumentDefaultsHelpFormatter
    )
    p.add_argument(
        "-q", "--queue",
        help = "SQS input queue",
        required = True
    )
    p.add_argument(
        "-k", "--aws-key",
        help = "AWS Key"
    )
    p.add_argument(
        "-s", "--aws-secret-key",
        help = "AWS Secret Key"
    )
    p.add_argument(
        "-w", "--work-dir",
        help = "Location to put temporary work files",
        required = True
    )
    p.add_argument(
        "-j", "--nb-workers",
        help = "Number of parallel workers",
        default = "cpu-count"
    )
    cfg = p.parse_args()

    nb_workers = None
    try:
        nb_workers = int(cfg.nb_workers)
    except ValueError:
        nb_workers = cpu_count()

    aws_cred = {
        'aws_access_key_id':        cfg.aws_key,
        'aws_secret_access_key':    cfg.aws_secret_key
    }

    for index in xrange(0, nb_workers):
        manager = Manager(aws_cred, cfg.work_dir, cfg.queue, index)
        manager.start()


if __name__ == "__main__":
    retval = 0
    try:
        retval = main()
    except KeyboardInterrupt:
        print >> sys.stderr, "Exit requested by user"
        raise
    except:
        print >> sys.stderr, "Failed job, cleaning up after this:"
        print_exc(file = sys.stderr)
        raise
    finally:
        # Terminate all children
        for child in active_children():
            child.terminate()
    sys.exit(retval)