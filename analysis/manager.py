#!/usr/bin/env python
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from multiprocessing import Queue, cpu_count, active_children
from threading import Thread, Lock
from boto.sqs import connect_to_region as sqs_connect
from boto.sqs.jsonmessage import JSONMessage
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from worker import AnalysisWorker
from traceback import print_exc
from shutil import rmtree
from downloader import DownloaderProcess
from time import sleep
import os, sys
from utils import mkdirp

NUMBER_DOWNLOADERS = 6

IDLE_WAIT_BEFORE_SHUTDOWN = 5 * 60

class ActiveCounter:
    """" Auxiliary counter that tracks how many processes are active """
    def __init__(self):
        self.counter = 0
        self.lock = Lock()

    def increment(self):
        self.lock.acquire()
        self.counter += 1
        self.lock.release()

    def decrement(self):
        self.lock.acquire()
        self.counter -= 1
        self.lock.release()

    def count(self):
        self.lock.acquire()
        retval = self.counter
        self.lock.release()
        return retval

class Manager(Thread):
    def __init__(self, aws_cred, work_dir, sqs_input_queue, index, activeCount):
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
        self.activeCount = activeCount

    def run(self):
        # Connect to SQS input queue
        self.sqs = sqs_connect(self.aws_region, **self.aws_cred)
        self.sqs_input_queue = self.sqs.get_queue(self.sqs_input_queue_name)
        self.sqs_input_queue.set_message_class(JSONMessage)

        while True:
            msgs = self.sqs_input_queue.get_messages(
                num_messages        = 1,
                wait_time_seconds   = 19,
                attributes          = ['ApproximateReceiveCount']
            )
            print "Fetched %i messages" % len(msgs)
            if len(msgs) > 0:
                try:
                    self.activeCount.increment()
                    msg = msgs[0]
                    retries = int(msg.attributes['ApproximateReceiveCount'])
                    if retries >= 5:
                        self.abort_message(msg)
                        print "Aborted message after %s retries, msg-id: %s" % (retries, msg['id'])
                    else:
                        self.process_message(msg)
                        print "Message processed successfully, msg-id: %s" % msg['id']
                    self.activeCount.decrement()
                except:
                    raise
                finally:
                    self.activeCount.decrement()
            else:
                sleep(23)

    def abort_message(self, msg):
        """ Don't try to execute the message again, just abort it """
        # Delete message
        self.sqs_input_queue.delete_message(msg)

        # Get output SQS queue
        target_queue = self.sqs.get_queue(msg["target-queue"])
        target_queue.set_message_class(JSONMessage)

        m = target_queue.new_message(body = {
            'id':               msg['id'],
            'name':             msg['name'],
            'owner':            msg['owner'],
            'code':             msg['code'],
            'target-queue':     msg['target-queue'],
            'files':            msg['files'],
            'size':             msg['size'],
            'target-prefix':    None
        })
        target_queue.write(m)

    def process_message(self, msg):
        # Create queues for communication
        download_queue = Queue()
        processing_queue = Queue()
        upload_queue = Queue()

        # Ensure workdir exits
        mkdirp(self.work_dir)

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
            val = upload_queue.get(timeout = 30 * 60)
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

            m = target_queue.new_message(body = {
                'id':               msg['id'],
                'name':             msg['name'],
                'owner':            msg['owner'],
                'code':             msg['code'],
                'target-queue':     msg['target-queue'],
                'files':            msg['files'],
                'size':             msg['size'],
                'target-prefix':    target_prefix
            })
            target_queue.write(m)

        # Kill all downloaders
        for downloader in downloaders:
            downloader.terminate()
        # Kill worker
        worker.terminate()

        # Clear work folder
        rmtree(self.work_dir, ignore_errors = True)

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

    activeCount = ActiveCounter()

    for index in xrange(0, nb_workers):
        manager = Manager(aws_cred, cfg.work_dir, cfg.queue, index, activeCount)
        manager.start()

    # TODO: Figure out how to reenable this... we probably can't do it with
    #       AWS autoscaling groups as they might bring up the instances again.
    #       At least this needs more investigation...
    #idle_wait = IDLE_WAIT_BEFORE_SHUTDOWN
    #while True:
    #    sleep(47)
    #    if activeCount.count() == 0:
    #        idle_wait -= 47
    #    else:
    #        idle_wait = IDLE_WAIT_BEFORE_SHUTDOWN
    #    if idle_wait <= 0:
    #        for child in active_children():
    #            child.terminate()
    #        sys.exit(0)


if __name__ == "__main__":
    try:
        main()
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