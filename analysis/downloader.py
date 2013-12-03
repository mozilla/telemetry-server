from multiprocessing import Process
from boto.s3.connection import S3Connection
from traceback import print_exc
from utils import mkdirp
import os, sys

class DownloaderProcess(Process):
    """ Worker process that download files from queue to folder """
    def __init__(self, input_queue, output_queue,
                       work_folder, aws_cred):
        super(DownloaderProcess, self).__init__()
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.work_folder = work_folder
        mkdirp(self.work_folder)
        self.input_bucket = "telemetry-published-v1"
        self.aws_cred = aws_cred
        self.s3 = S3Connection(**self.aws_cred)
        self.bucket = self.s3.get_bucket(self.input_bucket, validate = False)

    def run(self):
        while True:
            prefix = self.input_queue.get()
            self.download(prefix)

    def download(self, prefix):
        # Get filename from prefix
        filename = os.path.basename(prefix)
        # Get target path
        target = os.path.join(self.work_folder, filename)
        # Download file
        retries = 1
        success = False
        while retries < 3:
            try:
                k = self.bucket.get_key(prefix)
                k.get_contents_to_filename(target)
                success = True
                break
            except:
                retries += 1
                print >> sys.stderr, "Error on %i'th try:" % retries
                print_exc(file = sys.stderr)

        if success:
            # Put file to output query
            self.output_queue.put((prefix, target))
        else:
            print >> sys.stderr, "Failed to download: %s" % prefix
            self.output_queue.put((prefix, None))