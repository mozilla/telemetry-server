from multiprocessing import Process
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import os, sys
from traceback import print_exc

class DownloaderProcess(Process):
    """ Worker process that download files from queue to folder """
    def __init__(self, input_queue, output_queue,
                       work_folder,
                       aws_key, aws_secret_key):
        super(DownloaderProcess, self).__init__()
        self._input_queue = input_queue
        self._output_queue = output_queue
        self._work_folder = work_folder
        self._input_bucket = "telemetry-published-v1"
        self._aws_key = aws_key
        self._aws_secret_key = aws_secret_key
        self._conn = S3Connection(self._aws_key, self._aws_secret_key)
        self._bucket = self._conn.get_bucket(self._input_bucket)

    def run(self):
        while True:
            filepath = self._input_queue.get()
            self.download(filepath)

    def download(self, filepath):
        # Get filename from path
        filename = os.path.basename(filepath)
        # Get target filepath
        target = os.path.join(self._work_folder, filename)
        # Download file
        retries = 1
        success = False
        while retries < 3:
            try:
                k = Key(self._bucket)
                k.key = filepath
                k.get_contents_to_filename(target)
                success = True
                break
            except:
                retries += 1
                print >> sys.stderr, "Error on %i'th try:" % retries
                print_exc(file = sys.stderr)

        if success:
            # Put file to output query
            self._output_queue.put((target, filepath))
        else:
            print >> sys.stderr, "Failed to download: %s" % filepath
            self._output_queue.put((None, filepath))