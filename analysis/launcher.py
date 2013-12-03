from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto import sqs
from boto.sqs.jsonmessage import JSONMessage
from uuid import uuid4
import json
from telemetry.telemetry_schema import TelemetrySchema
import sys

TASK_TIMEOUT = 60 * 60

class AnalysisJob:
    def __init__(self, cfg):
        self.job_bundle = cfg.job_bundle
        self.input_filter = TelemetrySchema(json.load(open(cfg.input_filter)))
        self.job_id = str(uuid4())
        self.target_queue = cfg.target_queue
        self.aws_key = cfg.aws_key
        self.aws_secret_key = cfg.aws_secret_key
        self.input_bucket = "telemetry-published-v1"
        self.job_name = cfg.name
        self.job_owner = cfg.owner

        # Bucket with intermediate data for this analysis job
        self.analysis_bucket = "jonasfj-telemetry-analysis"

        self.s3_code_path = "batch-jobs/" + self.job_id + ".egg"

        # S3 region of operation
        self.aws_region = "us-west-2"
        self.task_size_limit = 400 * 1024 * 1024
        self.sqs_input_name = cfg.sqs_queue


    def get_filtered_files(self):
        conn = S3Connection(self.aws_key, self.aws_secret_key)
        bucket = conn.get_bucket(self.input_bucket)
        for k,s in self.list_partitions(bucket):
            if k.split('/')[-1].split('.')[1] < '20131104':
                yield (k, s)

    def get_filtered_files_old(self):
        """ Get tuples of name and size for all input files """
        # Setup some auxiliary functions
        allowed_values = self.input_filter.sanitize_allowed_values()
        nb_dims = len(allowed_values)
        def filter_includes(level, value):
            return self.input_filter.is_allowed(value, allowed_values[level])

        # iterate over all files in bucket, this is very slow and we should be
        # be able to something much smarter using prefix listing and ordering
        # to break listing.
        count = 0
        selected = 0
        conn = S3Connection(self.aws_key, self.aws_secret_key)
        bucket = conn.get_bucket(self.input_bucket)
        for f in bucket.list():
            count += 1
            dims = self.input_filter.get_dimensions(".", f.key)
            include = True
            for i in xrange(nb_dims):
                if not filter_includes(i, dims[i]):
                    include = False
                    break
            if include:
                selected += 1
                yield (f.key, f.size)
            if count % 5000 == 0:
                print "%i files listed with %i selected" % (count, selected)
        conn.close()

    def list_partitions(self, bucket, prefix='', level=0):
        #print "Listing...", prefix, level
        allowed_values = self.input_filter.sanitize_allowed_values()
        delimiter = '/'
        if level > 3:
            delimiter = '.'
        for k in bucket.list(prefix=prefix, delimiter=delimiter):
            partitions = k.name.split("/")
            if level > 3:
                # split the last couple of partition components by "." instead of "/"
                partitions.extend(partitions.pop().split(".", 2))
            if self.input_filter.is_allowed(partitions[level], allowed_values[level]):
                if level >= 5:
                    for f in bucket.list(prefix=k.name):
                        yield (f.key, f.size)
                else:
                    for k, s in self.list_partitions(bucket, k.name, level + 1):
                        yield (k, s)


    def generate_tasks(self):
        """ Generates SQS tasks, we batch small files into a single task """
        uid = str(uuid4())
        taskid = 1
        taskfiles = []
        tasksize = 0
        total_size_of_all = 0
        for key, size in self.get_filtered_files():
            # If the task have reached desired size we yield it
            # Note, as SQS messages are limited to 65 KiB we limit tasks to
            # 100 filenames, for simplicity
            # boto only uses signature version 4, hence, we're limited to 65 KiB
            if 0 < len(taskfiles) and (tasksize + size > self.task_size_limit or
                                       len(taskfiles) > 200):
                # Reduce to only filenames, sort by size... smallest first they are
                # faster to download when handling the job
                taskfiles =  [f for f,s in sorted(taskfiles, key=lambda (f,s): s)]
                yield {
                    'id':               uid + "/" +  str(taskid),
                    'name':             self.job_name,
                    'owner':            self.job_owner,
                    'code':             self.s3_code_path,
                    'target-queue':     self.target_queue,
                    'files':            taskfiles,
                    'size':             tasksize
                }
                total_size_of_all += tasksize
                print "%i tasks created acc. size: %s" % (taskid, total_size_of_all)
                taskid += 1
                taskfiles = []
                tasksize = 0
            tasksize += size
            taskfiles.append((key, size))
        if len(taskfiles) > 0:
            taskfiles =  [f for f,s in sorted(taskfiles, key=lambda (f,s): s)]
            yield {
                'id':               uid + "/" + str(taskid),
                'name':             self.job_name,
                'owner':            self.job_owner,
                'code':             self.s3_code_path,
                'target-queue':     self.target_queue,
                'files':            taskfiles,
                'size':             tasksize
            }
        print "Finished:"
        print "%i tasks created total size: %s" % (taskid, total_size_of_all + tasksize)

    def put_sqs_tasks(self):
        """ Create an SQS tasks for this analysis job """
        print "Populate SQS input queue with tasks"
        # Connect to SQS is desired region
        conn = sqs.connect_to_region(
            self.aws_region,
            aws_access_key_id = self.aws_key,
            aws_secret_access_key = self.aws_secret_key
        )
        # Create queue
        queue = conn.get_queue(self.sqs_input_name)
        queue.set_message_class(JSONMessage)
        # Populate queue with tasks
        for task in self.generate_tasks():
            msg = queue.new_message(body = task)
            queue.write(msg)
        conn.close()

    def setup(self):
        self.upload_job_bundle()
        self.put_sqs_tasks()
        print "Uploaded with job_id: %s" % self.job_id

    def upload_job_bundle(self):
        """ Upload job bundle to S3 """
        conn = S3Connection(self.aws_key, self.aws_secret_key)
        bucket = conn.get_bucket(self.analysis_bucket)
        k = Key(bucket)
        k.key = self.s3_code_path
        k.set_contents_from_filename(self.job_bundle)
        conn.close()


def main():
    p = ArgumentParser(
        description = 'Run analysis job',
        formatter_class = ArgumentDefaultsHelpFormatter
    )
    p.add_argument(
        "job_bundle",
        help = "The analysis bundle to run"
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
        "-n", "--name",
        help = "Job name",
        required = True
    )
    p.add_argument(
        "-o", "--owner",
        help = "Job owner, should be an @mozilla.com email address",
        required = True
    )
    p.add_argument(
        "-f", "--input-filter",
        help = "File containing filter spec",
        required = True
    )
    p.add_argument(
        "-t", "--target-queue",
        help = "SQS queue for communicating finished tasks",
        required = True
    )
    p.add_argument(
        "-q", "--sqs-queue",
        help = "SQS input queue for analysis worker stack",
        required = True
    )
    job = AnalysisJob(p.parse_args())
    job.setup()

if __name__ == "__main__":
    sys.exit(main())
