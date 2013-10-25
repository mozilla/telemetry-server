import argparse
from multiprocessing import Queue, cpu_count
from threading import Thread
from worker import AnalysisWorker
import os, sys

class Manager(Thread):
    def __init__(self, aws_key, aws_secret, work_dir, index):
        super(Manager, self).__init__()
        self.aws_key = aws_key
        self.aws_secret = aws_secret
        self.work_dir = os.path.join(work_dir, "worker-%i" % index)

    def run(self):
        while True:
            # A manager is essential a guy who creates/hires a worker
            worker = AnalysisWorker(self.aws_key, self.aws_secret, self.work_dir)
            # Puts the worker to work
            worker.start()
            # Sit's back and wait for the worker to die
            worker.join() #TODO, timeout and kill worker process tree, also only retry failed sqs messages 3 times
            # Then goes on to create the next worker :)
            continue

def main():
    p = argparse.ArgumentParser(description='Run analysis worker', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("-q", "--queue", help="SQS input queue")
    p.add_argument("-k", "--aws-key", help="AWS Key")
    p.add_argument("-s", "--aws-secret-key", help="AWS Secret Key")
    p.add_argument("-w", "--work-dir", help="Location to put temporary work files")
    cfg = p.parse_args()

    for index in xrange(0, cpu_count()):
        manager = Manager(cfg.aws_key, cfg.aws_secret_key, cfg.work_dir, index)
        manager.start()

if __name__ == "__main__":
    sys.exit(main())