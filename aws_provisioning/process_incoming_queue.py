#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import time
from fabric.api import *
import sys
import aws_util
from process_incoming_distributed import ProcessIncomingLauncher
import traceback


class ProcessIncomingQueueLauncher(ProcessIncomingLauncher):
    def run(self, instance):
        home = "/home/" + self.ssl_user

        # Update from github
        with cd(home + "/telemetry-server"):
            run("git pull")

        q_conn = aws_util.connect_sqs(self.config["region"], self.aws_key, self.aws_secret_key)
        incoming_queue = q_conn.get_queue(self.config["incoming_queue"])

        if self.config.get("loop", False):
            while True:
                if incoming_queue.count() == 0:
                    print "No files to process yet. Sleeping for a while..."
                    # TODO: Terminate 'instance' and fire up a new one when we need it?
                    time.sleep(60)
                    continue
                self.process_incoming_queue(instance)
        else:
            self.process_incoming_queue(instance)

    def process_incoming_queue(self, instance):
        home = "/home/" + self.ssl_user
        with cd(home + "/telemetry-server"):
            skip_conversion = ""
            if self.config.get("skip_conversion", False):
                skip_conversion = "--skip-conversion"
            print "Processing incoming queue:", self.config["incoming_queue"]
            run('python process_incoming_mp.py --bad-data-log /mnt/telemetry/bad_records.txt -k "%s" -s "%s" -r "%s" -w /mnt/telemetry/work -o /mnt/telemetry/processed -t ./telemetry_schema.json -q "%s" %s %s %s' % (self.aws_key, self.aws_secret_key, self.config["region"], self.config["incoming_queue"], skip_conversion, self.config["incoming_bucket"], self.config["publish_bucket"]))

def main():
    try:
        launcher = ProcessIncomingQueueLauncher()
        launcher.go()
        return 0
    except Exception, e:
        print "Error:", e
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
