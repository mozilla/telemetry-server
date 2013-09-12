#!/usr/bin/env python
# encoding: utf-8
"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""

import boto.ec2
import time
import os
import simplejson as json
from fabric.api import *
from fabric.exceptions import NetworkError
import sys
import aws_util
from boto.s3.connection import S3Connection
from aws_launcher import Launcher


class ProcessIncomingLauncher(Launcher):
    def bootstrap_instance(self, instance):
        # NOTE: This code assumes we're launching from the pre-bootstrapped AMI
        #       image that contains s3funnel, lzma, etc (ami-76831f46)
        #       If that's not the case, then the generic Launcher bootstrap
        #       should be run on this instance first.
        self.create_work_dir(instance)
        run("mkdir -p {0}/work {0}/processed".format(self.config.get("base_dir", "/mnt/telemetry")))

        self.install_telemetry_code(instance)
        home = "/home/" + self.ssl_user
        with cd(home + "/telemetry-server"):
            run("bash get_histogram_tools.sh")

    def run(self, instance):
        home = "/home/" + self.ssl_user

        # Update from github
        with cd(home + "/telemetry-server"):
            run("git pull")
        s3conn = S3Connection(self.aws_key, self.aws_secret_key)
        incoming_bucket = s3conn.get_bucket(self.config["incoming_bucket"])
        incoming_filenames = []
        for f in incoming_bucket.list():
            incoming_filenames.append(f.name)

        incoming_batch_size = self.config.get("incoming_batch_size", 8)
        # TODO: sort the incoming list by time (oldest first)
        with cd(home + "/telemetry-server"):
            while len(incoming_filenames) > 0:
                current_filenames = incoming_filenames[0:incoming_batch_size]
                incoming_filenames = incoming_filenames[incoming_batch_size:]
                run("echo '%s' > inputs.txt" % (current_filenames.pop(0)))
                for c in current_filenames:
                    run("echo '%s' >> inputs.txt" % (c))
                run("echo 'Processing files:'")
                run("cat inputs.txt")
                skip_conversion = ""
                if self.config.get("skip_conversion", False):
                    skip_conversion = "--skip-conversion"
                print "Processing", len(current_filenames), "inputs,", len(incoming_filenames), "remaining"
                run('python process_incoming_mp.py -i inputs.txt --bad-data-log /mnt/telemetry/bad_records.txt -k "%s" -s "%s" -w /mnt/telemetry/work -o /mnt/telemetry/processed -t ./telemetry_schema.json %s %s %s' % (self.aws_key, self.aws_secret_key, skip_conversion, self.config["incoming_bucket"], self.config["publish_bucket"]))

def main():
    try:
        launcher = ProcessIncomingLauncher()
        launcher.go()
        return 0
    except:
        return 1

if __name__ == "__main__":
    sys.exit(main())
