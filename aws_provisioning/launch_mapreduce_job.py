#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import boto.ec2
import time
import os
import simplejson as json
from fabric.api import *
from fabric.exceptions import NetworkError
import sys
import aws_util
from aws_launcher import SimpleLauncher

class MapReduceLauncher(SimpleLauncher):
    def post_install(self, instance):
        base_dir = self.config.get("base_dir", "/mnt/telemetry")
        with cd(base_dir):
            # "data" is a dummy dir just to give it somewhere to look for local data.
            run("mkdir job work data")

    def run(self, instance):
        home = "/home/" + self.ssl_user
        mr_cfg = self.config["mapreduce"]
        base_dir = self.config.get("base_dir", "/mnt/telemetry")
        job_dir = base_dir + "/job"
        data_dir = base_dir + "/data"
        work_dir = base_dir + "/work"
        with cd(home + "/telemetry-server"):
            job_script = mr_cfg["job_script"]
            input_filter = mr_cfg["input_filter"]
            put(job_script, job_dir)
            put(input_filter, job_dir)
            job_script_path = "/".join((job_dir, os.path.basename(job_script)))
            input_filter_path = "/".join((job_dir, os.path.basename(input_filter)))
            output_path = "/".join((job_dir, "output.txt"))
            job_args = (job_script_path, input_filter_path, data_dir, work_dir, output_path, self.aws_key, self.aws_secret_key, mr_cfg["data_bucket"])
            run('python job.py %s --input-filter %s --data-dir %s --work-dir %s --output %s --aws-key "%s" --aws-secret-key "%s" --bucket "%s"' % job_args)
            # TODO: consult "output_compression"
            run("lzma " + output_path)
            # TODO: upload job/output.txt.lzma to S3 output_bucket.output_filename
            result = get(output_path + ".lzma", mr_cfg["output_filename"])
            # TODO: check result.succeeded before bailing.

def main():
    try:
        launcher = MapReduceLauncher()
        launcher.go()
        return 0
    except:
        return 1

if __name__ == "__main__":
    sys.exit(main())
