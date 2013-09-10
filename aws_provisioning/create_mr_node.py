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

def bootstrap_instance(config, instance):
    ssl_user = config.get("ssl_user", "ubuntu")
    ssl_key_path = config.get("ssl_key_path", "~/.ssh/id_rsa.pub")
    ssl_host = "@".join((ssl_user, instance.public_dns_name))
    print "To connect to it:"
    print "ssh -i", ssl_key_path, ssl_host

    # Now configure the instance:
    print "Installing dependencies"
    aws_util.install_packages("git python-pip build-essential python-dev xz-utils")

    #sudo("apt-get --yes dist-upgrade")
    sudo('pip install simplejson scales boto')

    base_dir = config.get("base_dir", "/mnt/telemetry")
    # Put work dirs in /mnt where there's plenty of space:
    sudo("mkdir -p " + base_dir)
    sudo("chown %s:%s %s" % (ssl_user, ssl_user, base_dir))

    home = "/home/" + ssl_user
    print "Preparing MR code"
    with cd(home):
        run("git clone https://github.com/mreid-moz/telemetry-server.git")
        run("git clone https://github.com/sstoiana/s3funnel.git")
    with cd(home + "/s3funnel"):
        sudo("python setup.py install")
    with cd(base_dir):
        # "data" is a dummy dir just to give it somewhere to look for local data.
        run("mkdir job work data")

def run_mapreduce(config, instance):
    ssl_user = config.get("ssl_user", "ubuntu")
    home = "/home/" + ssl_user
    mr_cfg = config["mapreduce"]
    base_dir = config.get("base_dir", "/mnt/telemetry")
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
        job_args = (job_script_path, input_filter_path, data_dir, work_dir, output_path, config["aws_key"], config["aws_secret_key"], mr_cfg["data_bucket"])
        run('python job.py %s --input-filter %s --data-dir %s --work-dir %s --output %s --aws-key "%s" --aws-secret-key "%s" --bucket "%s"' % job_args)
        # TODO: consult "output_compression"
        run("lzma " + output_path)
        # TODO: upload job/output.txt.lzma to S3 output_bucket.output_filename
        result = get(output_path + ".lzma", mr_cfg["output_filename"])
        # TODO: check result.succeeded before bailing.

if len(sys.argv) < 2:
    print "Usage:", sys.argv[0], "/path/to/config_file.json"
    sys.exit(1)

config_file = open(sys.argv[1])
config = json.load(config_file)
config_file.close()

print "Using the following config:"
print json.dumps(config)

if "instance_id" in config:
    conn = aws_util.connect_cfg(config)
    instance = aws_util.get_instance(conn, config["instance_id"])
else:
    conn, instance = aws_util.create_instance(config)

try:
    ssl_user = config.get("ssl_user", "ubuntu")
    ssl_key_path = config.get("ssl_key_path", "~/.ssh/id_rsa.pub")
    ssl_host = "@".join((ssl_user, instance.public_dns_name))
    print "To connect to it:"
    print "ssh -i", ssl_key_path, ssl_host

    # Use ssh config to specify the correct key and username
    env.key_filename = config["ssl_key_path"]
    env.host_string = ssl_host

    # Can't connect when using known hosts :(
    env.disable_known_hosts = True
    env.keepalive = 5

    if aws_util.wait_for_ssh(config.get("ssl_retries", 3)):
        print "SSH Connection is ready."
    else:
        print "Failed to establish SSH Connection to", instance.id
        sys.exit(2)

    if not config.get("skip_bootstrap", False):
        bootstrap_instance(config, instance)

    run_mapreduce(config, instance)
finally:
    # All done: Terminate this mofo
    if "instance_id" in config:
        # It was an already-existing instance, leave it alone
        print "Not terminating instance", instance.id
    else:
        # we created it ourselves, terminate it.
        print "Terminating", instance.id
        conn.terminate_instances(instance_ids=[instance.id])
