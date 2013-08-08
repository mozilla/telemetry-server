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
import sys

def connect_aws(config):
    # Use AWS keys from config
    conn = boto.ec2.connect_to_region(config["region"],
            aws_access_key_id=config["mapreduce"]["aws_key"],
            aws_secret_access_key=config["mapreduce"]["aws_secret_key"])
    return conn

def create_instance(config):
    conn = connect_aws(config)
    # Known images:
    # ami-bf1d8a8f == Ubuntu 13.04
    reservation = conn.run_instances(
            config.get("image", "ami-bf1d8a8f"),
            key_name=config["ssl_key_name"],
            instance_type=config.get("instance_type", "m1.large"),
            security_groups=config["security_groups"],
            placement=config["placement"],
            instance_initiated_shutdown_behavior="terminate") # TODO: stop?

    instance = reservation.instances[0]

    default_tags = config.get("default_tags", {})
    if len(default_tags) > 0:
        conn.create_tags([instance.id], default_tags)
    # TODO:
    # - find all instances where Owner = mreid and Application = telemetry-server
    # - get the highest number
    # - use the next one (or first unused one) for the current instance name.
    name_tag = {"Name": config["name"]}
    conn.create_tags([instance.id], name_tag)

    while instance.state == 'pending':
        print "Instance is pending - Waiting 10s for instance to start up..."
        time.sleep(10)
        instance.update()

    print "Instance", instance.id, "is", instance.state
    return conn, instance

def get_running_instance(config):
    conn = connect_aws(config)
    reservations = conn.get_all_instances(instance_ids=[config["instance_id"]])
    instance = reservations[0].instances[0]
    print "Instance", instance.id, "is", instance.state
    return conn, instance

def bootstrap_instance(config, instance):
    ssl_user = config.get("ssl_user", "ubuntu")
    ssl_key_path = config.get("ssl_key_path", "~/.ssh/id_rsa.pub")
    ssl_host = "@".join((ssl_user, instance.public_dns_name))
    print "To connect to it:"
    print "ssh -i", ssl_key_path, ssl_host

    # TODO: add server's key fingerprint to known_hosts

    # Use ssh config to specify the correct key and username
    #env.key_filename = config["ssl_key_path"]
    #env.host_string = ssl_host

    # can't connect when using known hosts :(
    #env.disable_known_hosts = True

    #run("whoami")
    #run("hostname")

    # Now configure the instance:
    print "Installing dependencies"
    sudo("apt-get update")
    #sudo("apt-get --yes dist-upgrade")
    sudo('apt-get --yes install git python-pip build-essential python-dev lzma')
    sudo('pip install simplejson scales boto')

    mr_cfg = config["mapreduce"]
    home = "/home/" + ssl_user
    print "Preparing MR code"
    with cd(home):
        run("git clone https://github.com/mreid-moz/telemetry-server.git")
        run("git clone https://github.com/sstoiana/s3funnel.git")
    with cd(home + "/s3funnel"):
        sudo("python setup.py install")
    with cd(home + "/telemetry-server"):
        # "data" is a dummy dir just to give it somewhere to look for local data.
        run("mkdir job work data")

def run_mapreduce(config, instance):
    ssl_user = config.get("ssl_user", "ubuntu")
    home = "/home/" + ssl_user
    mr_cfg = config["mapreduce"]
    with cd(home + "/telemetry-server"):
        job_script = mr_cfg["job_script"]
        input_filter = mr_cfg["input_filter"]
        put(job_script, "job")
        put(input_filter, "job")
        job_script_base = os.path.basename(job_script)
        input_filter_base = os.path.basename(input_filter)
        job_args = (job_script_base, input_filter_base, mr_cfg["aws_key"], mr_cfg["aws_secret_key"], mr_cfg["data_bucket"])
        run('python job.py job/%s --input-filter job/%s --data-dir ./data --work-dir ./work --aws-key "%s" --aws-secret-key "%s" --bucket "%s" --output job/output.txt' % job_args)
        # TODO: consult "output_compression"
        run("lzma job/output.txt")
        # TODO: upload job/output.txt.lzma to S3 output_bucket.output_filename
        result = get("job/output.txt.lzma", mr_cfg["output_filename"])
        # TODO: check result.succeeded before bailing.

if len(sys.argv) < 2:
    print "Usage:", sys.argv[0], "/path/to/config_file.json"
    sys.exit(1)

config_file = open(sys.argv[1])
config = json.load(config_file)
config_file.close()

print "Using the following config:"
print json.dumps(config)
#sys.exit(-1)

conn, instance = create_instance(config)
#conn, instance = get_running_instance(config)
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

    retries = config.get("ssl_retries", 3)
    for i in range(1, retries + 1):
        try:
            run("hostname")
            break
        except NetworkError:
            print "SSH connection attempt", i, "of", retries, "failed. Trying again in 10s"
            time.sleep(10)

    bootstrap_instance(config, instance)
    run_mapreduce(config, instance)
finally:
    # All done: Terminate this mofo
    print "Terminating", instance.id
    conn.terminate_instances(instance_ids=[instance.id])
