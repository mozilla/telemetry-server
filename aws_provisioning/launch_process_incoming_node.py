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

    home = "/home/" + ssl_user
    print "Preparing code"
    with cd(home):
        run("git clone https://github.com/mreid-moz/telemetry-server.git")
        run("git clone https://github.com/sstoiana/s3funnel.git")
    with cd(home + "/s3funnel"):
        sudo("python setup.py install")
    with cd(home + "/telemetry-server"):
        # "data" is a dummy dir just to give it somewhere to look for local data.
        # FIXME: remove this once we merge into 'master'
        run("git checkout node_server")
        run("mkdir work processed")

def process_incoming(config, instance):
    ssl_user = config.get("ssl_user", "ubuntu")
    home = "/home/" + ssl_user
    with cd(home + "/telemetry-server"):
        run('python process_incoming.py -k "%s" -s "%s" -w work -o processed -t ./telemetry_schema.json %s %s' % (config["aws_key"], config["aws_secret_key"], config["incoming_bucket"], config["publish_bucket"]))

if len(sys.argv) < 2:
    print "Usage:", sys.argv[0], "/path/to/config_file.json"
    sys.exit(1)

config_file = open(sys.argv[1])
config = json.load(config_file)
config_file.close()

print "Using the following config:"
print json.dumps(config)
#sys.exit(-1)

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

    if aws_util.wait_for_ssh(config.get("ssl_retries", 3)):
        print "SSH Connection is ready."
    else:
        print "Failed to establish SSH Connection to", instance.id
        sys.exit(2)

    bootstrap_instance(config, instance)
    process_incoming(config, instance)
finally:
    # All done: Terminate this mofo
    print "Terminating", instance.id
    conn.terminate_instances(instance_ids=[instance.id])
