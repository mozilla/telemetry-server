#!/usr/bin/env python
# encoding: utf-8
"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""

import boto.ec2
from fabric.api import *
from fabric.exceptions import NetworkError

def connect_cfg(config):
    return connect(config["region"],
                   config["aws_key"],
                   config["aws_secret_key"])

def connect(region, aws_key, aws_secret_key):
    # Use AWS keys from config
    conn = boto.ec2.connect_to_region(region,
            aws_access_key_id=aws_key,
            aws_secret_access_key=aws_secret_key)
    return conn

def get_instance(conn, instance_id):
    reservations = conn.get_all_instances(instance_ids=[instance_id])
    instance = reservations[0].instances[0]
    return instance

# SSH is usually not available right away (the machine has to start up), so
# here we retry a few times until we can connect.
def wait_for_ssh(retries):
    for i in range(1, retries + 1):
        try:
            run("hostname")
            return True
        except NetworkError:
            print "SSH connection attempt", i, "of", retries, "failed. Trying again in 10s"
            time.sleep(10)
    return False

# Sometimes `apt-get update` fails, so retry the update/install until it works.
# Pass in a space-separated string
def install_packages(packages):
    with settings(warn_only=True):
        for i in range(1,20):
            sudo("apt-get update")
            result = sudo(" ".join('apt-get --yes install', packages))
            if result.succeeded:
                break
            print "apt-get attempt", i, "failed, retrying in 2s"
            time.sleep(2)

