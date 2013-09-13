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


if len(sys.argv) < 2:
    print "Usage:", sys.argv[0], "/path/to/config_file.json"
    sys.exit(1)

config_file = open(sys.argv[1], "r")
config = json.load(config_file)
config_file.close()
print "Using the following config:"
print json.dumps(config)

conn = aws_util.connect_cfg(config)
instance = aws_util.get_instance(conn, config["instance_id"])
print "Instance", instance.id, "is", instance.state

# Set up Fabric:
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

print "Installing dummy nginx site on port 8080"
aws_util.install_file("./dummy_nginx_endpoint", "/etc/nginx/sites-available/dummy")
sudo("ln -s /etc/nginx/sites-available/dummy /etc/nginx/sites-enabled/dummy")
sudo("mkdir -p /var/www/dummy")
aws_util.install_file("./dummy_index.html", "/var/www/dummy/index.html")
sudo("service nginx restart")

print "All finished with instance", instance.id
print "To connect to it: ssh -i", ssl_key_path, ssl_host
