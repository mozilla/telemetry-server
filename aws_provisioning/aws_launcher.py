#!/usr/bin/env python
# encoding: utf-8
"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""

import argparse
import boto.ec2
import time
import os
import simplejson as json
from fabric.api import *
from fabric.exceptions import NetworkError
import sys
import aws_util

class Launcher(object):
    def __init__(self):
        parser = self.get_arg_parser()
        args = parser.parse_args()
        self.config = json.load(args.config)
        self.aws_key = args.aws_key
        self.aws_secret_key = args.aws_secret_key
        self.ssl_user = self.config.get("ssl_user", "ubuntu")
        self.ssl_key_path = self.config.get("ssl_key_path", "~/.ssh/id_rsa.pub")

    def get_arg_parser(self):
        parser = argparse.ArgumentParser(description='Launch AWS EC2 instances')
        parser.add_argument("config", help="JSON File contianing configuration", type=file)
        parser.add_argument("-k", "--aws-key", help="AWS Key", required=True)
        parser.add_argument("-s", "--aws-secret-key", help="AWS Secret Key", required=True)
        return parser

    def install_apt_dependencies(self, instance):
        print "Installing apt dependencies"
        aws_util.install_packages("git python-pip build-essential python-dev xz-utils")

    def install_python_dependencies(self, instance):
        print "Installing python dependencies"
        sudo('pip install simplejson boto')

    def install_misc_dependencies(self, instance):
        print "Installing other dependencies"
        # By default, install S3Funnel
        home = "/home/" + self.ssl_user
        with cd(home):
            run("git clone https://github.com/sstoiana/s3funnel.git")
        with cd(home + "/s3funnel"):
            sudo("python setup.py install")

    def install_telemetry_code(self, instance):
        home = "/home/" + self.ssl_user
        with cd(home):
            run("git clone https://github.com/mreid-moz/telemetry-server.git")

    def choose_telemetry_branch(self, instance):
        # By default we use the master branch, but if you wanted to use a
        # specific branch you could do it here.
        pass

    def create_work_dir(self, instance):
        base_dir = self.config.get("base_dir", "/mnt/telemetry")
        # By default, put work dir in /mnt where there's plenty of space:
        sudo("mkdir -p " + base_dir)
        sudo("chown %s:%s %s" % (self.ssl_user, self.ssl_user, base_dir))

    def bootstrap_instance(self, instance):
        # Now configure the instance:
        self.install_apt_dependencies(instance)
        self.install_python_dependencies(instance)
        self.install_misc_dependencies(instance)
        self.install_telemetry_code(instance)
        self.choose_telemetry_branch(instance)
        self.create_work_dir(instance)

    def terminate(self, conn, instance):
        print "Terminating", instance.id
        conn.terminate_instances(instance_ids=[instance.id])

    def go(self):
        print "Using the following config:"
        print json.dumps(self.config)

        if "instance_id" in self.config:
            print "Fetching instance", self.config["instance_id"]
            conn = aws_util.connect(self.config["region"], self.aws_key, self.aws_secret_key)
            instance = aws_util.get_instance(conn, self.config["instance_id"])
        else:
            print "Creating instance..."
            conn, instance = aws_util.create_instance(self.config, self.aws_key, self.aws_secret_key)

        print "Ready to connect..."
        try:
            ssl_host = "@".join((self.ssl_user, instance.public_dns_name))
            print "To connect to it:"
            print "ssh -i", self.ssl_key_path, ssl_host
            env.key_filename = self.ssl_key_path
            env.host_string = ssl_host
            
            # Can't connect when using known hosts :(
            env.disable_known_hosts = True

            # Long-running commands may time out if we don't set this
            env.keepalive = 5

            if aws_util.wait_for_ssh(self.config.get("ssl_retries", 3)):
                print "SSH Connection is ready."
            else:
                print "Failed to establish SSH Connection to", instance.id
                sys.exit(2)

            if not self.config.get("skip_bootstrap", False):
                self.bootstrap_instance(instance)

            self.run(instance)
        except Exception, e:
            print "Launch Error:", e
        finally:
            # All done: Terminate this mofo
            if "instance_id" in self.config:
                # It was an already-existing instance, leave it alone
                print "Not terminating instance", instance.id
            elif self.config.get("skip_termination", False):
                print "Config said not to terminate instance", instance.id
            else:
                # we created it ourselves, terminate it.
                self.terminate(conn, instance)

    def run(self, instance):
        pass


def main():
    try:
        launcher = Launcher()
        launcher.go()
        return 0
    except:
        return 1

if __name__ == "__main__":
    sys.exit(main())
