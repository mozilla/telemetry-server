#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from aws_launcher import Launcher
from launch_telemetry_server import TelemetryServerLauncher
import aws_util
import simplejson as json
from fabric.api import *
import fabric.network
import os
import sys
import traceback
from telemetry.convert import Converter

class HotfixServerLauncher(TelemetryServerLauncher):
    def choose_telemetry_branch(self, instance):
        with cd(self.home + "/telemetry-server/telemetry"):
            run("git checkout addon_hotfix_bug1019788")

    def post_install(self, instance):
        # Install some more:
        self.install_nodejs_bin()
        self.install_histogram_tools(instance)

        # Create log dir (within base_dir, but symlinked to /var/log):
        self.create_log_dir()

        # Create data dir:
        base_dir = self.config.get("base_dir", "/mnt/hotfix")
        with cd(base_dir):
            run("mkdir data work processed")

        # Increase limits on open files.
        sudo("echo '*                soft    nofile          10000' > /etc/security/limits.conf")
        sudo("echo '*                hard    nofile          40000' >> /etc/security/limits.conf")

        # Each fabric 'run' starts a separate shell, so the limits above should
        # be set correctly. However, we actually need to disconnect from SSH to
        # get a fresh connection first.
        fabric.network.disconnect_all()

        run("echo 'Soft limit:'; ulimit -S -n")
        run("echo 'Hard limit:'; ulimit -H -n")

        log_dir_name = self.config.get("log_dir_name", "hotfix")
        log_dir_prefix = "/var/log/" + log_dir_name
        # Setup logrotate for the server log files
        self.create_logrotate_config("/etc/logrotate.d/hotfix-server",
                log_dir_prefix + "/hotfix-server.log")
        self.create_logrotate_config("/etc/logrotate.d/hotfix-incoming",
                log_dir_prefix + "/hotfix-incoming.log")
        self.create_logrotate_config("/etc/logrotate.d/hotfix-incoming-stats",
                log_dir_prefix + "/hotfix-incoming-stats.log")

        code_base = self.home + "/telemetry-server"
        # Create startup scripts:
        c_file = "/etc/init/hotfix-server.conf"
        self.start_suid_script(c_file, self.ssl_user)
        sudo("echo '    cd {1}/http' >> {0}".format(c_file, code_base))
        sudo("echo '    /usr/local/bin/node ./server.js ./server_config.addon_hotfix.json >> {1}/hotfix-server.out' >> {0}".format(c_file, log_dir_prefix))
        self.end_suid_script(c_file)
        #sudo("echo 'start on runlevel [2345]' >> {0}".format(c_file))
        # Automatically stop on shutdown.
        sudo("echo 'stop on runlevel [016]' >> {0}".format(c_file))

        c_file = "/etc/init/hotfix-export.conf"
        base_export_command = "/usr/bin/python -u -m telemetry.util.export " \
            "-d {0}/data " \
            "-p '^hotfix.log.*[.]finished$' " \
            "--config /etc/mozilla/telemetry_aws.json".format(base_dir)

        self.start_suid_script(c_file, self.ssl_user)
        sudo("echo '    cd {1}' >> {0}".format(c_file, code_base))
        sudo("echo \"    {1} --loop >> {2}/hotfix-export.out\" >> {0}".format(c_file, base_export_command, log_dir_prefix))
        self.end_suid_script(c_file)
        # after we receive "stop", run once more in non-looping mode to make
        # sure we exported everything.
        sudo("echo 'post-stop script' >> {0}".format(c_file))
        sudo("echo '    cd {1}' >> {0}".format(c_file, code_base))
        sudo("echo \"    {1} >> {2}/hotfix-export.out\" >> {0}".format(c_file, base_export_command, log_dir_prefix))
        sudo("echo 'end script' >> {0}".format(c_file))
        # Start/stop this in lock step with telemetry-server
        sudo("echo 'start on started hotfix-server' >> {0}".format(c_file))
        sudo("echo 'stop on stopped hotfix-server' >> {0}".format(c_file))

        c_file = "/etc/init/hotfix-incoming.conf"
        self.start_suid_script(c_file, self.ssl_user)
        sudo("echo '    cd {1}' >> {0}".format(c_file, code_base))
        # Use unbuffered output (-u) so we can see things in the log
        # immediately.
        sudo("echo \"    /usr/bin/python -u " \
             "-m process_incoming.process_incoming_hotfix " \
             "-c /etc/mozilla/telemetry_aws.json " \
             "-w {1}/work " \
             "-o {1}/processed " \
             "-t telemetry/hotfix_schema.json " \
             "-l {2}/hotfix-incoming.log " \
             "-s {2}/hotfix-incoming-stats.log >> " \
             "{2}/hotfix-incoming.out 2>&1\" >> {0}".format(
                c_file, base_dir, log_dir_prefix))
        # NOTE: Don't automatically start/stop this service, since we only want
        #       to start it on "primary" nodes, and we only want to stop it in
        #       safe parts of the process-incoming code.
        self.end_suid_script(c_file)
        # We trap SIGINT and shutdown cleanly (if we're in the middle of
        # publishing, we continue until it's done).
        sudo("echo 'kill signal INT' >> {0}".format(c_file))
        # Wait up to 10 minutes for the current exports to finish.
        sudo("echo 'kill timeout 600' >> {0}".format(c_file))
        # Automatically stop on shutdown.
        sudo("echo 'stop on runlevel [016]' >> {0}".format(c_file))

        # Configure boto
        aws_util.install_file("provisioning/config/boto.cfg", "/etc/boto.cfg")

        # Install the default config file:
        sudo("mkdir -p /etc/mozilla")
        prod_aws_config_file = "provisioning/config/telemetry_aws.hotfix.json"
        if self.config.get("add_aws_credentials", False):
            # add aws credentials
            fin = open(prod_aws_config_file)
            prod_aws_config = json.load(fin)
            fin.close()
            prod_aws_config["aws_key"] = self.aws_key
            prod_aws_config["aws_secret_key"] = self.aws_secret_key
            sudo("echo '{0}' >> /etc/mozilla/telemetry_aws.json".format(json.dumps(prod_aws_config)))
        else:
            aws_util.install_file(prod_aws_config_file, "/etc/mozilla/telemetry_aws.json")

    def run(self, instance):
        # Start up HTTP server
        sudo("start hotfix-server")
        print "Hotfix server started"
        # Note: This also starts up the data export due to dependencies.

        # Start up 'process incoming' too (do everything on one node)
        sudo("start hotfix-incoming")
        print "Hotfix incoming started"

def main():
    try:
        launcher = HotfixServerLauncher()
        launcher.go()
        return 0
    except Exception, e:
        print "Error:", e
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
