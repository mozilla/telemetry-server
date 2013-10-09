#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from aws_launcher import Launcher
from fabric.api import *
import fabric.network
import sys
import traceback

class TelemetryServerLauncher(Launcher):
    def nodejs_version(self):
        return "0.10.19"

    def install_nodejs_bin(self):
        node_version = self.nodejs_version()
        run("wget http://nodejs.org/dist/v{0}/node-v{0}-linux-x64.tar.gz".format(node_version))
        run("tar xzf node-v{0}-linux-x64.tar.gz".format(node_version))
        sudo("mv node-v{0}-linux-x64 /usr/local/".format(node_version))
        sudo("ln -s /usr/local/node-v{0}-linux-x64 /usr/local/node".format(node_version))
        sudo("ln -s /usr/local/node/bin/node /usr/local/bin/node")
        sudo("ln -s /usr/local/node/lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm")
        run("node --version")
        run("npm --version")

    def install_nodejs_src(self):
        node_version = self.nodejs_version()
        run("wget http://nodejs.org/dist/v{0}/node-v{0}.tar.gz".format(node_version))
        run("tar xzf node-v{0}.tar.gz".format(node_version))
        with cd("node-v{0}".format(node_version)):
            run("./configure")
            run("make")
            sudo("make install")

    def heka_pkg_name(self):
        return "heka-0_4_0-linux-amd64.deb"

    def install_heka(self):
        heka_pkg = self.heka_pkg_name()
        run("wget http://people.mozilla.org/~mreid/{0}".format(heka_pkg))
        sudo("dpkg -i {0}".format(heka_pkg))

    def start_suid_script(self, c_file, username):
        sudo("echo 'setuid {1}' > {0}".format(c_file, username))
        sudo("echo 'setgid {1}' >> {0}".format(c_file, username))
        # Set the ulimit for # open files in the upstart scripts (since the
        # ones set in limits.conf don't seem to apply here)
        sudo("echo 'limit nofile 10000 40000' >> " + c_file)
        sudo("echo 'script' >> " + c_file)

    def end_suid_script(self, c_file):
        sudo("echo 'end script' >> {0}".format(c_file))
        sudo("echo 'respawn' >> {0}".format(c_file))

    def post_install(self, instance):
        # Install some more:
        self.install_nodejs_bin()
        self.install_heka()

        # Create log dir (within base_dir, but symlinked to /var/log):
        base_dir = self.config.get("base_dir", "/mnt/telemetry")
        log_dir = base_dir + "/log"
        run("mkdir {0}".format(log_dir))
        sudo("ln -s {0} /var/log/telemetry".format(log_dir))

        # Create data dir:
        run("mkdir {0}/data".format(base_dir))

        # Install security certificate for running 'process incoming':
        run("mkdir -p ~/.ssh/aws")
        put("~/.ssh/aws/mreid.pem", "~/.ssh/aws/mreid.pem")

        # Increase limits on open files.
        sudo("echo '*                soft    nofile          10000' > /etc/security/limits.conf")
        sudo("echo '*                hard    nofile          30000' >> /etc/security/limits.conf")

        # Each fabric 'run' starts a separate shell, so the limits above should
        # be set correctly. However, we actually need to disconnect from SSH to
        # get a fresh connection first.
        fabric.network.disconnect_all()

        run("echo 'Soft limit:'; ulimit -S -n")
        run("echo 'Hard limit:'; ulimit -H -n")

        # Setup logrotate for the stats log
        lr_file = "/etc/logrotate.d/telemetry"
        sudo("echo '/var/log/telemetry/telemetry-server.log {' > " + lr_file)
        sudo("echo '    su {1} {1}' >> {0}".format(lr_file, self.ssl_user))
        sudo("echo '    rotate 10' >> {0}".format(lr_file))
        sudo("echo '    daily' >> {0}".format(lr_file))
        sudo("echo '    compress' >> {0}".format(lr_file))
        sudo("echo '    missingok' >> {0}".format(lr_file))
        sudo("echo '    create 640 {1} {1}' >> {0}".format(lr_file, self.ssl_user))
        sudo("echo '}' >> " + lr_file)
        with settings(warn_only=True):
            # This will warn if there's no file there.
            sudo("logrotate -f /etc/logrotate.d/telemetry")

        # Create startup scripts:
        code_base = "/home/" + self.ssl_user + "/telemetry-server"
        c_file = "/etc/init/telemetry-server.conf"
        self.start_suid_script(c_file, self.ssl_user)
        sudo("echo '    cd {1}/server' >> {0}".format(c_file, code_base))
        sudo("echo '    /usr/local/bin/node ./server.js ./server_config.json >> /var/log/telemetry/telemetry-server.out' >> {0}".format(c_file))
        self.end_suid_script(c_file)

        c_file = "/etc/init/telemetry-export.conf"
        self.start_suid_script(c_file, self.ssl_user)
        sudo("echo '    cd {1}' >> {0}".format(c_file, code_base))
        sudo("echo \"    /usr/bin/python -u ./export.py -d {1}/data -p '^telemetry.log.*[.]finished$' -k '{2}' -s '{3}' -r '{4}' -b '{5}' -q '{6}' --remove-files --loop >> /var/log/telemetry/telemetry-export.out\" >> {0}".format(c_file, base_dir, self.aws_key, self.aws_secret_key, self.config["region"], self.config.get("incoming_bucket", "telemetry-incoming"), self.config.get("incoming_queue", "telemetry-incoming")))
        self.end_suid_script(c_file)

        # Install a specific aws_incoming.json to use
        process_incoming_config = self.config.get("process_incoming_config", "aws_incoming.json")
        put(process_incoming_config, code_base + "/aws_provisioning/aws_incoming.json")

        c_file = "/etc/init/telemetry-incoming.conf"
        self.start_suid_script(c_file, self.ssl_user)
        sudo("echo '    cd {1}/aws_provisioning' >> {0}".format(c_file, code_base))
        # Use unbuffered output (-u) so we can see things in the log
        # immediately.
        sudo("echo \"    /usr/bin/python -u process_incoming_queue.py -k '{1}' -s '{2}' ./aws_incoming.json >> /var/log/telemetry/telemetry-incoming.out\" >> {0}".format(c_file, self.aws_key, self.aws_secret_key))
        self.end_suid_script(c_file)

        c_file = "/etc/init/telemetry-heka.conf"
        self.start_suid_script(c_file, self.ssl_user)
        sudo("echo '    cd {1}/heka' >> {0}".format(c_file, code_base))
        sudo("echo \"    /usr/bin/hekad -config heka.toml >> /var/log/telemetry/telemetry-heka.out\" >> {0}".format(c_file))
        self.end_suid_script(c_file)
        sudo("echo 'kill signal INT' >> {0}".format(c_file))

    def run(self, instance):
        # Start up HTTP server
        sudo("start telemetry-server")
        print "Telemetry server started"

        # Start up exporter
        sudo("start telemetry-export")
        print "Telemetry export started"
        
        # Start up 'process incoming' only on the primary node
        if self.config.get("primary_server", False):
            sudo("start telemetry-incoming")
            print "Telemetry incoming started"
        else:
            print "Not starting telemetry-incoming since this is not a primary server"

        # Start up heka
        sudo("start telemetry-heka")
        print "Heka daemon started"

def main():
    try:
        launcher = TelemetryServerLauncher()
        launcher.go()
        return 0
    except Exception, e:
        print "Error:", e
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
