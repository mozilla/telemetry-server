#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from aws_launcher import Launcher
import aws_util
import simplejson as json
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

    def heka_pkg_version(self):
        return "0.5.0"

    def heka_pkg_url(self):
        return "https://github.com/mozilla-services/heka/releases/" \
            "download/v{0}/heka_{0}_amd64.deb".format(self.heka_pkg_version())

    def install_heka(self):
        heka_pkg = self.heka_pkg_name()
        run("wget {0} -O heka.deb".format(self.heka_pkg_url()))
        sudo("dpkg -i heka.deb"

    def create_logrotate_config(self, lr_file, target_log, create=True):
        sudo("echo '%s {' > %s" % (target_log, lr_file))
        sudo("echo '    su {1} {1}' >> {0}".format(lr_file, self.ssl_user))
        sudo("echo '    rotate 5' >> {0}".format(lr_file))
        sudo("echo '    daily' >> {0}".format(lr_file))
        sudo("echo '    compress' >> {0}".format(lr_file))
        sudo("echo '    missingok' >> {0}".format(lr_file))
        if create:
            sudo("echo '    create 640 {1} {1}' >> {0}".format(lr_file, self.ssl_user))
        else:
            sudo("echo '    copytruncate' >> {0}".format(lr_file))
        sudo("echo '}' >> " + lr_file)
        with settings(warn_only=True):
            # This will warn if there's no file there.
            sudo("logrotate -f {0}".format(lr_file))

    def post_install(self, instance):
        # Install some more:
        self.install_nodejs_bin()
        self.install_heka()
        self.install_histogram_tools(instance)

        # Create log dir (within base_dir, but symlinked to /var/log):
        self.create_log_dir()

        # Create data dir:
        base_dir = self.config.get("base_dir", "/mnt/telemetry")
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

        # Setup logrotate for the telemetry log files
        self.create_logrotate_config("/etc/logrotate.d/telemetry-server",
                "/var/log/telemetry/telemetry-server.log")
        self.create_logrotate_config("/etc/logrotate.d/telemetry-incoming",
                "/var/log/telemetry/telemetry-incoming.log")
        self.create_logrotate_config("/etc/logrotate.d/telemetry-incoming-stats",
                "/var/log/telemetry/telemetry-incoming-stats.log")

        # Create startup scripts:
        code_base = self.home + "/telemetry-server"
        c_file = "/etc/init/telemetry-server.conf"
        self.start_suid_script(c_file, self.ssl_user)
        sudo("echo '    cd {1}/http' >> {0}".format(c_file, code_base))
        sudo("echo '    /usr/local/bin/node ./server.js ./server_config.json >> /var/log/telemetry/telemetry-server.out' >> {0}".format(c_file))
        self.end_suid_script(c_file)
        #sudo("echo 'start on runlevel [2345]' >> {0}".format(c_file))
        # Automatically stop on shutdown.
        sudo("echo 'stop on runlevel [016]' >> {0}".format(c_file))

        c_file = "/etc/init/telemetry-export.conf"
        base_export_command = "/usr/bin/python -u -m telemetry.util.export " \
            "-d {0}/data " \
            "-p '^telemetry.log.*[.]finished$' " \
            "--config /etc/mozilla/telemetry_aws.json".format(base_dir)

        self.start_suid_script(c_file, self.ssl_user)
        sudo("echo '    cd {1}' >> {0}".format(c_file, code_base))
        sudo("echo \"    {1} --loop >> /var/log/telemetry/telemetry-export.out\" >> {0}".format(c_file, base_export_command))
        self.end_suid_script(c_file)
        # after we receive "stop", run once more in non-looping mode to make
        # sure we exported everything.
        sudo("echo 'post-stop script' >> {0}".format(c_file))
        sudo("echo '    cd {1}' >> {0}".format(c_file, code_base))
        sudo("echo \"    {1} >> /var/log/telemetry/telemetry-export.out\" >> {0}".format(c_file, base_export_command))
        sudo("echo 'end script' >> {0}".format(c_file))
        # Start/stop this in lock step with telemetry-server
        sudo("echo 'start on started telemetry-server' >> {0}".format(c_file))
        sudo("echo 'stop on stopped telemetry-server' >> {0}".format(c_file))

        c_file = "/etc/init/telemetry-incoming.conf"
        self.start_suid_script(c_file, self.ssl_user)
        sudo("echo '    cd {1}' >> {0}".format(c_file, code_base))
        # Use unbuffered output (-u) so we can see things in the log
        # immediately.
        sudo("echo \"    /usr/bin/python -u " \
             "-m process_incoming.process_incoming_standalone " \
             "-c /etc/mozilla/telemetry_aws.json " \
             "-w {1}/work " \
             "-o {1}/processed " \
             "-t telemetry/telemetry_schema.json " \
             "-l /var/log/telemetry/telemetry-incoming.log " \
             "-s /var/log/telemetry/telemetry-incoming-stats.log >> " \
             "/var/log/telemetry/telemetry-incoming.out 2>&1\" >> {0}".format(
                c_file, base_dir))
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

        c_file = "/etc/init/telemetry-heka.conf"
        self.start_suid_script(c_file, self.ssl_user)
        sudo("echo '    cd {1}/monitoring/heka' >> {0}".format(c_file, code_base))
        sudo("echo \"    /usr/bin/hekad -config heka.toml >> /var/log/telemetry/telemetry-heka.out\" >> {0}".format(c_file))
        self.end_suid_script(c_file)
        sudo("echo 'kill signal INT' >> {0}".format(c_file))
        # Start/stop this in lock step with telemetry-server
        sudo("echo 'start on started telemetry-server' >> {0}".format(c_file))
        sudo("echo 'stop on stopped telemetry-server' >> {0}".format(c_file))

        # Service configuration for telemetry-analysis
        c_file = "/etc/init/telemetry-analysis.conf"
        self.start_suid_script(c_file, self.ssl_user)
        self.append_suid_script(c_file, "cd {0}".format(code_base))
        self.append_suid_script(c_file, "python -m analysis.manager -q `cat /etc/telemetry-analysis-input-queue` -w /mnt/work/")
        self.end_suid_script(c_file)
        sudo("echo 'stop on runlevel [016]' >> {0}".format(c_file))

        # Configure boto
        aws_util.install_file("provisioning/config/boto.cfg", "/etc/boto.cfg")

        # Install the default config file:
        sudo("mkdir -p /etc/mozilla")
        prod_aws_config_file = "provisioning/config/telemetry_aws.prod.json"
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
        sudo("start telemetry-server")
        print "Telemetry server started"
        # Note: This also starts up telemetry-export and telemetry-heka due to dependencies.

        # Start up 'process incoming' only on the primary node
        # TODO: pass in user-data to set this.
        if self.config.get("primary_server", False):
            sudo("start telemetry-incoming")
            print "Telemetry incoming started"
        else:
            print "Not starting telemetry-incoming since this is not a primary server"

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
