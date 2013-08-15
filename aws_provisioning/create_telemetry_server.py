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

def create_instance(config):
    conn = aws_util.connect_cfg(config)
    # Known images:
    # ami-bf1d8a8f == Ubuntu 13.04
    reservation = conn.run_instances(
            config.get("image", "ami-bf1d8a8f"),
            key_name=config["ssl_key_name"],
            instance_type=config.get("instance_type", "m1.large"),
            security_groups=config["security_groups"],
            placement=config["placement"],
            instance_initiated_shutdown_behavior="stop")

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

def bootstrap_instance(config, instance):
    # Do some configuration:
    ssl_user = config.get("ssl_user", "ubuntu")
    ssl_key_path = config.get("ssl_key_path", "~/.ssh/id_rsa.pub")
    ssl_host = "@".join((ssl_user, instance.public_dns_name))
    aws_util.install_packages("git python-pip build-essential python-dev nginx xz-utils")
    sudo("apt-get --yes dist-upgrade")

    sudo('pip install flask simplejson uwsgi virtualenv scales')
    sudo('useradd -m -s /bin/bash telemetry')
    sudo("useradd -c 'uwsgi user,,,' -g www-data -d /nonexistent -s /bin/false uwsgi")
    sudo("usermod -a -G www-data telemetry")
    sudo("usermod -a -G www-data uwsgi")

    # TODO: attach a newly-created or existing EBS volume
    #sudo("mkfs -t ext4 /dev/xvdf")
    #sudo("mkdir /data")
    #sudo("echo '/dev/xvdf       /data    ext4   noatime,nodiratime      0 0' >> /etc/fstab")
    #sudo("mount /data")

    # Parse server_config to find the correct storage path.
    with open(config["files"]["server_config"], "r") as server_config_file:
        server_config = json.load(server_config_file)

    telemetry_data = server_config["storage_path"]
    telemetry_home = "/home/telemetry/telemetry-server"
    telemetry_server = config.get("server_path", "/var/www/telemetry-server")

    sudo("mkdir -p " + telemetry_data)
    sudo("chown telemetry:telemetry " + telemetry_data)

    ## grant www-data access to read/write telemetry data:
    sudo("chgrp -R www-data " + telemetry_data)
    sudo("chmod -R g+w " + telemetry_data)

    ## create config files:
    aws_util.install_file(config["files"]["uwsgi_config"], "/etc/init/uwsgi.conf")
    aws_util.install_file(config["files"]["uwsgi_logrotate"], "/etc/logrotate.d/uwsgi")

    sudo("touch /var/log/uwsgi.log")
    with settings(warn_only=True):
        sudo("logrotate -f /etc/logrotate.d/uwsgi")
    sudo("mkdir -p /var/www")
    sudo("chgrp -R www-data /var/www/")
    sudo("mkdir -p " + telemetry_server)
    sudo("chown -R telemetry " + telemetry_server)
    sudo("chmod -R g+w " + telemetry_server)

    # Install s3funnel
    home = "/home/" + ssl_user
    with cd(home):
        run("git clone https://github.com/sstoiana/s3funnel.git")
    with cd(home + "/s3funnel"):
        sudo("python setup.py install")

    with cd("/home/telemetry"):
        sudo('git clone https://github.com/mreid-moz/telemetry-server.git', user="telemetry")
    with cd(telemetry_home):
        sudo('bash get_histogram_tools.sh', user="telemetry")
        sudo('bash util/install_server.sh ' + telemetry_server, user="telemetry")
    aws_util.install_file(config["files"]["server_config"], telemetry_server + "/telemetry_server_config.json")
    sudo("chown telemetry:www-data %s/telemetry_server_config.json" % (telemetry_server))

    sudo('virtualenv ' + telemetry_server + '/env && source ' + telemetry_server + '/env/bin/activate && pip install Flask simplejson scales', user="telemetry")

    sites_avail = "/etc/nginx/sites-available/"
    t_site = sites_avail + "telemetry-server"
    # Can't just upload a file for this since it needs the dns name in there.
    sudo("echo 'server {' > %s" % (t_site))
    sudo("echo '    listen       80;' >> %s" % (t_site))
    sudo("echo '    server_name  %s;' >> %s" % (t_site, instance.public_dns_name))
    sudo("echo '    client_max_body_size 20M;' >> %s" % (t_site))
    sudo("echo '' >> %s" % (t_site))
    sudo("echo '    location / {' >> %s" % (t_site))
    sudo("echo '        include uwsgi_params;' >> %s" % (t_site))
    sudo("echo '        uwsgi_pass unix:/tmp/uwsgi.sock;' >> %s" % (t_site))
    sudo("echo '        uwsgi_param UWSGI_PYHOME %s/env;' >> %s" % (telemetry_server, t_site))
    sudo("echo '        uwsgi_param UWSGI_CHDIR %s;' >> %s" % (telemetry_server, t_site))
    sudo("echo '        uwsgi_param UWSGI_MODULE server;' >> %s" % (t_site))
    sudo("echo '        uwsgi_param UWSGI_CALLABLE app;' >> %s" % (t_site))
    sudo("echo '    }' >> %s" % (t_site))
    sudo("echo '}' >> %s" % (t_site))

    # Now do the SSL version:
    # FIXME: use a real SSL cert. For now, we'll generate a self-signed one though:
    cert_base = "telemetry_server"

    with cd("/etc/ssl"):
        sudo('openssl req -new -newkey rsa:4096 -days 365 -nodes -x509 -subj "/C=US/ST=California/L=Mountain View/O=Mozilla/CN=%s" -keyout %s.key  -out %s.crt' % (instance.public_dns_name, cert_base, cert_base))
        #sudo("openssl genrsa -out %s.key 2048" % (cert_base))
        #sudo("openssl req -new -key %s.key -out %s.csr" % (cert_base, cert_base))
        #sudo("openssl x509 -req -days 365 -in %s.csr -signkey %s.key -out %s.crt" % (cert_base, cert_base, cert_base))

    t_site = sites_avail + "telemetry-server-ssl"
    sudo("echo 'server {' > %s" % (t_site))
    sudo("echo '    listen       443;' >> %s" % (t_site))
    sudo("echo '    server_name  %s;' >> %s" % (instance.public_dns_name, t_site))
    sudo("echo '    client_max_body_size 20M;' >> %s" % (t_site))
    sudo("echo '    ssl on;' >> %s" % (t_site))
    sudo("echo '    ssl_certificate /etc/ssl/%s.crt;' >> %s" % (cert_base, t_site))
    sudo("echo '    ssl_certificate_key /etc/ssl/%s.key;' >> %s" % (cert_base, t_site))
    sudo("echo '' >> %s" % (t_site))
    sudo("echo '    location / {' >> %s" % (t_site))
    sudo("echo '        include uwsgi_params;' >> %s" % (t_site))
    sudo("echo '        uwsgi_pass unix:/tmp/uwsgi.sock;' >> %s" % (t_site))
    sudo("echo '        uwsgi_param UWSGI_PYHOME %s/env;' >> %s" % (telemetry_server, t_site))
    sudo("echo '        uwsgi_param UWSGI_CHDIR %s;' >> %s" % (telemetry_server, t_site))
    sudo("echo '        uwsgi_param UWSGI_MODULE server;' >> %s" % (t_site))
    sudo("echo '        uwsgi_param UWSGI_CALLABLE app;' >> %s" % (t_site))
    sudo("echo '    }' >> %s" % (t_site))
    sudo("echo '}' >> %s" % (t_site))

    # install sites
    sudo("rm /etc/nginx/sites-enabled/default")
    sudo("ln -s /etc/nginx/sites-available/telemetry-server /etc/nginx/sites-enabled/telemetry-server")
    sudo("ln -s /etc/nginx/sites-available/telemetry-server-ssl /etc/nginx/sites-enabled/telemetry-server-ssl")

    # configure nginx (mostly just to add 'client_max_body_size 20M;')
    aws_util.install_file(config["files"]["nginx_config"], "/etc/nginx/nginx.conf")

    # copy over a starter-pack of cached histograms
    # TODO: generate a more up-to-date pack from mercurial? or otherwise use
    #       something that's not going to get stale.
    put(config["files"]["histograms"], "/tmp/histograms.tar.bz2")
    sudo("chmod 644 /tmp/histograms.tar.bz2")

    # Use server config to find the correct histogram_cache location
    revision_cache_path = server_config["revision_cache_path"]
    if revision_cache_path.startswith("./"):
        revision_cache_path = telemetry_server + revision_cache_path[1:]
    if revision_cache_path.endswith("/histogram_cache"):
        revision_cache_path = os.path.dirname(revision_cache_path)

    with cd(revision_cache_path):
        sudo("tar xjvf /tmp/histograms.tar.bz2", user="telemetry")

    # Add cron jobs
    sudo('crontab -l | { cat; echo "4 * * * * sudo -u uwsgi /bin/bash %s/util/archive_logs.sh %s %s"; } | crontab -' % (telemetry_home, telemetry_server, telemetry_data))
    sudo("crontab -l | { cat; echo '34 * * * * sudo -u uwsgi /usr/bin/python %s/export.py -d %s -k \"%s\" -s \"%s\" -b \"%s\" >> /tmp/telemetry_export.log'; } | crontab -" % (telemetry_home, telemetry_data, config["aws_key"], config["aws_secret_key"], config["aws_bucket"]))

    # Restart web services
    sudo("service uwsgi restart")
    sudo("service nginx restart")


if len(sys.argv) < 2:
    print "Usage:", sys.argv[0], "/path/to/config_file.json"
    sys.exit(1)

config_file = open(sys.argv[1], "r")
config = json.load(config_file)
config_file.close()
print "Using the following config:"
print json.dumps(config)

if "instance_id" in config:
    print "Using already-created instance", config["instance_id"]
    conn = aws_util.connect_cfg(config)
    instance = aws_util.get_instance(conn, config["instance_id"])
    print "Instance", instance.id, "is", instance.state
else:
    print "Creating a new instance"
    conn, instance = create_instance(config)

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

bootstrap_instance(config, instance)

print "All finished with instance", instance.id
print "To connect to it: ssh -i", ssl_key_path, ssl_host
