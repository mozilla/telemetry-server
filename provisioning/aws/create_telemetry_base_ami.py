#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# Example invocation:
# $ cd /path/to/telemetry-server
# $ python -m provisioning.aws.create_telemetry_base_ami -k "my_aws_key" -s "my_aws_secret" provisioning/aws/telemetry_server_base.pv.json

from launch_telemetry_server import TelemetryServerLauncher
from fabric.api import *
import fabric.network
import sys
import traceback
import time
from datetime import date

def main():
    try:
        launcher = TelemetryServerLauncher()
        launcher.go()
        conn = launcher.get_connection()
        instance = launcher.get_instance()
        print "Instance", instance.id, "is now configured. Stopping it."
        stopping_instances = conn.stop_instances(instance_ids=[instance.id])
        instance.update()
        for i in range(120):
            print i, "Instance is", instance.state
            if instance.state == "stopped":
                break
            time.sleep(1)
            instance.update()

        print "Creating an AMI..."
        # Create an AMI (after stopping the instance)
        # Give it a good name %s-yyyymmdd where %s is instance name stolen from
        # launcher which reads it from config or commandline
        base_name = launcher.config["name"]
        ami_name = "{0}-{1}".format(base_name, date.today().strftime("%Y%m%d"))
        ami_desc = 'Pre-loaded image for telemetry nodes. Knows how to run all the core services, but does not auto-start them on boot.'
        # This automatically stops the image first (unless you tell it not to)
        ami_id = conn.create_image(instance.id, ami_name, description=ami_desc)
        print "Created a new AMI:"
        print "    ID:", ami_id
        print "  Name:", ami_name
        print "  Desc:", ami_desc
        # Get the image and wait for it to be available:
        ami_image = conn.get_image(ami_id)
        retry_count = 0
        while retry_count < 15 and ami_image.state != "available":
            retry_count += 1
            print "AMI is", ami_image.state, "... waiting 10s for it to become available"
            time.sleep(10)
            ami_image.update()
        print "AMI is", ami_image.state
        if ami_image.state != "available":
            print "The image is not quite available yet, but you're probably bored of waiting, so we'll continue."
        # Now clean up the instance.
        print "Terminating instance", instance.id
        launcher.terminate(conn, instance)
        print "Those AMI details again:"
        print "    ID:", ami_id
        print "  Name:", ami_name
        print "  Desc:", ami_desc
        return 0
    except Exception, e:
        print "Error:", e
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
