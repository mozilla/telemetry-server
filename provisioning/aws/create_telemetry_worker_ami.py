#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# Example invocation:
# $ cd /path/to/telemetry-server
# $ python -m provisioning.aws.create_telemetry_worker_ami -k "my_aws_key" -s "my_aws_secret" provisioning/aws/telemetry_worker.hvm.json

from aws_launcher import Launcher
from create_ami import AmiCreator
import sys
import traceback

def main():
    launcher = Launcher()
    creator = AmiCreator(launcher)
    try:
        result = creator.create('Pre-loaded image for telemetry workers. Use ' \
                                'it for scheduled or adhoc jobs.')
        return result
    except Exception, e:
        print "Error:", e
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
