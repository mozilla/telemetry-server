#!/usr/bin/env python
# encoding: utf-8
"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""

import boto.ec2

def connect(region, aws_key, aws_secret_key):
    # Use AWS keys from config
    conn = boto.ec2.connect_to_region(region,
            aws_access_key_id=aws_key,
            aws_secret_access_key=aws_secret_key)
    return conn
