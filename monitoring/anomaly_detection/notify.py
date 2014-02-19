#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# If stdin contains any non-whitespace data, send it as an email using SES.

import argparse
from boto.ses import connect_to_region as ses_connect
import sys
import traceback
import simplejson as json

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Telemetry notifier")
    parser.add_argument("-c", "--config", help="Configuration file", type=file)
    parser.add_argument("-f", "--from-email", help="Email 'from:' address")
    parser.add_argument("-t", "--to-email", help="Email 'to:' address (multiple allowed)", action="append")
    parser.add_argument("-s", "--subject", help="Email Subject")
    parser.add_argument("-d", "--dry-run", help="Print out what would happen instead of sending email", action="store_true")
    args = parser.parse_args()

    message_body = sys.stdin.read().strip()

    if message_body == "":
        # nothing to notify about.
        if args.dry_run:
            print "Would not have sent any mail."
    else:
        try:
            config = json.load(args.config)
        except:
            traceback.print_exc()
            config = {}

        if args.from_email:
            config["notify_from"] = args.from_email

        if args.to_email:
            config["notify_to"] = args.to_email

        if args.subject:
            config["notify_subject"] = args.subject

        if args.dry_run:
            print "Here is what we would have sent:"
            print "   From:", config["notify_from"]
            print "     To:", config["notify_to"]
            print "Subject:", config["notify_subject"]
            print "   Body:", message_body
        else:
            ses = ses_connect('us-east-1') # only supported region!
            ses.send_email(
                source          = config["notify_from"],
                subject         = config["notify_subject"],
                format          = "text",
                body            = message_body,
                to_addresses    = config["notify_to"]
            )
