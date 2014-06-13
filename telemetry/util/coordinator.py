#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import sys
import urllib2
import logging
try:
    import simplejson as json
except ImportError as e:
    import json


class Coordinator:
    def __init__(self, hostname, port=8080, timeout=10):
        self.hostname = hostname
        self.port = port
        self.base_url = "http://{0}:{1}".format(self.hostname, self.port)
        self.timeout = timeout
        self.logger = logging.getLogger(__name__)

    def files(self, schema):
        url = "{0}/files".format(self.base_url)
        headers = { "Content-Type": "application/json" }
        if isinstance(schema, basestring):
            schema = json.loads(schema)

        request_json = json.dumps({"filter": schema}, separators=(',', ':'))

        self.logger.debug("Fetching URL: {}".format(url))
        self.logger.debug("Sending request json: {}".format(request_json))
        requestHandle = urllib2.Request(url, request_json, headers)
        response = urllib2.urlopen(requestHandle, timeout=self.timeout)
        response_json = response.read()
        self.logger.debug("Response: {}".format(response_json))
        return json.loads(response_json)


def main():
    c = Coordinator(sys.argv[1])
    schema = {
        "dimensions": [
            { "allowed_values": ["saved-session"], "field_name": "reason" },
            { "allowed_values": ["Fennec"], "field_name": "appName" },
            { "allowed_values": ["nightly"], "field_name": "appUpdateChannel" },
            { "allowed_values": "22.0a1", "field_name": "appVersion" },
            { "allowed_values": "*", "field_name": "appBuildID" },
            { "allowed_values": "*", "field_name": "submission_date" }
        ],
        "version": 1
    }

    result = c.files(schema)
    print "Found", result["row_count"], "rows:"
    for f in result["files"]:
        print f
    return 0

if __name__ == "__main__":
    sys.exit(main())
