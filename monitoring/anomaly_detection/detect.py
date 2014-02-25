#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# Example Usage:
# To detect changes in status since the last run and print results:
#    ./detect.py
# To display a graph of outages/anomalies for historic data on 2 channels:
#    ./detect.py aurora nightly
# To detect changes in status and email mreid with the results (if anything
# interesting has changed):
#    ./detect.py | ./notify.py \
#                      --to-email mreid@some.tld \
#                      --from-email telemetry-alerts@some.tld \
#                      --subject "Telemetry submission rate alert"

try:
    import matplotlib as mp
    import matplotlib.pylab
    mp_available = True
except ImportError:
    mp_available = False

import numpy
import sys
import simplejson as json
from datetime import datetime

from scipy import stats
from urllib2 import urlopen

sample_unit = None
day_units = None

def get_nth_day_series(series, day):
    start = len(series) - (day+1)*day_units

    if start < 0:
        raise ValueError("Start cannot be less than zero: {}".format(start))

    end = start + day_units
    return series[start:end]

def predict(series):
    try:
        subsets = [get_nth_day_series(series, i) for i in range(0, 10)]
        mean = numpy.mean(subsets[0])
        result = 0

        for ss in subsets:
            if numpy.mean(ss) - mean > 0:
                tstat = stats.mannwhitneyu(subsets[0], ss)
                result += tstat[1] < 0.0001

        return result > (len(subsets) >> 2)
    except ValueError, e:
        return 0

def get_host():
    return "ec2-50-112-66-71.us-west-2.compute.amazonaws.com"

def get_data_url(channel):
    return "http://{0}:4352/data/TelemetryChannelMetrics60DaysAggregator.{1}.cbuf".format(get_host(), channel)

def get_graph_url(channel):
    return "http://{0}:4352/#sandboxes/TelemetryChannelMetrics60DaysAggregator/outputs/TelemetryChannelMetrics60DaysAggregator.{1}.cbuf".format(get_host(), channel)

def parse(url, column_name):
    global sample_unit
    global day_units

    lines = urlopen(url).readlines()
    meta = json.loads(lines[0].decode())
    col_index = -1
    data = []

    sample_unit = meta['seconds_per_row']
    day_units = 24*60*60//sample_unit

    for idx, col_info in enumerate(meta['column_info']):
        if col_info['name'] == column_name:
            col_index = idx
    assert(col_index >= 0)

    for line in lines[1:]:
        data.append(int(line.decode().split()[col_index]))

    return data

if __name__ == "__main__":
    try:
        with open("last_run.json", "r") as f:
            last_run = json.load(f)
    except:
        last_run = {}

    timestamp = datetime.now().isoformat()
    changed = False
    if len(sys.argv) == 1:
        # Predict all channels and check with historic values.
        channels = ("nightly", "aurora", "beta", "release", "other", "ALL")
        predictions = [predict(parse(get_data_url(channel), "Requests")) for channel in channels]

        for channel, prediction in zip(channels, predictions):
            status = "ok"
            if prediction:
                status = "falling"

            if channel not in last_run:
                last_run[channel] = {"status": "unknown", "history": []}
            if "status" not in last_run[channel]:
                last_run[channel]["status"] = "unknown"
            if "history" not in last_run[channel]:
                last_run[channel]["history"] = []

            if status != last_run[channel]["status"]:
                changed = True
                print "Channel {0}: status changed from [{1}] to [{2}]. Graph URL: {3}".format(channel, last_run[channel]["status"], status, get_graph_url(channel))
                last_run[channel]["history"].append([last_run[channel]["status"], timestamp])
                last_run[channel]["status"] = status

        if changed:
            with open("last_run.json", "w") as f:
                json.dump(last_run, f)
            print json.dumps(last_run, sort_keys=True, indent=2, separators=(',', ': '))
    elif len(sys.argv) > 1:
        if not mp_available:
            print "Sorry, matplotlib is not available.  Please install it to continue..."
        else:
            for channel in sys.argv[1:]:
                # Display a graph of the specified channel
                series = parse(get_data_url(channel), "Requests")
                print "Detecting anomalies for channel: {0}. Graph URL: {1}".format(channel, get_graph_url(channel))
                results = [predict(series[:i]) for i in range(0, len(series))]
                cmap = mp.colors.ListedColormap(["white","red"], name='from_list', N=None)
                mp.pyplot.scatter(range(0, len(series)), series, c=results, cmap=cmap)
                mp.pylab.show()
