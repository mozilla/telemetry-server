#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

try:
    import matplotlib as mp
    import matplotlib.pylab
    mp_available = True
except ImportError:
    mp_available = False

import numpy
import os
import sys
import simplejson as json
import time
from argparse import ArgumentParser
from datetime import datetime, timedelta

from urllib2 import urlopen

def parse(url, column_aliases=None):
    #print "Getting url:", url
    lines = urlopen(url).readlines()
    meta = json.loads(lines[0].decode())

    now = datetime.fromtimestamp(meta['time'])
    #print "request time:", now
    rows = meta['rows']
    delta = timedelta(seconds=meta['seconds_per_row'])
    data = []
    for i in range(1, len(lines)):
        line = lines[i].strip()
        offset = (rows - i - 1) * delta
        pieces = line.split("\t")
        datum = { "time": now - offset }
        for p in range(len(pieces)):
            val = pieces[p]
            if val == 'nan':
                continue
            col = meta['column_info'][p]["name"]
            if column_aliases is not None and col in column_aliases:
                col = column_aliases[col]
            datum[col] = int(pieces[p])
        if len(datum.keys()) > 1:
            data.append(datum)
    return data

def combine_by_hour(data, combined={}):
    for datum in data:
        d = datum['time']
        trunc = datetime(d.year, d.month, d.day, d.hour).isoformat()
        if trunc not in combined:
            combined[trunc] = {}
        for k in datum.keys():
            if k == 'time':
                continue
            if k in combined[trunc]:
                combined[trunc][k] += datum[k]
            else:
                combined[trunc][k] = datum[k]
    return combined

def pct(numerator, denominator):
    return float(numerator) / max(denominator, 1) * 100

def calculate_rates(data):
    for hour, datum in data.iteritems():
        records_read = datum.get('Records_Read', 0)
        records_per_second = records_read / 60 / 60;
        bad_records = datum.get('Bad_Records', 0)
        bad_record_percentage = pct(bad_records, records_read)
        interesting_bad_records = bad_records
        for t in ['uuid_only_path', 'missing_revision', 'empty_data']:
            interesting_bad_records -= datum.get(t, 0)
        interesting_bad_record_pct = pct(interesting_bad_records, records_read)
        datum['Records_Read_Per_Second'] = records_per_second
        datum['Bad_Record_Percentage'] = bad_record_percentage
        datum['Interesting_Bad_Record_Percentage'] = interesting_bad_record_pct
        datum['UUID_Bad_Record_Percentage'] = pct(datum.get('uuid_only_path', 0), records_read)
    return data

def get_url(target, debug=False):
    if debug:
        return "file://{}/sample_data/{}".format(os.path.dirname(os.path.realpath(__file__)), target)
    host = "ec2-50-112-66-71.us-west-2.compute.amazonaws.com"
    port = 4352
    path = "/data/"
    return "http://{}:{}{}{}".format(host, port, path, target)

def get_graph_url():
    return "http://ec2-50-112-66-71.us-west-2.compute.amazonaws.com:4352/#sandboxes/TelemetryStatsRecordsAggregator/outputs/TelemetryStatsRecordsAggregator.ReaderALL.cbuf"

def render(data, error_types, display=True, save_to_filename=None):
    if not mp_available:
        raise RuntimeError("Sorry, matplotlib is not available. Install it to display the data.")
    dates = data.keys()
    dates.sort()
    rps = [ data[d]['Records_Read_Per_Second'] for d in dates ]
    bad_pct = [ data[d]['Bad_Record_Percentage'] for d in dates ]
    interesting_bad = [ data[d]['Interesting_Bad_Record_Percentage'] for d in dates ]

    mp.pyplot.figure(1)
    num_subplots = len(error_types) + 3
    mp.pyplot.subplot(num_subplots, 1, 1)
    mp.pyplot.plot(rps, 'k')
    mp.pyplot.ylabel('Records per sec')

    mp.pyplot.subplot(num_subplots, 1, 2)
    mp.pyplot.plot(bad_pct, 'r--')
    mp.pyplot.ylabel('% Bad Records')
    mp.pyplot.ylim(0,10)

    mp.pyplot.subplot(num_subplots, 1, 3)
    mp.pyplot.plot(interesting_bad, 'r+')
    mp.pyplot.ylabel('% Interesting bad records')

    for i in range(len(error_types)):
        et = error_types[i]
        mp.pyplot.subplot(num_subplots, 1, i + 4)
        mp.pyplot.plot([ data[d].get(et, 0) for d in dates ], 'r')
        mp.pyplot.ylabel(et)

    if save_to_filename:
        mp.pyplot.savefig(save_to_filename, bbox_inches='tight')

    if display:
        mp.pyplot.show()

def alert(data, max_error_rate=10, max_interesting_error_rate=1):
    # If error_rate > threshold, we should alert.
    # We also look at the "interesting" error rates.
    # There are a bunch of known error types we don't
    # really care about (UUID-only), but some we do.
    dates = data.keys()
    dates.sort()
    errors = []
    for d in dates:
        if data[d]['Bad_Record_Percentage'] > max_error_rate:
            errors.append("Overall Bad Record rate exceeded threshold on {}: {}% > {}%".format(
                d, data[d]['Bad_Record_Percentage'], max_error_rate))
        if data[d]['Interesting_Bad_Record_Percentage'] > max_interesting_error_rate:
            errors.append("Interesting bad record exceeded threshold on {}: {}% > {}%".format(
                d, data[d]['Interesting_Bad_Record_Percentage'], max_interesting_error_rate))
    if errors:
        print "Errors detected. Check graph at\n{}\n".format(get_graph_url())
        for error in errors:
            print error

def combine_with(main_data, additional_data):
    # TODO: have some cutoff in the age of keys we keep. 2 years?
    for k, v in additional_data.iteritems():
        if k not in main_data:
            main_data[k] = v
    return main_data


def main(args):
    # Graph processing rate and error rate per hour
    data = parse(get_url('TelemetryStatsRecordsAggregator.ReaderALL.cbuf', debug=args.debug))
    combined = combine_by_hour(data)

    error_types = [
        'conversion_error',
        'missing_revision',
        'missing_revision_repo',
        'empty_data',
        'uuid_only_path',
        'write_failed',
        'bad_payload',
        'invalid_path',
        'corrupted_data'
    ]
    for error_type in error_types:
        m = {"Total_Errors": error_type}
        url = get_url('TelemetryStatsErrorsAggregator.{}.cbuf'.format(error_type), debug=args.debug)
        rev_errors = parse(url, m)
        combined = combine_by_hour(rev_errors, combined)

    calculated = calculate_rates(combined)

    # Alert before we combine with old data. We don't want to keep repeating
    # alerts forever.
    alert(calculated,
          max_error_rate=args.overall_threshold,
          max_interesting_error_rate=args.interesting_threshold)

    if args.combine_with:
        with open(args.combine_with, 'r') as more_data_file:
            try:
                more_data = json.load(more_data_file)
                combine_with(calculated, more_data)
            except Exception as e:
                print "Error loading additional data from", args.combine_with

    if args.save_data_as:
        with open(args.save_data_as, 'w') as fout:
            json.dump(calculated, fout)

    interesting_error_types = [
        'conversion_error',
        'write_failed',
        'bad_payload',
        'invalid_path',
        'corrupted_data',
        'UUID_Bad_Record_Percentage'
    ]

    if args.display or args.save_graph_as:
        render(calculated, interesting_error_types, args.display, args.save_graph_as)

    return 0

if __name__ == "__main__":
    parser = ArgumentParser(description='Check Telemetry processing error rate')
    parser.add_argument("--display", action="store_true")
    parser.add_argument("--save-graph-as")
    parser.add_argument("--save-data-as")
    parser.add_argument("--combine-with")
    parser.add_argument("--overall-threshold", default=10, type=float)
    parser.add_argument("--interesting-threshold", default=1, type=float)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    sys.exit(main(args))

