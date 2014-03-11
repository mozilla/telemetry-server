"""
Get the distribution of one or more boolean/enumerated measurements.
"""

import json

keys = [
    ("NEWTAB_PAGE_SHOWN", 2), # boolean
    ("NEWTAB_PAGE_SITE_CLICKED", 10), # 9-bucket
]

extra_histogram_entries = 6 # bucketN, sum, log_sum, log_sum_squares, sum_squares_lo, sum_squares_hi

def map(k, d, v, cx):
    j = json.loads(v)
    histograms = j.get("histograms", {})

    counts = ()
    for key, buckets in keys:
        if key in histograms:
            val = histograms[key]
            if len(val) != buckets + extra_histogram_entries:
                raise ValueError("Unexpected length for key %s: %s" % (key, val))
            counts += tuple(val[0:buckets])
        else:
            counts += (0,) * buckets
    cx.write(counts, 1)

def reduce(k, v, cx):
    cx.writecsv(list(k) + [sum(v)])
