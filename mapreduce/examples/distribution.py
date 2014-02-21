"""
Get the distribution of a boolean measurement.
"""

import json

key = "NEWTAB_PAGE_SHOWN"

def map(k, d, v, cx):
    j = json.loads(v)
    histograms = j.get("histograms", {})
    count = 0
    if key in histograms:
        count = histograms[key][1]
    cx.write(count, 1)

def reduce(k, v, cx):
    cx.write(k, sum(v))
