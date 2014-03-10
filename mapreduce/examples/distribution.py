"""
Get the distribution of one or more boolean/enumerated measurements.
"""

import json

keys = [
    ("NEWTAB_PAGE_SHOWN", 2), # boolean
    ("NEWTAB_PAGE_SITE_CLICKED", 10), # 9-bucket
]

def map(k, d, v, cx):
    j = json.loads(v)
    histograms = j.get("histograms", {})

    counts = ()
    for key, buckets in keys:
        if key in histograms:
            val = histograms[key]
            if len(val) != buckets + 5:
                raise ValueError("Unexpected length for key %s: %s" % (key, val))
            counts += tuple(val[0:buckets])
        else:
            counts += (0,) * buckets
    cx.write(counts, 1)

def reduce(k, v, cx):
    cx.writeline(",".join(list(k) + [sum(v)]))
