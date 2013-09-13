Telemetry Data Storage Format
=============================

A specification for how Telemetry submissions will be transformed for
persistent server-side storage.


Overview
--------

The format submitted to the server is not ideal for long-term storage, since 
it contains a lot of redundant information. This info is useful for validation
but we do not want to store anything more than we need.

In particular, the Histograms section does not need to include all the metadata
about each histogram.

The Histogram metadata can be retrieved from [Histograms.json](http://hg.mozilla.org/mozilla-central/file/tip/toolkit/components/telemetry/Histograms.json)
in the Mozilla source, as long as we know which revision to look at for each
payload.

The Histogram name will continue to be the key for each histogram. If we can 
get significantly better compression by converting the name into a numeric ID,
we will use that instead, but the improved readability of the name makes it a
preferable key.

The new Value for a Histogram will be calculated as an array containing:
`[bucket0, bucket1, ..., bucketN, sum, log_sum, log_sum_squares]`

Having the metadata fields (sum, log_sum, etc) at the end of the array has the
advantage that buckets are located at the expected offset in the array, and
metadata can be referenced with "N from the end" type indices (or negative
indices, depending on the language).

Example
-------

As an example, we will look at the `DOM_TIMERS_FIRED_PER_NATIVE_TIMEOUT`
Histogram.
The allowed lower bounds (ie. keys in the "values" object) for this histogram
are: 0, 1, 3, 8, 21, 57, 154, 414, 1114, 3000

Original format (minimal):
```json
"DOM_TIMERS_FIRED_PER_NATIVE_TIMEOUT":{"log_sum_squares":624.027626294358,"log_sum":873.474196434021,"sum":1279,"values":{"1114":0,"414":1,"8":0,"3":7,"1":1232,"0":0},"histogram_type":0,"bucket_count":10,"range":[1,3000]}
```

Original format (expanded for readability):
```json
"DOM_TIMERS_FIRED_PER_NATIVE_TIMEOUT": {
  "log_sum_squares": 624.027626294358,
  "log_sum": 873.474196434021,
  "sum": 1279,
  "values": {
    "1114": 0,
    "414": 1,
    "8": 2,
    "3": 7,
    "1": 1232,
    "0": 0
  },
  "histogram_type": 0,
  "bucket_count": 10,
  "range": [
    1,
    3000
  ]
}
```

In the new storage form, this becomes:

Converted format (minimal):
```json
"DOM_TIMERS_FIRED_PER_NATIVE_TIMEOUT":[0,1232,7,2,0,0,0,1,0,0,1279,873.474196434021,624.027626294358]
```

Converted format (expanded):
```json
"DOM_TIMERS_FIRED_PER_NATIVE_TIMEOUT": [
  0,                    // Bucket 0
  1232,                 // Bucket 1
  7,                    // Bucket 2
  2,                    // Bucket 3
  0,                    // .
  0,                    // .
  0,                    // .
  1,                    // Bucket 7
  0,                    
  0,                    // Bucket N
  1279,                 // sum
  873.474196434021,     // log_sum
  624.027626294358      // log_sum_squares
]
```

Histograms.json Version
-----------------------

We need a way to determine which version of Histograms.json was used as the
reference specification for a given payload.

In modern payloads, this is contained in the `info.revision` field as described
in [Bug 832007](https://bugzilla.mozilla.org/show_bug.cgi?id=832007).

In payloads that predate the `revision` field, we can determine the
correct version of Histograms.json using a combination of `appUpdateChannel`
and `appBuildID` or `appVersion` as needed.

We can then insert the missing `revision` information before saving.

Ideally we do not want to have to modify the payload as part of the data
pipeline going forward, but `revision` information should only need to be added
to historic data and submissions from old browser versions, and as such should
require less and less processing over time.

