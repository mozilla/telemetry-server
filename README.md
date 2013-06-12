Telemetry Server
================

Server for the Mozilla Telemetry project

Roadmap
=======

1. Nail down the new storage format [Bug 856263](https://bugzilla.mozilla.org/show_bug.cgi?id=856263)
2. Define on-disk storage structure [telemetry-reboot](https://etherpad.mozilla.org/telemetry-reboot)
3. Build a converter to take existing data as input and output in the new format + structure
4. Build mapreduce job to take new format + structure as input and output data as required by the [telemetry-frontend](https://github.com/tarasglek/telemetry-frontend)
5. Plumb converter into the current pipeline (Bagheera -> Kafka -> converter -> format.v2)
6. Build replacement frontend acquisition pipeline (HTTP -> persister -> format.v2)


1. Storage Format
-----------------

First, we'll need to sort out the new and efficient format for storing Histograms.

The new Key for a Histogram will be its `Histogram ID`, which will be calculated as the index into the sorted list of histogram names from [Histograms.json](http://hg.mozilla.org/mozilla-central/file/tip/toolkit/components/telemetry/Histograms.json).

The new Value for a Histogram will be calculated as an array containing:
`[bucket0, bucket1, ..., bucketN, sum, log_sum, log_sum_squares]`
Having the metadata fields at the end of the array has the advantage that buckets are located at the expected offset in the array, and metadata can be referenced with "N from the end" type indices (or negative indices, depending on the language).

As an example, we'll look at the `DOM_TIMERS_FIRED_PER_NATIVE_TIMEOUT` Histogram:
``` json Original format (minimal)
"DOM_TIMERS_FIRED_PER_NATIVE_TIMEOUT":{"log_sum_squares":624.027626294358,"log_sum":873.474196434021,"sum":1279,"values":{"8":0,"3":7,"1":1232,"0":0},"histogram_type":0,"bucket_count":10,"range":[1,3000]}
```
``` json Original format (expanded for readability)
"DOM_TIMERS_FIRED_PER_NATIVE_TIMEOUT": {
  "log_sum_squares": 624.027626294358,
  "log_sum": 873.474196434021,
  "sum": 1279,
  "values": {
    "8": 0,
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
``` json Converted format (minimal)
"162":[0,1232,0,7,0,0,0,0,0,0,1279,873.474196434021,624.027626294358]
```
``` json Converted format (expanded)
"162": [                // DOM_TIMERS_FIRED_PER_NATIVE_TIMEOUT is the 162nd Histogram in Histograms.json (by name)
  0,                    // Bucket 0
  1232,                 // Bucket 1
  0,                    // Bucket 2
  7,                    // Bucket 3
  0,                    // .
  0,                    // .
  0,                    // .
  0,                    
  0,                    
  0,                    // Bucket N
  1279,                 // sum
  873.474196434021,     // log_sum
  624.027626294358      // log_sum_squares
]
```

One thing still to sort out is how to relate this submission back to the correct Histograms.json (if that is even neccessary).



2. On-disk Storage Structure
----------------------------

The basic idea is to partition the data by a number of useful dimensions, then use the dimensions to form a directory hierarchy. Finally, the actual submissions will be stored in compressed files that may be read and processed in parallel. The files in a directory will be split into manageable sized pieces. Each line in the file will be of the form <uuid><tab><json>.

The main thing to define here is exactly which dimensions will be used for partitioning, and in which order to apply them.

If we used channel, submission day, and operating system, we would end up with a structure like this
``` bash File Layout
20130612/
  nightly/
    winnt/
      001.lz4
      002.lz4
    darwin/
      001.lz4
    linux/
      001.lz4
  aurora/
    winnt/
      001.lz4
      002.lz4
      ...
      005.lz4
    darwin/
      001.lz4
    linux/
      001.lz4
  beta/
    winnt/
      001.lz4
      002.lz4
      ...
      005.lz4
    darwin/
      001.lz4
    linux/
      001.lz4
  release/
    winnt/
      001.lz4
      002.lz4
      ...
      061.lz4
    darwin/
      001.lz4
      002.lz4
    linux/
      001.lz4
  other/
    winnt/
      001.lz4
    darwin/
      001.lz4
20130611/
  nightly/
...
```


3. Data Converter
-----------------

1. Find the correct Histograms.json for a given payload
    1. Use `revision` if possible
    2. Fall back to `appUpdateChannel` and `appBuildID` or `appVersion` as needed
    3. Use the Mercurial history to export each version of Histograms.json with the date range it was in effect for each repo (mozilla-central, -aurora, -beta, -release)
    4. Keep local cache of Histograms.json versions to avoid re-fetching
2. Filter out bad submission data
    1. Invalid histogram names
    2. Histogram configs that don't match the expected parameters (histogram type, num buckets, etc)
    3. Keep metrics for bad data


4. MapReduce
------------

We will implement a lightweight MapReduce framework that uses the Operating System's support for parallelism.  It will rely on simple binaries for the Map, Combine, and Reduce phases.

For data stored on multiple machines, each machine will run a combine phase, with the final reduce combining output for the entire cluster.

5. Plumbing
-----------

Once we have the converter and MapReduce framework available, we can easily consume from the existing Telemetry data source. This will mark the first point that the new dashboards can be fed with live data.

6. Data Acquisition
-------------------

If necessary, we will route the client (Firefox) submissions directly into the new pipeline.
