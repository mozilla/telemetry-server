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

See [StorageFormat.md](StorageFormat.md) for details.


2. On-disk Storage Structure
----------------------------

The basic idea is to partition the data by a number of useful dimensions, then use the dimensions to form a directory hierarchy. Finally, the actual submissions will be stored in compressed files that may be read and processed in parallel. The files in a directory will be split into manageable sized pieces. Each line in the file will be of the form <uuid><tab><json>.

The main thing to define here is exactly which dimensions will be used for partitioning, and in which order to apply them.

If we used channel, submission day, and operating system, we would end up with a structure like this:
```bash
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
