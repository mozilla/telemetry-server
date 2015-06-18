Telemetry Server
================

Server components to receive, validate, convert, store, and process Telemetry
data from the [Mozilla Firefox](http://www.mozilla.org) browser.

Talk to us on `irc.mozilla.org` in the `#telemetry` channel, or visit the
[Project Wiki][3] for more information.

See the [TODO list](TODO.md) for some outstanding tasks.


Storage Format
-----------------
See [StorageFormat][1] for details.


On-disk Storage Structure
----------------------------
See [StorageLayout][2] for details.

Data Converter
-----------------
1. Use [RevisionCache](telemetry/revision_cache.py) to load the correct Histograms.json for a given payload
    1. Use `revision` if possible
    2. Fall back to `appUpdateChannel` and `appBuildID` or `appVersion` as needed
    3. Use the Mercurial history to export each version of Histograms.json with the date range it was in effect for each repo (mozilla-central, -aurora, -beta, -release)
    4. Keep local cache of Histograms.json versions to avoid re-fetching
2. Filter out bad submission data
    1. Invalid histogram names
    2. Histogram configs that don't match the expected parameters (histogram type, num buckets, etc)
    3. Keep metrics for bad data

MapReduce
------------
We have implemented a lightweight [MapReduce framework][6] that uses the Operating System's support for parallelism.  It relies on simple python functions for the Map, Combine, and Reduce phases.

For data stored on multiple machines, each machine will run a combine phase, with the final reduce combining output for the entire cluster.

Mongodb Importer
----------------
Telemetry data can be optionally imported into mongodb. The benefits of doing that is
the reduced time to run multiple map-reduce jobs on the same dataset, as mongodb keeps
as much data as possible in memory.

1. Start mongodb, e.g. `mongod --nojournal`
2. Fetch a dataset from S3, e.g. `aws s3 cp s3://... /mnt/yourdataset --recursive`
3. Import the dataset, e.g. `python3 -m mongodb.importer /mnt/yourdataset`
4. Run a map-reduce job, e.g. `mongo localhost/telemetry mongodb/examples/osdistribution.js`

Plumbing
-----------
Once we have the converter and MapReduce framework available, we can easily consume from the existing Telemetry data source. This will mark the first point that the new dashboards can be fed with live data.

Integration with the existing pipeline is discussed in more detail on the [Bagheera Integration][7] page.

Data Acquisition
-------------------

When everything is ready and productionized, we will route the client (Firefox) submissions directly into the [new pipeline][8].


Code Overview
=============

These are the important parts of the Telemetry Server architecture.

`http/server.js`
-----------
Contains the Node.js HTTP server for receiving payloads. The server's job is
simply to write incoming submissions to disk as quickly as possible.

It accepts single submissions using the same type of URLs supported by
[Bagheera][7], and expects (but doesn't require) the [partition information][9]
to be submitted as part of the URL.

To set up a test server locally:

1. Install node.js (left as an exercise to the reader)
2. Edit `http/server_config.json`, replacing `log_path` and `stats_log_file` with directories suitable to your machine
3. Run the server using `cd http; node ./server.js ./server_config.js`
4. Send some test data to the server. Using curl: `curl -X POST http://127.0.0.1:8080/submit/telemetry/foo/bar/baz -d '{"test": 1}'`

Stop the server, and check that there is a `telemetry.log.<something>.finished` file in the directory you specified in step 2 above.

You can examine the resulting file in python (from the root of the repo):
```python
import telemetry.util.files as fu
for r in fu.unpack('/path/to/telemetry.log.<something>.finished'):
    print "URL Path:", r.path
    print "JSON Payload:", r.data
    print "Submission Timestamp:", r.timestamp
    print "Submission IP:", r.ip
    print "Error (if any):", r.error
```

`telemetry/convert.py`
------------
Contains the `Converter` class, which is used to convert a JSON payload from
the raw form submitted by Firefox to the more compact [storage format][1] for
on-disk storage and processing.

You can run the main method in this file to process a given data file (the
expected format is one record per line, each line containing an id followed by
a tab character, followed by a json string).

You can also use the `Converter` class to convert data in a more flexible way.

`telemetry/export.py`
-----------
Contains code to export data to Amazon S3.

`telemetry/persist.py`
------------
Contains the `StorageLayout` class, which is used to save payloads to disk
using the directory structure as documented in the [storage layout][2] section
above.

`telemetry/revision_cache.py`
-------------------
Contains the `RevisionCache` class, which provides a mechanism for fetching
the `Histograms.json` spec file for a given revision URL. Histogram data is
cached locally on disk and in-memory as revisions are requested.

`telemetry/telemetry_schema.py`
---------------------
Contains the `TelemetrySchema` class, which encapsulates logic used by the
StorageLayout and MapReduce code.

`process_incoming/process_incoming_mp.py`
------------------------
Contains the multi-process version of the data-transformation code. This is
used to download incoming data (as received by the HTTP server), validate and
convert it, then publish the results back to S3.

`process_incoming/worker`
----
Contains the C++ data validation and conversion routines.

Prerequisites
----
* Clang 3.1 or GCC 4.7.0 or Visual Studio 10
* CMake (2.8.7+) - http://cmake.org/cmake/resources/software.html
* Boost (1.54.0) - http://www.boost.org/users/download/
* zlib
* OpenSSL
* Protobuf

Optional (used for documentation)
----
* Graphviz (2.28.0) - http://graphviz.org/Download..php
* Doxygen (1.8+)- http://www.stack.nl/~dimitri/doxygen/download.html#latestsrc

convert - Build instructions (from the telemetry-server root)
----
    mkdir release
    cd release
    cmake -DCMAKE_BUILD_TYPE=release ..
    make

Configuring the converter
----
* `heka_server` (string) - Hostname:port of the heka log/stats service.
* `histogram_server` (string) - Hostname:port of the histogram.json web service.
* `telemetry_schema` (string) - JSON file containing the dimension mapping.
* `histogram_server` (string) - Hostname:port of the histogram.json web service.
* `storage_path` (string) - Converter output directory
* `upload_path` (string) - Staging directory for S3 uploads.
* `max_uncompressed` (int) - Maximum uncompressed size of a telemetry record.
* `memory_constraint` (int) -
* `compression_preset` (int) -

```
    {
        "heka_server": "localhost:5565",
        "telemetry_schema": "telemetry_schema.json",
        "histogram_server": "localhost:9898",
        "storage_path": "storage",
        "upload_path": "upload",
        "max_uncompressed": 1048576,
        "memory_constraint": 1000,
        "compression_preset": 0
    }
```


Setting up/running the histogram server
---

    pushd http
    ./get_histogram_tools.sh
    popd
    python -m http.histogram_server

Running the converter
----
*in the release directory*

    mkdir input
    ./convert convert.json input.txt

    # input.txt should contain a list of files to process (newline delimited)
    # i.e. /<path to telemetry-server>/release/input/telemetry1.log

*from another shell, in the release directory*

    cp ../process_incoming/worker/common/test/data/telemetry1.log input

Without the histogram server running it will produce something like this:

    processing file:"telemetry1.log"
    LoadHistogram - connect: Connection refused
    ConvertHistogramData - histogram not found: http://hg.mozilla.org/releases/mozilla-release/rev/a55c55edf302
    done processing file:"telemetry1.log" processed:1 failures:1 time:0.001871 throughput (MiB/s):9.3563 data in (B):18356 data out (B):0

With the histogram server running:

    processing file:"telemetry1.log"
    done processing file:"telemetry1.log" processed:1 failures:0 time:0.013622 throughput (MiB/s):1.2851 data in (B):18356 data out (B):45909

Ubuntu Notes
----
```
apt-get install cmake libprotoc-dev zlib1g-dev libboost-system1.54-dev \
   libboost-filesystem1.54-dev libboost-thread1.54-dev libboost-test1.54-dev \
   libboost-log1.54-dev libboost-regex1.54-dev protobuf-compiler libssl-dev \
   liblzma-dev xz-utils
```

`mapreduce/job.py`
--------
Contains the [MapReduce][6] code. This is the interface for running jobs on
Telemetry data. There are example job scripts and input filters in the
`examples/` directory.

`provisioning/aws/*`
-----------------------
Contains scripts to provision and launch various kinds of cloud services. This
includes launching a telemetry server node, a MapReduce job, or a node to
process incoming data.

`monitoring/heka/*`
--------
Contains the configuration used by [Heka][4] to process server logs.

[1]: docs/StorageFormat.md "Storage Format"
[2]: docs/StorageLayout.md "On-disk Storage Layout"
[3]: https://wiki.mozilla.org/Telemetry/Reboot "Telemetry Reboot wiki"
[4]: http://hekad.readthedocs.org/ "Heka"
[6]: docs/MapReduce.md "Telemetry MapReduce Framework"
[7]: docs/BagheeraIntegration.md "Integration with Bagheera"
[8]: http/server.js "Telemetry Server"
[9]: https://bugzilla.mozilla.org/show_bug.cgi?id=860846 "Bug 860846"
