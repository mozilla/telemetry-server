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
