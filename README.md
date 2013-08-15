Telemetry Server
================

Server components to receive, validate, convert, store, and process Telemetry
data from the [Mozilla Firefox](http://www.mozilla.org) browser.

Talk to us on `irc.mozilla.org` in the `#telemetry` channel.

Roadmap
=======

### Next / In Progress:
See the [TODO list](TODO.md)

### Completed:
1. Nail down the new [storage format][1] based on [Bug 856263][4]
2. Define [on-disk storage structure][2] based on the [telemetry-reboot][5]
   etherpad
3. Build a [converter](convert.py) to take existing data as input and output
   in the new format + structure
4. [Plumb converter into the current pipeline][7] (Bagheera -> Kafka ->
   converter -> format.v2)
5. Build [MapReduce framework][6] to take [new format][1] + [structure][2] as
   input and output data as required by the [telemetry-frontend][3]
6. Build replacement frontend acquisition pipeline (HTTP -> persister -> format.v2)

Storage Format
-----------------
See [StorageFormat.md][1] for details.


On-disk Storage Structure
----------------------------
See [StorageLayout.md][2] for details.

Data Converter
-----------------
1. Use [RevisionCache](revision_cache.py) to load the correct Histograms.json for a given payload
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

`server.py`
-----------
Contains the prototype http server for receiving payloads. The `submit`
function is where the interesting things happen.

It accepts single submissions using the same type of URLs supported by
[Bagheera][7], and also has endpoints for batch submission (which improves
throughput for the production -> prototype relay).

`convert.py`
------------
Contains the `Converter` class, which is used to convert a JSON payload from
the raw form submitted by Firefox to the more compact [storage format][1] for
on-disk storage and processing.

You can run the main method in this file to process data exported from the
old telemetry backend (via pig, jydoop, etc), or you can use the `Converter`
class to convert data in a more fine-grained way.

`persist.py`
------------
Contains the `StorageLayout` class, which is used to save payloads to disk
using the directory structure as documented in the [storage layout][2] section
above.

`revision_cache.py`
-------------------
Contains the `RevisionCache` class, which provides a mechanism for fetching
the `Histograms.json` spec file for a given revision URL. Histogram data is
cached locally on disk and in-memory as revisions are requested.

`telemetry_schema.py`
---------------------
Contains the `TelemetrySchema` class, which encapsulates logic used by the
StorageLayout and MapReduce code.

`job.py`
--------
Contains the [MapReduce][6] code. This is the interface for running jobs on
Telemetry data. There are example job scripts and input filters in the
`examples/` directory.

`compressor.py`
---------------
Contains code to compress and rotate raw data files. Suitable for running from
`cron`.

[1]: StorageFormat.md "Storage Format"
[2]: StorageLayout.md "On-disk Storage Layout"
[3]: https://github.com/tarasglek/telemetry-frontend "Telemetry Frontend"
[4]: https://bugzilla.mozilla.org/show_bug.cgi?id=856263 "Bug 856263"
[5]: https://etherpad.mozilla.org/telemetry-reboot "Telemetry Reboot"
[6]: MapReduce.md "Telemetry MapReduce Framework"
[7]: BagheeraIntegration.md "Integration with Bagheera"
[8]: server.py "Telemetry Server"

