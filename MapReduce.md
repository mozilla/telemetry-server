Telemetry MapReduce
====================

We will provide a basic [MapReduce][1] framework to process Telemetry Data.

Given the storage layout, it should be easy to parallelize the processing of
the data between all available CPU cores within a machine, but also across 
multiple machines if need be.

There will be a way to leverage the [storage layout][2] to quickly and easily
filter for the data set you want to work with.  The most obvious way is to
allow jobs to specify the desired dimensions using a version of the
`telemetry_schema.json` file.

[1]: http://en.wikipedia.org/wiki/MapReduce "MapReduce"
[2]: StorageLayout.md "On-disk Storage Layout"
