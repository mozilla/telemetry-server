Telemetry MapReduce
====================

We provide a basic [MapReduce][1] framework to process Telemetry Data.

The base code is in [job.py](../job.py), and there are a few examples of job
scripts (and filters) in the [examples/](../examples) directory.

Given the storage layout, it is easy to parallelize the processing of data
between all available CPU cores within a machine, but also across multiple
machines if need be.

Each record passed to the `map` function also includes that record's dimensions
which avoids having to parse the json string for simple count-type tasks.

You can specify a `filter` that leverages the [storage layout][2] to quickly and
easily limit the data set you want to work with.  It uses the same type of
[Telemetry Schema](../telemetry_schema.py) document to determine which files are
included. There are examples of filters in the [examples/](../examples) dir as well.

[1]: http://en.wikipedia.org/wiki/MapReduce "MapReduce"
[2]: StorageLayout.md "On-disk Storage Layout"
