Telemetry MapReduce
====================

We provide a basic [MapReduce][1] framework to process Telemetry Data.

There is a full example of how to run a Telemetry MapReduce job on [mreid's blog][6].

The base code is in [job.py][4], and there are a few examples of job
scripts (and filters) in the [examples/][3] directory.

Given the storage layout, it is easy to parallelize the processing of data
between all available CPU cores within a machine.

Overview
========

A basic MapReduce job implements two methods: `map` and `reduce`.

The `map` function
------------------

An extremely simple `map` function that just counts records looks like:
```python
def map(key, dimensions, value, map_context):
    map_context.write("record_count", 1)
```
The arguments to the `map` function are:
- `key` - The Document ID of a single telemetry document.
- `dimensions` - An array of strings corresponding to the filterable dimensions of the document.
- `value` - The String value of the telemetry document. The data is in JSON form, but is not parsed into an object.
- `map_context` - The Mapper's context is used to write output for use by the next stage of MapReduce. You may write out any Python key and value here, as long as it can be serialized by the [marshal][8] module and may be used as a map key.

The `reduce` function
---------------------

An example `reduce` function that completes the "record count" example above:
```python
def reduce(key, values, reduce_context):
    reduce_context.write(key, sum(values))
```

The arguments to the `reduce` function are:
- `key` - The first argument passed to `map_context.write`
- `values` - An array of all the outputs from the map phase with the given key
- `reduce_context` - The Reducer's context, used to write the final data output.

By default, the Reducer context output will be a line containing `str(key)` and `str(value)` separated by a tab character. You may modify the context's `field_separator` and `record_separator` member fields to format output differently. Notably, set `field_separator` to `,` to output csv. Set these values in the `setup_reduce` function as described below.

Example data flow
-----------------

If we imagine that our input contained three records, the logical flow would look like:
```python
map(key1, dims1, val1, map_context) => map_context.write("record_count", 1)
map(key2, dims2, val2, map_context) => map_context.write("record_count", 1)
map(key3, dims3, val3, map_context) => map_context.write("record_count", 1)

reduce("record_count", [1, 1, 1], reduce_context) => reduce_context.write("record_count", 3)
```

So the final output would be `record_count  3`

Variations on the standard MapReduce job
========================================

Map-only jobs
-------------

The `reduce` function is optional, so if you simply want to output a filtered subset of
the data, you can write out the desired data from the `map` function, and simply omit the
`reduce` function altogether. Each record written by `map_context.write(...)` will be
written out to the final output file as-is.

The `combine` function
----------------------

The `combine` function has the same signature as the `reduce` function, and allows you to do a "partial reduce". It will be called every time the number of records for a given key exceeds a certain threshold (currently 50 records).

If your reduce logic can be performed incrementally, this is a good way to lower the memory requirements of the reduce phase.

In the "record count" example above, the reduce logic can safely be done incrementally, so you could add `combine = reduce` in your mapreduce job script.

If the reduce can be done incrementally, but not with the exact same code as the final `reduce` pass, you can implement arbitrary logic:
```python
def combine(key, values, reduce_context):
    # My fancy code here
    # ... logic to reduce "values" to a single item ...
    reduce_context.write(key, my_single_value)
```

Your `combine` function should write out a single value to replace all the values for the key.

Sometimes it's not feasible to do the reduce in pieces, so in that case, omit the `combine` function.

Setting up the reduce context
-----------------------------

There is an optional `setup_reduce(reduce_context)` function that, if implemented, allows
you do do any initial configuration of the reduce context. It will be called before the
first invocation of the `reduce` function.

This is where you would override the context's defaults for `field_separator` and
`record_separator`. For example, if you wanted to output CSV instead of tab-separated
values:
```python
def setup_reduce(context):
    context.field_separator = ','
```

Filtering data
==============

You can specify a `filter` that leverages the [storage layout][2] to quickly and
easily limit the data set you want to work with.  It uses the same type of
[Telemetry Schema](../telemetry_schema.py) document to determine which files are
included. There are [examples of filters][5] in the [examples/][3] dir as well.

Running the job
===============

Once you've got your mapreduce script written and your input filter ready, you can run the job as follows:
```bash
$ cd ~/telemetry-server
$ python -m mapreduce.job mapreduce/examples/my_record_counter.py \
   --input-filter /path/to/filter.json \
   --num-mappers 8 \
   --num-reducers 1 \
   --data-dir /mnt/telemetry/work/cache \
   --work-dir /mnt/telemetry/work \
   --output /mnt/telemetry/my_mapreduce_results.out \
   --bucket "telemetry-published-v2"
```

If you have AWS credentials, you can run this from your local machine. Otherwise you should run it using the [telemetry analysis service][7] as described in [this blog post][6].

The first time you run the job, it will download all data matching the filter you've specified. As such, it's important to make sure your filter is reasonably restrictive otherwise your job will take a really long time, and you run the risk of running out of local hard drive space.

Debugging: Local-only jobs
--------------------------

After you've run the job once, all the data is available locally, so while you're debugging you can save a lot of time by not re-downloading from S3 every time.

If your input filter is not changing, you can run the analysis job in "local-only" mode, which skips the download and uses the locally cached files instead. In fact, you can still adjust your input filter as long as you make it *more* restrictive. If you were to make it *less* restrictive, you would be missing some files and your results would be inaccurate.

The local data cache is located at `<work-dir>/cache`, and you specify where to look for local data using the `--data-dir` parameter. To run a local version of the job above:
```bash
$ python -m mapreduce.job mapreduce/examples/my_record_counter.py \
   --input-filter /path/to/filter.json \
   --num-mappers 8 \
   --num-reducers 1 \
   --data-dir /mnt/telemetry/work/cache \
   --work-dir /mnt/telemetry/work \
   --output /mnt/telemetry/my_mapreduce_results.out \
   --local-only
```

Debugging: Out of memory
------------------------

Since this MapReduce framework currently runs on a single machine, it is limited by the local resources. The most common problem is running out of memory, typically during the Reduce phase.

The easiest way to work around OOM problems is to use a more selective filter to reduce the amount of input data.

If that is not practical, the next best way is to implement the `combine` function.


Scheduling Telemetry Analysis Jobs
==================================

Coming soon.

[1]: http://en.wikipedia.org/wiki/MapReduce "MapReduce"
[2]: StorageLayout.md "On-disk Storage Layout"
[3]: ../mapreduce/examples/ "MapReduce examples"
[4]: ../mapreduce/job.py "MapReduce code"
[5]: ../mapreduce/examples/filter_saved_session_Fx_prerelease.json "Example Filter"
[6]: http://mreid-moz.github.io/blog/2013/11/06/current-state-of-telemetry-analysis/
[7]: http://telemetry-dash.mozilla.org/
[8]: http://docs.python.org/2/library/marshal.html#module-marshal "Python marshal module"
