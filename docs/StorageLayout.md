Telemetry Storage Layout
========================

The basic idea is to partition the data by a number of useful dimensions, then
use the dimensions to form a directory hierarchy. Finally, the actual
submissions will be stored in compressed files that may be read and processed
in parallel. The files in a directory will be split into manageable sized
pieces. Each line in the file will be of the form `<uuid><tab><json>`.  See
[StorageFormat](StorageFormat.md) for more details about the contents of the 
files.

The main thing to define here is exactly which dimensions will be used for
partitioning, and in which order to apply them.

If we used submission day, channel, and operating system, we would end up with
a structure like this:
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

In order for each individual file not to get too small (and thus make
compression less effective), we will want to bucket certain rarely-occuring
dimensions into an "other" directory.  This way we do not need to maintain a
mostly-empty directory tree for customized channels, for example.

The way we accomplish this is to maintain a predefined schema of which values
are acceptable in a dimension, with anything outside the whitelist being
grouped into "other".


Schema-based Storage
--------------------

We specify the schema as a list of acceptable values for each dimension, and
any value will be replaced with "OTHER".  The code can then create any
directories on demand.

This has the advantage that the schema is defined explicitly, and is easily
shared in a multi-server scenario.

One disadvantage is that you would have to signal the partitioner of any change
to the schema so that documents could be re-routed with the updated schema.

This is the approach that will be used.

### `telemetry_schema.json`

The schema is defined in [telemetry_schema.json](../telemetry/telemetry_schema.json) and
contains an array of `dimensions` that are used to determine what is allowed
at each level of the storage hierarchy.  Currently supported values are:
- String value `*`: allow any value
- Array of strings: allow any value in the array
- Min / max range: allow values in a range (or specify only an upper or lower
  bound)

Types that may be supported in the future (if and when they are needed):
- Regular expression: allow only values matching the specified regex

Values outside of the allowed values will be replaced with "OTHER" to make sure
that the "long tail" of dimension values does not cause a huge number of small
files to be created.

Code for handling a schema is found in the `TelemetrySchema` class
in [telemetry_schema.py](../telemetry/telemetry_schema.py)

Considered, but unused approaches
---------------------------------

### Filesystem as Schema
One can use the filesystem itself as the schema, whereby if an
expected directory does not exist, a document is automatically put in the 
"other" category for that level.

This has the advantage that you can update the schema in real time, simply by
creating new directories in the filesystem.

One disadvantage is that you would have to reprocess everything in the "other"
category to redistribute documents into newly created directories.  It would
also require the ongoing creation of submission day directories (though those
could be created say 5 days ahead).

Another disadvantage is that the schema is not defined explicitly, so it could
become inconsistent across servers or days.

### Size-based partitions
Another way is not to use a schema, but to "roll up" small partitions based on
the number/size of documents.

Rather than using a schema, one could have a batch process to go through the
data for the previous day, and combine any files that contained less than a
certain amount of data.

This has the advantage that it does not require manual intervention to maintain
reasonably well-balanced splitting of data files.

A disadvantage is that during the current day, the "long tail" of infrequently
appearing dimension values could result in a huge number of files and
directories being created.


