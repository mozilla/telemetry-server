TODO
====

- [P2] Add "number of records" to exported filenames
- [P2] Figure out idle-daily de-duplication
- [P2] Supply the correct Histograms.json spec for each record to the Mapper
- [P2] MapReduce: delete downloaded data files after they have been processed.
- [P2] Improve speed of the conversion process
- [P3] Have the "process_incoming" job write bad input records back to S3
- [P3] Stream data from S3 for MapReduce instead of downloading first
- [P3] Add timeout/retry around fetching Histograms.json from hg.mozilla.org
- [P3] Add many tests
- [P3] Add runtime performance metrics
- [P3] Ensure things are in order to accept Addon Histograms, such as
       from [pdf.js][5]
- [P4] Change the RevisionCache to fetch the entire history of Histograms.json
       and then convert incoming revisions to times to find the right version

[1]: https://github.com/Cue/scales "Scales"
[2]: http://docs.python.org/2/library/logging.html "Python Logging"
[3]: http://docs.python.org/2/library/profile.html "Python Profilers"
[5]: https://github.com/mozilla/pdf.js/pull/3532/files#L1R29
[7]: http://docs.aws.amazon.com/AmazonS3/latest/dev/object-lifecycle-mgmt.html
