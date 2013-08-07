TODO
====

- [P4] Preopen the mapper input files in the parent process, pass fd's to child process to avoid race condition with the compressor.
- [P1] Error handling in pipe-based compressor
- [P2] nginx: Check into load-balancing
- [P2] nginx: Accept gzip-encoded submissions
- [P2] Figure out idle-daily de-duplication
- [P2] Supply the correct Histograms.json spec for each record to the Mapper
- [P2] MapReduce: make sure to create all the mapper_x_y files (since we might end up with a file whose keys don't
       hash to all possible reducer buckets).
- [P3] Check if the compressor (and exporter) cron job is already running, and if so don't start another instance.
- [P3] Add timeout/retry around fetching Histograms.json from hg.mozilla.org
- [P3] Add many tests
- [P3] Add runtime performance metrics using [scales][1] and on-demand perf tests
  using [cProfile][3]
- [P3] Add stats for throughput to/from S3
- [P3] Add proper [logging][2]
- [P3] Ensure things are in order to accept Addon Histograms, ie from [pdf.js][5]
- [P2] Improve speed of the conversion process
- [P4] Change the RevisionCache to fetch the entire history of Histograms.json and
  then convert incoming revisions to times to find the right version
- [P2] Define data access policy
  -  read access?
  -  retention period

[1]: https://github.com/Cue/scales "Scales"
[2]: http://docs.python.org/2/library/logging.html "Python Logging"
[3]: http://docs.python.org/2/library/profile.html "Python Profilers"
[4]: http://boto.s3.amazonaws.com/s3_tut.html "Using S3 with boto"
[5]: https://github.com/mozilla/pdf.js/pull/3532/files#L1R29
[6]: http://stackoverflow.com/questions/7561663/appending-to-the-end-of-a-file-in-a-concurrent-environment
