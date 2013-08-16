TODO
====

- [P1] Error handling in pipe-based compressor
- [P1] Add a server unique identifier to the log file names so that we can run
       many server nodes, or possibly just add a UUID to the filename instead
       of .3.log, .4.log, etc.
- [P2] nginx: Check into load-balancing
- [P2] nginx: Accept gzip-encoded submissions
- [P2] Figure out idle-daily de-duplication
- [P2] Supply the correct Histograms.json spec for each record to the Mapper
- [P2] MapReduce: make sure to create all the mapper_x_y files (since we might
       end up with a file whose keys don't hash to all possible reducer
       buckets).
- [P2] MapReduce: delete downloaded data files after they have been processed.
- [P2] Improve speed of the conversion process
- [P2] Define data access policy
  -  read access?
  -  retention period
  -  investigate [S3's Lifecycle policy][7] (send to glacier, eventually
     expire)
- [P3] Have the "archive" job write bad input records to the invalid_data dir
- [P3] Create AMI images for bootstrapped server and mapreduce nodes
- [P3] Check if the compressor (and exporter) cron job is already running, and
       if so don't start another instance.
- [P3] Stream data from S3 for MapReduce instead of downloading first
- [P3] Investigate using Amazon ElasticMapReduce for MR jobs (instead of
       fetching and running locally)
- [P3] Add timeout/retry around fetching Histograms.json from hg.mozilla.org
- [P3] Add many tests
- [P3] Add runtime performance metrics using [scales][1] and on-demand perf
       tests using [cProfile][3]
- [P3] Add proper [logging][2]
- [P3] Ensure things are in order to accept Addon Histograms, such as
       from [pdf.js][5]
- [P4] Make sure we handle fetching Histograms.json from the server for feature
       branches (cedar/larch/fig, etc):
         http://hg.mozilla.org/releases/cedar/raw-file/9a6b98023a90/toolkit/components/telemetry/Histograms.json
       is wrong.
- [P4] Change the RevisionCache to fetch the entire history of Histograms.json
       and then convert incoming revisions to times to find the right version
- [P4] Preopen the mapper input files in the parent process, pass fd's to child
       process to avoid race condition with the compressor.

[1]: https://github.com/Cue/scales "Scales"
[2]: http://docs.python.org/2/library/logging.html "Python Logging"
[3]: http://docs.python.org/2/library/profile.html "Python Profilers"
[5]: https://github.com/mozilla/pdf.js/pull/3532/files#L1R29
[7]: http://docs.aws.amazon.com/AmazonS3/latest/dev/object-lifecycle-mgmt.html
