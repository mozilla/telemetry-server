Architecture for Processing Incoming Data
=========================================

Let `N` denote the number of CPU cores available on the processing machine.

The `Server` Process:
---------------------
*  Create following folders on same storage device
    * `downloading/`  for files while being downloaded,
    * `upload/`       for files ready for upload
    * `uploading/`    for files being uploaded
    * `incoming/`     for incoming files that have been downloaded
*  Start `N / 2` instances of the `Download` process
*  Start `N / 2` instances of the `Upload` process
*  For `i = 0` to `N` do:
    * Create folders `work-i/`, `input-i/`, `log-i/`
    * Start `Worker` process (given it a reference to `i`)

The `Download` Process:
-----------------------
* While number of files in the `incoming` folder is less than `N`.
    * Download a new raw telemetry log file to `downloading/`
    * Move downloaded file from `downloading/` to `incoming/`

The `Upload` Process:
---------------------
* While the `upload/` contains files:
    * Move a file from `upload/` to `uploading/`
    * Upload file to S3
    * Delete from file from `uploading/`

The `Worker` Process i:
-----------------------
* While `incoming` is non-empty:
    * Move file from `incoming/` to `input-i/`
    * For each line in file:
        * Parse line giving us path and histogram
        * if parse error
            * Write to somewhere in `log-i/`
            * Skip line
        * Convert histogram
        * RecordWriter.write(path, historgram.serilize())
    * Delete input file
    * If SIGHUP has been seen:
        * close all files and compressor context in HashTable
        * Compress files and move them to `upload/`
* On SIGHUP: Raise a boolean flag.


The idea with WorkerProcess:
----------------------------
* We can stop it at anytime and upload (by sending it a SIGHUP)
* We can keep it running and feed it data until it produces big files (worst
  case, one file per day for a given set of partitions)
* We can tweak number of compression contexts, reducing intermediate disk I/O
  in exchange for increased memory usage
* If we crash, uncompressed files from `work-i/` can be compressed and uploaded
* If we crash, offending `incoming` file is located in `input-i/` this can be
  uploaded for tests (not for reapplication if we do previous thing)
* Both conversion and compression happens in WorkerProcess, so we can't fill up
  a pipe somewhere and have IPC problems
* Problem with conversion and compression in same process if that if conversion
  crashes, partially compressed data is corrupt
