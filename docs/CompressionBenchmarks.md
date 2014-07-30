Compression Benchmarks
======================

One of the important considerations here is what compression format
to use, and within that format, what level of compression to use.

After a round of testing that is largely lost to the sands of time,
we settled on the LZMA / XZ format.

Some statistics on the time vs. space characteristics of various
compression levels can be found at [compression notes][1].

To run a real-world test, you can use some code like the following:

    aws s3 cp s3://telemetry-published-bucket/path/to/sample_file.lzma ./
    lzma -d sample_file.lzma
    for level in $(seq 0 9); do
      echo "compressing with level $level"
      time cat sample_file | lzma -${level} > test$level.lzma
      ls -l test$level.lzma
    done &>> comptest.log

Using a ~500MB raw input file on a `c3.large` EC2 node, this gives
a result like:


     Level     Time       Size      Filename
    -------  ---------  ---------  ----------
    level 0  0m26.176s  105830359  test0.lzma
    level 1  0m28.231s   89387336  test1.lzma
    level 2  0m37.868s   81364589  test2.lzma
    level 3  0m52.852s   76801476  test3.lzma
    level 4  1m40.807s   73784033  test4.lzma
    level 5  2m36.868s   65191241  test5.lzma
    level 6  3m39.400s   61367748  test6.lzma
    level 7   4m1.284s   60218864  test7.lzma
    level 8  4m19.748s   59183316  test8.lzma
    level 9  4m47.116s   58338421  test9.lzma


Using `xz` instead of `lzma` gives nearly identical numbers, but `xz` is to be
preferred since those files can be concatenated without having to decompress
and compress again.

[1]: https://docs.google.com/spreadsheet/pub?key=0AoRU282jPz57dFBuX0pZX25NNVRlU3lQTDZUVzlEUEE&output=html
