var http = require('http');
var fs = require('fs');
var max_data_length = 200 * 1024;
var max_path_length = 10 * 1024;
var log_file = 'log.txt';
var max_log_size = 500000000;

function finish(code, request, response, msg) {
  response.writeHead(code, {'Content-Type': 'text/plain'});
  response.end(msg);
}

function unique_name(name) {
  // Could use UUID or something, but pid + timestamp should suffice.
  return name + "." + process.pid + "." + new Date().getTime();
}

function postRequest(request, response, callback) {
  var data_length = parseInt(request.headers["content-length"]);
  if (!data_length) {
    finish(411, request, response, "Missing content length");
  } else if (data_length > max_data_length) {
    finish(413, request, response, "Request too large (" + data_length + " bytes). Limit is " + max_data_length + " bytes");
    // TODO: return 202 Accepted instead (so that clients don't retry)?
  } else if (request.method != 'POST') {
    finish(405, request, response, "Wrong request type");
  } else if (request.url.length > max_path_length) {
    // TODO: stop at the "?" part of the url?
    finish(413, request, response, "Path too long (" + request.url.length + " bytes). Limit is " + max_path_length + " bytes");
  } else {
    var chunks = [];
    request.on('data', function(data) {
      chunks.push(data);
    });

    request.on('end', function() {
      var data_offset = 2 * 4;
      var path_length = request.url.length;
      var buffer_length = path_length + data_length + data_offset;
      var buf = new Buffer(buffer_length);

      // Write the preamble so we can read the pieces back out:
      // 4 bytes to indicate path length
      // 4 bytes to indicate data length
      buf.writeUInt32LE(path_length, 0);
      buf.writeUInt32LE(data_length, 4);

      // now write the path:
      buf.write(request.url, data_offset);

      // now write all the data:
      numchunks = chunks.length;
      pos = data_offset + path_length;

      for (var i = 0; i < numchunks; i++) {
        //console.log("writing chunk " + i + " (" + chunks[i].length + " bytes)");
        chunks[i].copy(buf, pos);
        pos += chunks[i].length;
      }

      fs.appendFile(log_file, buf, function (err) {
        if (err) {
          finish(500, request, response, err);
          throw err;
        }
        console.log("pathlen: " + request.url.length + "=" + request.url + ", datalen: " + data_length);
        // If length of outputfile is > max_log_size, rename it to something unique
        fs.stat(log_file, function(err, stats) {
          if (err) {
            console.log("error stat'ing log file :(");
          } else {
            if (stats.size > max_log_size) {
              console.log("rotating log file after " + stats.size + " bytes");
              fs.rename(log_file, unique_name(log_file));
            }
          }
        });

        // All is well, call the callback
        callback();
      });
    });
  }
}

function run_server(port) {
  // Workers can share any TCP connection
  // In this case its a HTTP server
  http.createServer(function(request, response) {
    postRequest(request, response, function() {
      finish(200, request, response, 'OK');
    });
  }).listen(port);
  console.log("Listening on port "+port);
}
var cluster = require('cluster');
var numCPUs = require('os').cpus().length;

if (cluster.isMaster) {
  // Fork workers.
  for (var i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  cluster.on('exit', function(worker, code, signal) {
    console.log('worker ' + worker.process.pid + ' died');
  });
} else {
  run_server(8080);
}

/*windows dualcore laptop benching quadcore server:
$ ./ab -c600  -k -t 5 -n 900000 -p 30K  http://localhost:8080/                                                                                                                                                                                                                        This is ApacheBench, Version 2.3 <$Revision: 1430300 $>
Copyright 1996 Adam Twiss, Zeus Technology Ltd, http://www.zeustech.net/
Licensed to The Apache Software Foundation, http://www.apache.org/

Benchmarking quad. (be patient)


Server Software:
Server Hostname:        quad.
Server Port:            8080

Document Path:          /
Document Length:        2 bytes

Concurrency Level:      600
Time taken for tests:   5.140 seconds
Complete requests:      5988
Failed requests:        0
Write errors:           0
Keep-Alive requests:    0
Total transferred:      617794 bytes
Total body sent:        198641376
HTML transferred:       11996 bytes
Requests per second:    1164.88 [#/sec] (mean)
Time per request:       515.076 [ms] (mean)
Time per request:       0.858 [ms] (mean, across all concurrent requests)
Transfer rate:          117.37 [Kbytes/sec] received
                        37737.03 kb/s sent
                        37854.39 kb/s total

Connection Times (ms)
              min  mean[+/-sd] median   max
Connect:        0    1   0.6      1      17
Processing:   370  473  68.4    467     660
Waiting:        6  238 137.0    236     638
Total:        370  473  68.4    467     661

Percentage of the requests served within a certain time (ms)
  50%    467
  66%    476
  75%    489
  80%    497
  90%    587
  95%    640
  98%    653
  99%    658
 100%    661 (longest request)
Finished 5988 requests

quadcore server benching node..EG WINDOWS being faster at being server on slower hw...wtf
taras@quad:~/tmp$ ab -c600  -k -t 10 -n 900000 -p 30K  http://xps13.:8080/
This is ApacheBench, Version 2.3 <$Revision: 1430300 $>
Copyright 1996 Adam Twiss, Zeus Technology Ltd, http://www.zeustech.net/
Licensed to The Apache Software Foundation, http://www.apache.org/

Benchmarking xps13. (be patient)
Finished 12149 requests


Server Software:
Server Hostname:        xps13.
Server Port:            8080

Document Path:          /
Document Length:        2 bytes

Concurrency Level:      600
Time taken for tests:   10.345 seconds
Complete requests:      12149
Failed requests:        0
Write errors:           0
Keep-Alive requests:    0
Total transferred:      1254849 bytes
Total body sent:        384330138
HTML transferred:       24366 bytes
Requests per second:    1174.33 [#/sec] (mean)
Time per request:       510.931 [ms] (mean)
Time per request:       0.852 [ms] (mean, across all concurrent requests)
Transfer rate:          118.45 [Kbytes/sec] received
                        36278.82 kb/s sent
                        36397.27 kb/s total

Connection Times (ms)
              min  mean[+/-sd] median   max
Connect:        9  256 117.3    255     485
Processing:    12  231 120.0    230     503
Waiting:        6  223 119.3    222     493
Total:        339  487  78.3    482     949

Percentage of the requests served within a certain time (ms)
  50%    482
  66%    487
  75%    488
  80%    493
  90%    501
  95%    505
  98%    835
  99%    873
 100%    949 (longest request)
*/
