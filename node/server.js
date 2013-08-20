var http = require('http');
var fs = require('fs');
var max_data_length = 200 * 1024;
var max_path_length = 10 * 1024;
var log_path = "./";
var log_base = "telemetry.log";
if (process.argv.length > 2) {
  log_path = process.argv[2];
}

var log_file = unique_name(log_base);
var log_size = 0;
console.log("Using log file: " + log_file);

var max_log_size = 500 * 1024 * 1024;
var max_log_age_ms = 60 * 60 * 1000; // 1 hour in milliseconds
//var max_log_age_ms = 60 * 1000; // 1 minute in milliseconds

// TODO: keep track of "last touched" and don't rotate
//       until they've been untouched for max_log_age_ms.
// Rotate any in-progress logs occasionally.
var timer = setInterval(function(){ rotate(); }, max_log_age_ms);

function finish(code, request, response, msg) {
  response.writeHead(code, {'Content-Type': 'text/plain'});
  response.end(msg);
}

function rotate() {
  // Don't bother rotating empty log files (presumably by time).
  if (log_size == 0) {
    console.log("not rotating empty log");
    return;
  }
  console.log("rotating " + log_file + " after " + log_size + " bytes");
  fs.rename(log_file, log_file + ".finished", function (err) {
    if (err) {
      console.log("Error rotating " + log_file + " (" + log_size + "): " + err);
    }
  });

  // Start a new file whether the rename succeeded or not.
  log_file = unique_name(log_base);
  log_size = 0;
}

function unique_name(name) {
  // Could use UUID or something, but pid + timestamp should suffice.
  return log_path + "/" + name + "." + process.pid + "." + new Date().getTime();
}

function postRequest(request, response, callback) {
  var data_length = parseInt(request.headers["content-length"]);
  if (!data_length) {
    return finish(411, request, response, "Missing content length");
  }
  if (data_length > max_data_length) {
    return finish(413, request, response, "Request too large (" + data_length + " bytes). Limit is " + max_data_length + " bytes");
    // TODO: return 202 Accepted instead (so that clients don't retry)?
  }
  if (request.method != 'POST') {
    return finish(405, request, response, "Wrong request type");
  }
  if (request.url.length > max_path_length) {
    // TODO: stop at the "?" part of the url?
    return finish(413, request, response, "Path too long (" + request.url.length + " bytes). Limit is " + max_path_length + " bytes");
  }
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
  var pos = data_offset + path_length;

  // Write the data as it comes in
  request.on('data', function(data) {
    data.copy(buf, pos);
    pos += data.length;
  });

  request.on('end', function() {
    // Write buffered data to file.
    fs.appendFile(log_file, buf, function (err) {
      if (err) {
        console.log("Error appending to log file: " + err);
        // TODO: what about log_size?
        return finish(500, request, response, err);
      }
      log_size += buf.length;
      // If length of outputfile is > max_log_size, start writing a new one.
      if (log_size > max_log_size) {
        rotate();
      }

      // All is well, call the callback
      callback();
    });
  });
}

function run_server(port) {
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
  // TODO: make this work so we can finalize our log files on exit.
  /*
  process.on('exit', function() {
    console.log("Received exit message in pid " + process.pid);
    // TODO: rename log to log.finished
  });
  process.on('SIGTERM', function() {
    console.log("Received SIGTERM in pid " + process.pid);
    // TODO: rename log to log.finished
  });
  */
  run_server(8080);
}
