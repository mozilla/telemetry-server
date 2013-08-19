var http = require('http');
var fs = require('fs');
var max_data_length = 200 * 1024;
var max_path_length = 10 * 1024;
var log_file = "log.txt";
if (process.argv.length > 2) {
  log_file = process.argv[2];
}
console.log("Using log file: " + log_file);

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

        // If length of outputfile is > max_log_size, rename it to something unique
        // TODO: this should be part of the append() logic - if f.tell() > max, rotate immediately.
        try {
          stats = fs.statSync(log_file);
          if (stats.size > max_log_size) {
            console.log("rotating log file after " + stats.size + " bytes " + process.pid);
            fs.renameSync(log_file, unique_name(log_file));
          }
        } catch (err) {
          console.log("failed to rotate log file - someone else probably did it already");
        }

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
