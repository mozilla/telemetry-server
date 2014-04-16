/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

var http = require('http');
var fs = require('fs');
var os = require('os');
var url = require('url');

var log_version = "v1";
var config = {};
if (process.argv.length > 2) {
  // Attempt to read server config from the first argument
  try {
    config = require(process.argv[2]);
  } catch(e) {
    // TODO: catch malformed JSON separately.
    console.log(e);
    config = {};
  }
} else {
  config.motd = "Telemetry Server (default configuration)";
}

var max_data_length = config.max_data_length || 200 * 1024;
var max_path_length = config.max_path_length || 10 * 1024;

// Even a full IPv6 address shouldn't be longer than this...
var max_ip_length = config.max_ip_length || 100;

// NOTE: This is for logging actual telemetry submissions
var log_path = config.log_path || "./";
var log_base = config.log_base || "telemetry.log";

// TODO: URL Validation to ensure we're receiving dimensions
// ^/submit/telemetry/id/reason/appName/appUpdateChannel/appVersion/appBuildID$
// See http://mxr.mozilla.org/mozilla-central/source/toolkit/components/telemetry/TelemetryPing.js#658
var url_prefix = config.url_prefix || "/submit/telemetry/";
var url_prefix_len = url_prefix.length;
var include_ip = false;
if (config.include_request_ip) {
  log_version = "v2";
  include_ip = true;
}

var max_log_size = config.max_log_size || 500 * 1024 * 1024;
var max_log_age_ms = config.max_log_age_ms || 5 * 60 * 1000; // 5 minutes in milliseconds

var log_file = unique_name(log_base);
var log_time = new Date().getTime();
var log_size = 0;

// We keep track of "last touched" and then rotate after current logs have
// been untouched for max_log_age_ms.
var timer = setInterval(function(){ rotate_time(); }, max_log_age_ms);

// NOTE: This is for logging request metadata (for monitoring and stats)
var request_log_file = config.stats_log_file || "/var/log/telemetry/telemetry-server.log";

function finish(code, request, response, msg, start_time, bytes_stored) {
  var duration = process.hrtime(start_time);
  var duration_ms = duration[0] * 1000 + duration[1] / 1000000;
  response.writeHead(code, {'Content-Type': 'text/plain'});
  response.end(msg);
  stat = {
    "url": request.url,
    "duration_ms": duration_ms,
    "code": code,
    "size": bytes_stored,
    "level": "info",
    "message": msg,
    "timestamp": new Date()
  };
  log_message = JSON.stringify(stat);
  // Don't want to do this synchronously, but it seems the most foolproof way.
  // The async version appears to leak FDs (resulting in EMFILE errors after a
  // while)
  // NOTE: if this is changed to use a persistent fd or stream, we need to
  //       listen for SIGHUP and close the log file so it can be rotated by
  //       logrotate
  fs.appendFileSync(request_log_file, log_message + "\n");
}

// Get the IP Address of the client. If we're receiving forwarded requests from
// a load balancer, use the appropriate header value instead.
function get_client_ip(request) {
  var client_ip = null;
  if (request.headers['x-forwarded-for']) {
    client_ip = request.headers['x-forwarded-for'];
  } else {
    client_ip = request.connection.remoteAddress;
  }
  return client_ip;
}

// We don't want to do this calculation within rotate() because it is also
// called when the log reaches the max size and we don't need to check both
// conditions (time and size) every time.
function rotate_time() {
  // Don't bother rotating empty log files (by time). Instead, assign a new
  // name so that the timestamp reflects the contained data.
  if (log_size == 0) {
    log_file = unique_name(log_base);
    return;
  }
  last_modified_age = new Date().getTime() - log_time;
  if (last_modified_age > max_log_age_ms) {
    console.log("Time to rotate " + log_file + " unmodified for " + last_modified_age + "ms");
    rotate();
  }
}

function rotate() {
  console.log(new Date().toISOString() + ": Rotating " + log_file + " after " + log_size + " bytes");
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
  return log_path + "/" + name + "." + log_version + "." + os.hostname() + "." + process.pid + "." + new Date().getTime();
}

function getRequest(request, response, process_time, callback) {
  if (request.method != 'GET') {
    return finish(405, request, response, "Wrong request type", process_time, 0);
  }
  if (request.url == '/status') {
    callback();
    return;
  }
  return finish(404, request, response, "Not Found", process_time, 0);
}

function postRequest(request, response, process_time, callback) {
  var request_time = new Date().getTime();
  var data_length = parseInt(request.headers["content-length"]);
  if (!data_length) {
    return finish(411, request, response, "Missing content length", process_time, 0);
  }
  if (data_length > max_data_length) {
    // Note, the standard way to deal with "request too large" is to return
    // a HTTP Status 413, but we do not want clients to re-send large requests.
    return finish(202, request, response, "Request too large (" + data_length + " bytes). Limit is " + max_data_length + " bytes. Server will discard submission.", process_time, 0);
  }
  if (request.method != 'POST') {
    return finish(405, request, response, "Wrong request type", process_time, 0);
  }

  // Parse the url to strip off any query params.
  var url_parts = url.parse(request.url);
  var url_path = url_parts.pathname;
  // Make sure that url_path starts with the expected prefix, then chop that
  // off before storing.
  if (url_path.slice(0, url_prefix_len) != url_prefix) {
    return finish(404, request, response, "Not Found", process_time, 0);
  } else {
    // Strip off the un-interesting part of the path.
    url_path = url_path.slice(url_prefix_len);
  }
  var path_length = Buffer.byteLength(url_path);
  if (path_length > max_path_length) {
    // Similar to the content-length above, we would normally return 414, but
    // we don't want clients to retry these either.
    return finish(202, request, response, "Path too long (" + path_length + " bytes). Limit is " + max_path_length + " bytes", process_time, 0);
  }

  var client_ip = null;
  var client_ip_length = 0;
  var preamble_length = 15; // 1 sep + 2 path + 4 data + 8 timestamp
  if (include_ip) {
    preamble_length += 1; // length of client_ip
    client_ip = get_client_ip(request);
    client_ip_length = Buffer.byteLength(client_ip);
    if (client_ip_length > max_ip_length) {
      console.log("Received an excessively long ip address: " + client_ip_length + " > " + max_ip_length);
      client_ip = "0.0.0.0";
      client_ip_length = Buffer.byteLength(client_ip);
    }
  }
  var buffer_length = client_ip_length + path_length + data_length + preamble_length;
  var buf = new Buffer(buffer_length);

  //console.log("Received " + data_length + " on " + url_path + " at " + request_time);

  // Write the preamble so we can read the pieces back out:
  // 1 byte record separator 0x1e (so we can find our spot if we encounter a corrupted record)
  // [v2 only] 1 byte uint to indicate client ip address length
  // 2 byte uint to indicate path length
  // 4 byte uint to indicate data length
  // 8 byte uint to indicate request timestamp (epoch) split into two 4-byte writes
  var buffer_location = 0;
  buf.writeUInt8(0x1e, buffer_location);                  buffer_location += 1;
  if (include_ip) {
    buf.writeUInt8(client_ip_length, buffer_location);    buffer_location += 1;
  }
  buf.writeUInt16LE(path_length, buffer_location);        buffer_location += 2;
  buf.writeUInt32LE(data_length, buffer_location);        buffer_location += 4;

  // Blast the lack of 64 bit int support :(
  // Standard bitwise operations treat numbers as 32-bit integers, so we have
  // to do it the ugly way.  Note that Javascript can represent exact ints
  // up to 2^53 so timestamps are safe for approximately a bazillion years.
  // This produces the equivalent of a single little-endian 64-bit value (and
  // can be read back out that way by other code).
  buf.writeUInt32LE(request_time % 0x100000000, buffer_location);
  buffer_location += 4;
  buf.writeUInt32LE(Math.floor(request_time / 0x100000000), buffer_location);
  buffer_location += 4;

  if (buffer_location != preamble_length) {
    // TODO: assert
    console.log("ERROR: We should have written " + preamble_length +
                "preamble bytes, but we actually wrote " + buffer_location);
  }

  if (include_ip) {
    // Write the client ip address if need be:
    buf.write(client_ip, buffer_location);  buffer_location += client_ip_length;
  }
  // Now write the path:
  buf.write(url_path, buffer_location);     buffer_location += path_length;

  // Write the data as it comes in:
  request.on('data', function(data) {
    data.copy(buf, buffer_location);
    buffer_location += data.length;
  });

  request.on('end', function() {
    // Write buffered data to file.
    // TODO: Keep a persistent fd/stream open and append, instead of opening
    //       and closing every time we write.
    fs.appendFile(log_file, buf, function (err) {
      if (err) {
        console.log("Error appending to log file: " + err);
        // Since we can't easily recover from a partially written record, we
        // start a new file in case of error.
        log_file = unique_name(log_base);
        log_size = 0;
        log_time = request_time;
        // TODO: can we find out how many bytes we actually wrote?
        return finish(500, request, response, err.message, process_time, buffer_length);
      }
      log_size += buf.length;
      log_time = request_time;
      // If length of outputfile is > max_log_size, start writing a new one.
      if (log_size > max_log_size) {
        rotate();
      }

      // All is well, call the callback
      callback(buffer_length);
    });
  });
}

function run_server(port) {
  http.createServer(function(request, response) {
    var start_time = process.hrtime();
    if (request.method == "POST") {
      postRequest(request, response, start_time, function(bytes_written) {
        finish(200, request, response, 'OK', start_time, bytes_written);
      });
    } else {
      getRequest(request, response, start_time, function() {
        finish(200, request, response, 'OK', start_time, 0);
      });
    }
  }).listen(port);
  console.log("Process " + process.pid + " Listening on port " + port);
}
var cluster = require('cluster');
var numCPUs = os.cpus().length;

if (cluster.isMaster) {
  // Fork workers.
  for (var i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  if (config.motd) {
    console.log(config.motd);
  }

  cluster.on('exit', function(worker, code, signal) {
    console.log('Worker ' + worker.process.pid + ' died. Starting a new worker.');
    // Start another one:
    cluster.fork();
    // TODO: See how long the child actually stayed alive. We don't want to
    //       fork continuously, so if the child processes are dying right away
    //       we should abort the master (and have the server respawned
    //       externally).
  });
} else {
  // Finalize current log files on exit.
  process.on('exit', function() {
    console.log("Received exit message in pid " + process.pid);
    if (log_size != 0) {
      console.log("Finalizing log file:" + log_file);
      rotate();
    } else {
      console.log("No need to clean up empty log file.")
    }
  });

  // Catch signals that break the main loop. Since they don't exit directly,
  // on('exit') will also be called.
  process.on('SIGTERM', function() {
    console.log("Received SIGTERM in pid " + process.pid);
  });
  process.on('SIGINT', function() {
    console.log("Received SIGINT in pid " + process.pid);
  });

  run_server(config.port || 8080);
}
