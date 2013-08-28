var net = require('net');
var http = require('http');

var agent = new http.Agent();
agent.maxSockets = 50000;

var options = {
  hostname: "localhost",
  port: 8080,
  method: "POST",
  agent: agent
};

var DEBUG = false;

var listen_port = 9090;
if (process.argv.length > 2) {
  listen_port = parseInt(process.argv[2]);
}

var p_stats = null;

var stats = {
  sent: 0,
  completed: 0,
  errors: 0,
  bytes_sent: 0,
  responses: {
  }
};

var timer = setInterval(function() {
  console.log("Stats for port " + listen_port + " at " + new Date());
  stats.ts = new Date().getTime();
  console.log("  requests sent:      " + stats.sent);
  console.log("  requests completed: " + stats.completed);
  console.log("  bytes sent:         " + stats.bytes_sent);
  if (p_stats != null) {
    duration = (stats.ts - p_stats.ts) / 1000;
    //console.log("  duration:           " + duration);
    b_sent = stats.bytes_sent - p_stats.bytes_sent;
    rate = (b_sent / 1024.0 / 1024.0) / duration;
    console.log("  data rate:          " + rate.toFixed(2) + "MB/s");
    console.log("  req comp rate:      " + ((stats.completed - p_stats.completed) / duration).toFixed(2) + "r/s");
    console.log("  req send rate:      " + ((stats.sent - p_stats.sent) / duration).toFixed(2) + "r/s");
  }
  console.log("  errors:             " + stats.errors);
  console.log("  response codes:");
  var keys = Object.keys(stats.responses);
  keys.sort();
  for (var i = 0; i < keys.length; i++) {
    console.log("    " + keys[i] + ": " + stats.responses[keys[i]]);
  }
  p_stats = JSON.parse(JSON.stringify(stats));
}, 10000);

function debug(message) {
  if(DEBUG) {
    console.log(message);
  }
}

// process the data once it's reached end of line
function processData(curr_req) {
  options.path = curr_req.path;
  if (curr_req.path.slice(0, 18) != "/submit/telemetry/") {
    options.path = "/submit/telemetry/" + curr_req.path;
  }
  if (curr_req.data_len != curr_req.data.length) {
    console.log("SEVERE: actual data length (" + curr_req.data.length + ") does not match expected length (" + curr_req.data_len + ")");
  }
  options.headers = {
    'Content-Length': curr_req.data.length,
    'Connection': 'keep-alive'
  };
  //debug("Path: " + curr_req.path);

  var req_start = new Date().getTime();
  var req = http.request(options, function(res) {
    var req_end = new Date().getTime();
    var req_duration = req_end - req_start;
    debug("req finished: " + res.statusCode + " after " + req_duration + "ms (started " + req_start + ", fin: " + req_end + ")");
    stats.responses[res.statusCode] = (stats.responses[res.statusCode] || 0) + 1;
    stats.completed++;
    stats.bytes_sent += curr_req.data.length;
    res.on('data', function(chunk) {
      // discard.
    });
  });

  req.on('error', function(e) {
    console.log("Path " + curr_req.path + " errored: " + e.message);
    stats.errors++;
  });

  req.end(curr_req.data);
  stats.sent++;
}

// in v0.10 this data comes from the SlabAllocator, and we don't want
// to hold onto a large chunk of memory, so we're going to copy it out
function dupChunk(buf, start, end) {
  if (end - start < 0)
    throw new RangeError('end - start < 0');

  var new_buf = new Buffer(end - start);
  buf.copy(new_buf, 0, start, end);
  return new_buf;
}

function blank_request() {
  return {
    path_len: -1,
    data_len: -1,
    timestamp: -1,
    path: null,
    data: null
  };
}
var partials = [];
var data_read = 0;
var current_request = blank_request();

var server = net.createServer(function (socket) {
  console.log('Client connected');

  socket.on('data', function(data) {
    debug("data");
    debug("read " + data.length + " bytes");
    data_read += data.length;

    partials.push(data);
    if (partials.length == 1) {
      data = partials[0];
    } else {
      data = Buffer.concat(partials);
    }
    partials = [];

    var pending = stats.sent - stats.completed;
    if (pending > 500) {
      socket.pause();
      console.log("Too many pending requests (" + pending + "). pausing");
      setTimeout(function(){
        console.log("Resuming socket after a nice rest.");
        socket.resume();
      }, 1000);
    }
    while (data.length > 0) {
      if (current_request.path_len < 0) {
        if (data.length >= 4) {
          current_request.path_len = data.readUInt32LE(0);
          debug("Got path length: " + current_request.path_len);
          if (current_request.path_len > 1024) {
            debug("Warning: path length was " + current_request.path_len);
            // TODO: close socket since path is BS
          }
          data = data.slice(4);
        } else {
          // TODO: dupChunk?
          partials.push(data);
          return;
        }
      }

      if (current_request.data_len < 0) {
        if (data.length >= 4) {
          current_request.data_len = data.readUInt32LE(0);
          debug("Got data length: " + current_request.data_len);
          data = data.slice(4);
        } else {
          partials.push(data);
          return;
        }
      }

      if (current_request.timestamp < 0) {
        if (data.length >= 8) {
          current_request.timestamp = data.readUInt32LE(0);
          current_request.timestamp += data.readUInt32LE(4) * 0x100000000;
          debug("Got timestamp: " + current_request.timestamp);
          data = data.slice(8);
        } else {
          partials.push(data);
          return;
        }
      }

      if (current_request.path === null) {
        if (data.length >= current_request.path_len) {
          current_request.path = data.slice(0, current_request.path_len);
          debug("Got path: " + current_request.path);
          data = data.slice(current_request.path_len);
        } else {
          partials.push(data);
          return;
        }
      }

      if (current_request.data === null) {
        if (data.length >= current_request.data_len) {
          current_request.data = data.slice(0, current_request.data_len);
          debug("Got " + current_request.data.length + " bytes of data");
          data = data.slice(current_request.data_len);

          // Here we have a complete request: send it!
          debug("Sending complete request...");
          processData(current_request);
          current_request = blank_request();
        } else {
          partials.push(data);
          return;
        }
      }

      debug("Here we have " + data.length + " bytes left over");
    }
  });

  socket.on('end', function() {
    if (partials.length > 0) {
      console.log("SEVERE: we should have processed " + partials.length + " 'partials'");
      partials = [];
      // reset current_request:
      current_request = blank_request();
    }
    console.log("Client disconnected");
  });

});

server.listen(listen_port, function() {
  console.log('Server listening on 127.0.0.1:' + listen_port);
});
