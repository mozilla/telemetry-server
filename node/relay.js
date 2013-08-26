var net = require('net');
var http = require('http');

var options = {
  hostname: "localhost",
  port: 8080,
  method: "POST"
};

var DEBUG = false;

var eol = new Buffer('\n');

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
  console.log("Stats for " + new Date());
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

  // TODO: run gc
  /*
  console.log("Running gc");
  gc();
  console.log("Done");
  */
}, 10000);

function debug(message) {
  if(DEBUG) {
    console.log(message);
  }
}

// process the data once it's reached end of line
function processData(buf) {
  //debug("buf is " + buf.toString());
  if (buf.length == 0) {
    return;
  }
  // manually search for \t == 9
  var i = 0;
  for (; i < buf.length; i++) {
    // found \t
    if (buf[i] === 9) {
      options.path = buf.slice(0, i).toString();
      break;
    }
  }

  debug("Path: " + options.path);

  // Buffer -> String conversion is expensive. Don't do it unless necessary.
  //var data = buf.slice(i + 1).toString();
  //debug("Data: " + data);
  
  // Send the rest as the request body.
  buf = buf.slice(i + 1);
  options.headers = {
    'Content-Length': buf.length,
    'Connection': 'keep-alive'
  };

  var req = http.request(options, function(res) {
    debug("req finished: " + res.statusCode);
    stats.responses[res.statusCode] = (stats.responses[res.statusCode] || 0) + 1;
    stats.completed++;
    stats.bytes_sent += buf.length;
    res.on('data', function(chunk) {
      // discard.
    });
  });

  /*
  req.on('socket', function(socket) {
    socket.setTimeout(500);
    socket.on('timeout', function(){
      console.log("request timed out, aborting");
      req.abort();
    });
  });
  */

  req.on('error', function(e) {
    console.log("Path " + path + " errored: " + e.message);
    stats.errors++;
  });

  //req.write(buf);
  //req.end(eol);
  req.end(buf);
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

var partials = [];
var data_read = 0;
var server = net.createServer(function (socket) {
  console.log('Client connected');

  socket.on('data', function(data) {
    debug("data");
    debug("read " + data.length + " bytes");
    data_read += data.length;
    var start = 0;
    var i = 0;
    // manually search for new line (10 == ascii \n)
    for (i = 0; i < data.length; i++) {
      // we've hit a new line, copy out the data and process
      if (data[i] === 10) {
        debug("Found a new line at " + i);
        // dupChunk copies up to one less than i (so it skips the newline)
        debug("Adding eol partial #" + partials.length + " start: " + start + ", i:" + i);
        if (partials.length === 0) {
          processData(dupChunk(data, start, i));
        } else {
          partials.push(dupChunk(data, start, i));
          processData(Buffer.concat(partials));
          partials.length = 0;
        }
        // add one to skip the new line
        start = i + 1;
        //debug("New start: " + start);
        continue;
      }
    }
    // we've reached the end of the buffer, and there's still buffer left
    if (i === data.length && i > start) {
      debug("Appending partial #" + partials.length + ", start:" + start + ", end:" + data.length);
      //debug("Adding chunk: " + data.toString("utf8", start, data.length));
      partials.push(dupChunk(data, start, data.length));
    }
    
    /*
    // TODO: test to see if we free up memory.
    if (data_read > 1024 * 1024 * 1024) {
      socket.pause();
      console.log("pausing after reading " + data_read + " bytes");
    }
    */
    var outstanding = stats.sent - stats.completed;
    if (outstanding > 5000) {
      socket.pause();
      console.log("Too many pending requests (" + outstanding + "). pausing");
      setTimeout(function(){
        socket.resume();
      }, 1000);
    }
  });

  socket.on('end', function() {
    processData(Buffer.concat(partials));
    console.log("Client disconnected");
  });

});

server.listen(9090, function() {
  console.log('Server listening on 127.0.0.1:9090');
});
