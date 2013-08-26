var net = require('net');
var http = require('http');

var options = {
  hostname: "localhost",
  port: 8080,
  method: "POST"
};

var eol = new Buffer('\n');


// process the data once it's reached end of line
function processData(buf) {
  //console.log("buf is " + buf.toString());
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

  var path = buf.slice(0, i).toString();
  var data = buf.slice(i + 1).toString();

  console.log("Path: " + path);
  //console.log("Data: " + data);
  // now grab a slice of the rest
  buf = buf.slice(i + 1);
  //options.headers = { 'Content-Length': buf.length + 1};
  options.headers = { 'Content-Length': buf.length};

  var req = http.request(options, function(res) {
    console.log("req finished: " + res.statusCode);
  });

  req.on('error', function(e) {
    console.log("req errored");
  });

  //req.write(buf);
  //req.end(eol);
  req.end(buf);
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
var server = net.createServer(function (socket) {
  console.log('Client connected');

  socket.on('readable', function() {
    console.log("readable");
    var data;
    while (data = socket.read()) {
      console.log("read " + data.length + " bytes");
      var start = 0;
      var i = 0;
      // manually search for new line (10 == ascii \n)
      for (i = 0; i < data.length; i++) {
        // we've hit a new line, copy out the data and process
        if (data[i] === 10) {
          console.log("Found a new line at " + i);
          // dupChunk copies up to one less than i (so it skips the newline)
          console.log("Adding eol partial #" + partials.length + " start: " + start + ", i:" + i);
          partials.push(dupChunk(data, start, i));
          processData(Buffer.concat(partials));
          partials = [];
          // add one to skip the new line
          start = i + 1;
          console.log("New start: " + start);
          continue;
        }
      }
      // we've reached the end of the buffer, and there's still buffer left
      if (i === data.length && i > start) {
        console.log("Appending partial #" + partials.length + ", start:" + start + ", end:" + data.length);
        //console.log("Adding chunk: " + data.toString("utf8", start, data.length));
        partials.push(dupChunk(data, start, data.length));
      }
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
