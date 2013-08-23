var net = require('net');
var http = require('http');

var options = {
  hostname: "localhost",
  port: 8080,
  method: "POST"
};

var counts = {
};

var total_requests_sent = 0;
var total_requests_completed = 0;
var total_requests_error = 0;
var data_sent = 0;

var partial = '';
function line(l) {
  if (l.length > 0) {
    total_requests_sent++;
    //console.log("processing a line: " + l.substring(0, 80) + "...");
    if (total_requests_sent > 0 && total_requests_sent % 500 == 0) {
      console.log("counts: " + JSON.stringify(counts) + ", sent: " + data_sent + " bytes in " + total_requests_sent + " requests, of which " + total_requests_completed + " completed normally.");
    }

    var parts = l.split("\t");
    var path = parts[0];
    var data = parts[1];
    //console.log("submitting to " + path);
    options.path = path;
    data_length = data.length + 1;
    options.headers = {"Content-Length": data_length}
    data_sent += data_length;
    var req = http.request(options, function(response) {
      if (counts[response.statusCode]) {
        counts[response.statusCode]++;
      } else {
        counts[response.statusCode] = 1;
      }
      total_requests_completed++;
    });

    req.on('error', function(e) {
      var k = "err:" + e.message;
      total_requests_error++;
      if (counts[k]) {
        counts[k]++;
      } else {
        counts[k] = 1;
      }
    });
    req.end(data + "\n");
  //} else {
  //  console.log("got an empty line");
  }
}

var server = net.createServer(function (socket) {
  console.log("Server connected");
  socket.on('end', function() {
    line(partial);
    console.log("Client disconnected");
  });

  socket.on('data', function(chunk) {
    var outstanding = total_requests_sent - total_requests_completed - total_requests_error;
    if (outstanding > 500) {
      // If we don't rate-limit, we will run out of memory.
      console.log("there are too many outstanding requests (" + outstanding + "), pausing for 1sec");
      socket.pause();
      setTimeout(function(){ socket.resume(); }, 1000);
    }
    // console.log("Got a chunk of data");
    partial += chunk;
    var eol = partial.indexOf("\n");
    while (eol > 0) {
      // use eol - 1 to skip the \n itself.
      line(partial.substring(0, eol - 1));
      partial = partial.substring(eol + 1);
      eol = partial.indexOf("\n");
    }
  });
});

server.listen(9090);
