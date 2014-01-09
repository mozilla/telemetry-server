/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

var restify = require('restify');
var sqlite3 = require('sqlite3').verbose();
var db = new sqlite3.Database("./coordinator.db");


function schema2db(field_name) {
  if (field_name == "reason" || field_name == "submission_date")
    return field_name;
  if (field_name == "appName")
    return "app_name";
  if (field_name == "appUpdateChannel")
    return "app_update_channel";
  if (field_name == "appVersion")
    return "app_version";
  if (field_name == "appBuildID")
    return "app_build_id";

  // TODO: exception?
  return null;
}

function sanitize(value) {
  // See 'safe_filename' in telemetry_schema.py
  if (value) {
    return value.replace(/[^a-zA-Z0-9_/.]/, "_");
  }
  return value;
}

function filter2sql(filter) {
  var query = "SELECT * FROM published_files";
  var conditions = "";
  var params = [];
  for (var i = filter.dimensions.length - 1; i >= 0; i--) {
    //console.log("Checking dimension " + i);
    var dim = filter.dimensions[i];
    if (dim.allowed_values === "*") {
      // Allow all values.  Doesn't actually filter, so skip it.
      console.log("Skipping non-filtering " + dim.field_name + " = *");
      continue;
    }
    var db_field = schema2db(dim.field_name);
    //console.log("Getting dimension for " + dim.field_name + " = " + db_field);
    if (!db_field) {
      // Invalid field name.  TODO: exception
      return null;
    }
    var filter_type = Object.prototype.toString.call(dim.allowed_values);
    //console.log("Type of " + dim.field_name + " is " + filter_type);
    var cond = "";
    if (filter_type === "[object Array]") {
      cond += db_field;
      if (dim.allowed_values.length == 1) {
        cond += " = ?";
      } else {
        cond += " IN (?";
        for (var j = dim.allowed_values.length - 1; j >= 1; j--) {
          cond += ",?";
        }
        cond += ")";
      }
      params = params.concat(dim.allowed_values);
    } else if (filter_type === "[object Object]") {
      if (dim.allowed_values.min && dim.allowed_values.max) {
        cond += "(" + db_field + " >= ? AND " + db_field + " <= ?)";
        params.push(dim.allowed_values.min);
        params.push(dim.allowed_values.max);
      } else if (dim.allowed_values.min) {
        cond += db_field + " >= ?";
        params.push(dim.allowed_values.min);
      } else if (dim.allowed_values.max) {
        cond += db_field + " <= ?";
        params.push(dim.allowed_values.max);
      } else {
        console.log("Found a meaningless allowed_values object: " + dim);
      }
    } else {
      // Unknown... use it directly and let the database figure it out.
      cond = db_field + " = ?";
      params.push(dim.allowed_values);
    }
    if (cond !== "") {
      if (conditions !== "")
        conditions += " AND ";
      conditions += cond;
    }
  }

  if (conditions !== "") {
    query += " WHERE " + conditions;
  }

  for (var i = params.length - 1; i >= 0; i--) {
    params[i] = sanitize(params[i]);
  };
  query += " ORDER BY file_name;";
  return { sql: query, params: params };
}


required_files_params = ["filter"];
function filter_files(req, res, next) {
  console.log(req.params);
  // for (var i = required_files_params.length - 1; i >= 0; i--) {
  //   param_name = required_files_params[i];
  //   if (!req.params[param_name]) {
  //     res.send("Missing parameter: " + param_name);
  //     return next();
  //   }
  // }
  if (!req.params.filter) {
    try {
      req.params.filter = JSON.parse(req.query.filter);
    } catch (e) {
      console.log("Boo!");
    }
  }
  if (!req.params.filter) {
    res.send(400, "Missing or invalid 'filter' parameter");
    return next();
  }
  var filter = req.params.filter;
  var query = filter2sql(filter);

  console.log("running query: " + JSON.stringify(query));

  res.setHeader('content-type', 'application/json');
  console.log("a");

  // TODO: send content-type:application/json
  //res.writeHead();
  console.log("b");
  res.write('{ "files": [');
  console.log("c");
  var first = true;
  db.each(query.sql, query.params, function(err, row) {
    if (err) {
      console.log("Found an err: " + JSON.stringify(err));
      return next(err);
    } else {
      console.log("Found a file: " + JSON.stringify(row));
    }
    if (first) {
      first = false;
    } else {
      res.write(",");
    }
    res.write('"' + row.file_name + "\"\n");
  }, function(err, rowcount) {
    if (err) {
      console.log("Found an err on completion: " + JSON.stringify(err));
      return next(err);
    }
    else {
      console.log("Found " + rowcount + " rows");
    }
    res.write('], "row_count": ' + rowcount + '}');
    res.end();
    return next();
  });
  //job_script = req.params.job_script;

  //res.send("found some files based on your filter: " + JSON.stringify(query));
  //return next();
}

// TODO: load up the schema from the S3 bucket.
var server = restify.createServer({
  name: "Telemetry Coordinator",
});
server.use(restify.bodyParser());
server.use(restify.queryParser({ mapParams: false }));
server.use(restify.requestLogger());
server.post('/files', filter_files);
server.get('/files', filter_files);

server.listen(8080, function() {
  console.log('%s listening at %s', server.name, server.url)
});
