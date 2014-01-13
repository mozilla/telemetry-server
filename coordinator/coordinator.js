/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/*
 * Coordinator usage:
 *   - You:
 *      I want to run "histogram analysis" for "20140101": run launcher.py with args
 *        name="histogram-analysis-v1"
 *        owner="mreid@mozilla.com"
 *        filter="somefile-with-date-in.json"
 *        code="s3://pathto/code.tar.gz"
 *   - Coordinator:
 *        looks in "published_files" for matches of filter
 *        splits list into groups of X MB, creates an entry in "tasks"
 *        inserts files into task_files referencing ^
 *        scales the autoscale group to target based on number / size of tasks (some heuristic)
 *   - Worker node: manager.py => asks coordinator for task(s)
 *      Coordinator sets the taken_until field... and returns list of files + task info
 *      worker success:
 *        worker processes files, uploads to S3
 *        worker informs coordinator "I'm done"
 *        coordinator marks task status as "done"
 *        coordinator tells autoscale group to scale down by 1 (or something)
 *      worker fail:
 *        task becomes available again after timeout and retries is decremented
 *        when we run out of retries, inform owner that the task is broken
 *   - Task-specific aggregator node:
 *      Get all "done" tasks from coordinator with name="histogram analysis"
 */

var bunyan = require('bunyan');
var restify = require('restify');
var pg = require('pg');

var connection_string = process.argv[2];
var log = bunyan.createLogger({name: "coordinator"});

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
  var currentParam = 0;
  var params = [];
  for (var i = filter.dimensions.length - 1; i >= 0; i--) {
    log.trace("Checking dimension " + i);
    var dim = filter.dimensions[i];
    if (dim.allowed_values === "*") {
      // Allow all values.  Doesn't actually filter, so skip it.
      log.info("Skipping non-filtering " + dim.field_name + " = *");
      continue;
    }
    var db_field = schema2db(dim.field_name);
    log.trace("Getting dimension for " + dim.field_name + " = " + db_field);
    if (!db_field) {
      // Invalid field name.  TODO: exception
      return null;
    }
    var filter_type = Object.prototype.toString.call(dim.allowed_values);
    log.trace("Type of " + dim.field_name + " is " + filter_type);
    var cond = "";
    if (filter_type === "[object Array]") {
      cond += db_field;
      if (dim.allowed_values.length == 1) {
        cond += " = $" + ++currentParam;
      } else {
        cond += " IN ($" + ++currentParam;
        for (var j = dim.allowed_values.length - 1; j >= 1; j--) {
          cond += ",$" + ++currentParam;
        }
        cond += ")";
      }
      params = params.concat(dim.allowed_values);
    } else if (filter_type === "[object Object]") {
      if (dim.allowed_values.min && dim.allowed_values.max) {
        cond += "(" + db_field + " >= $" + ++currentParam + " AND " + db_field + " <= $" + ++currentParam + ")";
        params.push(dim.allowed_values.min);
        params.push(dim.allowed_values.max);
      } else if (dim.allowed_values.min) {
        cond += db_field + " >= $" + ++currentParam;
        params.push(dim.allowed_values.min);
      } else if (dim.allowed_values.max) {
        cond += db_field + " <= $" + ++currentParam;
        params.push(dim.allowed_values.max);
      } else {
        log.info("Found a meaningless allowed_values object: " + dim);
      }
    } else {
      // Unknown... use it directly and let the database figure it out.
      cond = db_field + " = $" + ++currentParam;
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
  query += " ORDER BY file_name LIMIT 10;";
  return { sql: query, params: params };
}

function get_filtered_files(req, res, next) {
  log.info(req.params);
  if (!req.params.filter) {
    try {
      req.params.filter = JSON.parse(req.query.filter);
    } catch (e) {
      log.info("Boo!");
    }
  }
  if (!req.params.filter) {
    res.send(400, "Missing or invalid 'filter' parameter");
    return next();
  }
  var filter = req.params.filter;

  // Force the type to application/json since we'll be streaming out the rows
  // manually.
  res.setHeader('content-type', 'application/json');
  var first = true;
  filter_files(filter, function(err, row) {
    // Process each file record
    if (err) {
      log.info("Found an err: " + JSON.stringify(err));
      return next(err);
    }
    if (first) {
      res.write('{ "files": [');
      first = false;
    } else {
      res.write(",");
    }
    res.write('"' + row.file_name + "\"\n");
  }, function(err, rowcount) {
    // End of files, finish up.
    if (err) {
      log.info("Found an err on completion: " + JSON.stringify(err));
      return next(err);
    }
    res.write('], "row_count": ' + rowcount + '}');
    res.end();
    return next();
  });
}

function filter_files(filter, onfile, onend) {
  // TODO: iterate through matching files calling onfile(err, row) for each, then call onend(err, rowcount)
  var query = filter2sql(filter);
  log.info("running query: " + JSON.stringify(query));
  pg.connect(connection_string, function(err, client, done) {
    if (err) {
      log.error(err);
      onend(err, null);
      done();
      return;
    }

    var cq = client.query(query.sql, query.params);
    cq.on('row', function(row) {
      log.trace("Retrieved one row: " + JSON.stringify(row));
      onfile(null, row);
    });

    cq.on('end', function(result){
      onend(null, result.rowCount);
      done();
    });

    cq.on('error', function(err){
      onend(err);
      done();
    })
  });

}

var BATCH_SIZE = 500 * 1024 * 1024;
var required_task_fields = ["name", "owner", "filter", "code_uri"];
function create_task(req, res, next) {
  for (var i = required_task_fields.length - 1; i >= 0; i--) {
    param_name = required_task_fields[i];
    if (!req.params[param_name]) {
      res.send(400, "Missing parameter: " + param_name);
      return next();
    }
  }

  // Get files matching filter
  // Split into tasks
  // Insert groups of files into task_files
  tasks = []
  current_batch_size = 0;
  current_batch = [];
  function add_task(err, task_id) {
    if (err) {
      log.info("error adding task");
      return next(err);
    }
    log.info("Adding task id: " + task_id);
    tasks.push(task_id);
  }
  filter_files(req.params.filter, function(err, row){
    if (current_batch_size > BATCH_SIZE) {
      save_batch(req.params.name, req.params.owner, req.params.code_uri, current_batch, add_task);
      log.info("saved intermediate batch");
      current_batch_size = 0;
      current_batch = [];
    }
    current_batch_size += row.file_size;
    current_batch.push(row.file_id);
  }, function(err, count){
      if (current_batch.length > 0) {
        save_batch(req.params.name, req.params.owner, req.params.code_uri, current_batch, function(err, task_id) {
          if (err) {
            return next(err);
          }
          tasks.push(task_id);
          log.info("saved final batch");
          log.info(JSON.stringify(tasks));
          var task_info = {name: req.params.name, tasks: tasks};
          // TODO: email task info to owner_email
          // TODO: spin up some nodes to tackle this task.
          res.send(task_info);
        });
      }
      return next();
  });
}

var sql_create_task      = 'INSERT INTO tasks (name, owner_email, code_uri) VALUES (?,?,?);';
var sql_create_task_file = 'INSERT INTO task_files (task_id, file_id) VALUES (?,?);';
function save_batch(name, owner, code_uri, files, onfinish) {
  log.info("Adding a task with " + files.length + " files");
  var stmt = db.prepare(sql_create_task_file);
  db.run(sql_create_task, name, owner, code_uri, function(err) {
    if (err) {
      // ABORT! there was an error
      onfinish(err, null);
    } else {
      var task_id = this.lastID;
      // for file_id in files:
      //   INSERT INTO task_files (task_id, file_id) VALUES (?, ?);
      for (var i = files.length - 1; i >= 0; i--) {
        stmt.run(task_id, files[i]);
      }
      stmt.finalize();
      onfinish(null, task_id);
    }
  });
}

var sql_get_task_info_by_name = 'SELECT * FROM tasks WHERE name = ? ORDER BY task_id;';
function get_task_info_by_name(req, res, next) {
  // Get information about tasks with the specified name
  if (!req.params.name) {
    res.send(400, "Missing 'name' parameter");
    return next();
  }
  log.info("Getting task info for " + req.params.name);
  var task_info = {name: req.params.name, tasks: []};
  db.each(sql_get_task_info_by_name, req.params.name, function(err, row){
    if (err) {
      log.info("error getting tasks");
      return next(err);
    }
    log.info("Found a task: " + row.name + ", " + row.task_id);
    task_info.tasks.push({
      id: row.task_id,
      code_uri: row.code_uri,
      owner: row.owner_email,
      status: row.status,
      claimed_until: row.claimed_until,
      retries_remaining: row.retries_remaining,
    });
  }, function(err, rowcount){
    if (err) {
      log.info("error on last get task");
      return next(err);
    }
    log.info("Found " + rowcount + " rows for " + req.params.name);
    task_info.num_tasks = rowcount;
    res.send(task_info);
    return next();
  });
}

// There should only ever be at most one, but might as well make sure.
var sql_get_task_info_by_id = 'SELECT * FROM tasks WHERE name = ? AND task_id = ? LIMIT 1;';
var required_get_task_fields = ["name", "task_id"];
function get_task_info(req, res, next) {
  // Get information about the task with the specified name and id
  for (var i = required_get_task_fields.length - 1; i >= 0; i--) {
    param_name = required_get_task_fields[i];
    if (!req.params[param_name]) {
      res.send(400, "Missing parameter: " + param_name);
      return next();
    }
  }

  log.info("Getting task info for " + req.params.name + " id: " + req.params.task_id)

  var task_info = {name: req.params.name, id: req.params.task_id};
  db.each(sql_get_task_info_by_id, req.params.name, req.params.task_id, function(err, row){
    if (err) {
      log.info("error getting task by id");
      return next(err);
    }
    log.info("Found a task: " + row.name + ", " + row.task_id);
    task_info.code_uri = row.code_uri;
    task_info.owner = row.owner_email;
    task_info.status = row.status;
    task_info.claimed_until = row.claimed_until;
    task_info.retries_remaining = row.retries_remaining;
  }, function(err, rowcount){
    if (err) {
      log.info("error finishing get task by id");
      return next(err);
    }
    if (rowcount == 0) {
      res.send(404, "No tasks found");
    } else {
      res.send(task_info);
    }
    return next();
  });
}

// TODO: load up the schema from the S3 bucket.
var server = restify.createServer({
  name: "Telemetry Coordinator",
});
server.use(restify.bodyParser());
server.use(restify.queryParser({ mapParams: false }));
server.use(restify.requestLogger());
server.post('/files', get_filtered_files);
server.get('/files', get_filtered_files);
server.post('/tasks', create_task);
server.get('/tasks/:name', get_task_info_by_name)
server.get('/tasks/:name/:task_id', get_task_info)

server.listen(8080, function() {
  log.info('%s listening at %s', server.name, server.url)
});
