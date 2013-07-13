"""                                                                             
This Source Code Form is subject to the terms of the Mozilla Public             
License, v. 2.0. If a copy of the MPL was not distributed with this             
file, You can obtain one at http://mozilla.org/MPL/2.0/.                        
"""

from datetime import date
try:
    import simplejson as json
except ImportError:
    import json
from flask import Flask
from flask import request
import flask
from revision_cache import RevisionCache
from telemetry_schema import TelemetrySchema
from convert import Converter
from persist import StorageLayout

app = Flask(__name__)

server_config_file = "./telemetry_server_config.json"
try:
    server_config = open(server_config_file, "r")
    config = json.load(server_config)
    server_config.close()
except IOError:
    config = {}

## General server config
# rotate log files at 500MB.
max_log_size = config.get("max_log_size", 500 * 1024 * 1024)
max_open_file_handles = config.get("max_open_file_handles", 500)
server_motd = config.get("motd", date.today().strftime("since %Y-%m-%d"))
server_port = config.get("port", 8080)
server_debug = config.get("debug", True)
convert_payloads = config.get("convert_payloads", True)

## Revision Cache
revision_cache_path = config.get("revision_cache_path", "./histogram_cache")
revision_cache_server = config.get("revision_cache_server", "hg.mozilla.org")
cache = RevisionCache(revision_cache_path, revision_cache_server)

## Schema
schema_filename = config.get("schema_filename", "./telemetry_schema.json")
schema_data = open(schema_filename)
schema = TelemetrySchema(json.load(schema_data))
schema_data.close()

## Storage
storage_path = config.get("storage_path", "./data")

converter = Converter(cache, schema)
storage = StorageLayout(schema, storage_path, max_log_size, max_open_file_handles)

@app.route('/', methods=['GET', 'POST'])
def licese_and_registration_please():
    return "Telemetry HTTP Server v0.0 " + server_motd

@app.route('/histograms/<repo>/<revision>')
def get_histograms(repo, revision):
    histograms = cache.get_revision(repo, revision)
    if histograms is None:
        abort(404)
    return json.dumps(histograms, separators=(',', ':'))

@app.route('/telemetry_schema')
def get_schema():
    return flask.send_file(schema_filename, mimetype="application/json")

def is_string(s):
    return isinstance(s, basestring)

def validate_body(json):
    return True

def validate_dims(dimensions):
    # Make sure all dimensions are strings
    if reduce(lambda x, y: x and is_string(y), dimensions, True):
        return True
    else:
        raise ValueError("Invalid dimension")

#http://<bagheera_host>/submit/telemetry/<id>/<reason>/<appName>/<appVersion>/<appUpdateChannel>/<appBuildID>
@app.route('/submit/telemetry/<id>/<reason>/<appName>/<appVersion>/<appUpdateChannel>/<appBuildID>', methods=['POST'])
def submit_with_dims(id, reason, appName, appVersion, appUpdateChannel, appBuildID):
    today = date.today().strftime("%Y%m%d")
    info = {
            "reason": reason,
            "appName": appName,
            "appVersion": appVersion,
            "appUpdateChannel": appUpdateChannel,
            "appBuildID": appBuildID
    }
    dimensions = schema.dimensions_from(info, today)
    return submit(id, request.data, today, dimensions)

@app.route('/submit/telemetry/<id>', methods=['POST'])
def submit_without_dims(id):
    today = date.today().strftime("%Y%m%d")
    return submit(id, request.data, today)

@app.route('/submit/telemetry/batch', methods=['POST'])
def submit_batch():
    return_message = "OK"
    status = 201;
    today = date.today().strftime("%Y%m%d")
    parts = request.data.split("\t")
    # incoming data is <id1>\t<json1>\t<id2>\t<json2>....
    while len(parts) > 0:
        # pop pairs off the end of the array
        json = parts.pop()
        key = parts.pop()
        print "Key:", key, "JSON:", json[0:50]
        try:
            message, code = submit(key, json, today)
            if code != 201:
                return_message = message
                status = code
        except:
            pass
    return return_message, status


def submit(id, json, today, dimensions=None):
    try:
        validate_body(json)
        if dimensions is not None:
            validate_dims(dimensions)
        if convert_payloads:
            converted, payload_dims = converter.convert_json(json, today)
        else:
            converted = json
            payload_dims = schema.dimensions_from({}, today)
        if dimensions is None:
            validate_dims(payload_dims)
            dimensions = payload_dims

        # TODO: check if payload_dims are the same as incoming dims?
        storage.write(id, converted, dimensions)
        # 201 CREATED
        return "Created", 201
    except ValueError, err:
        if dimensions is None:
            # At the very least, we know what day it is
            dimensions = schema.dimensions_from({}, today)
        storage.write_invalid(id, json, dimensions, err)
        # 400 BAD REQUEST
        return "Bad Request", 400
    except KeyError, err:
        storage.write_invalid(id, json, dimensions, err)
        return "Bad Request JSON", 400
    return "wtf...", 500

if __name__ == '__main__':
    app.debug = server_debug
    app.run(host='0.0.0.0', port=server_port)
