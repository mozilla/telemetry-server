"""                                                                             
This Source Code Form is subject to the terms of the Mozilla Public             
License, v. 2.0. If a copy of the MPL was not distributed with this             
file, You can obtain one at http://mozilla.org/MPL/2.0/.                        
"""

from datetime import date
from flask import json
from flask import Flask
from flask import request
import flask
from revision_cache import RevisionCache
from convert import Converter
import persist

app = Flask(__name__)
cache = RevisionCache("./histogram_cache", "hg.mozilla.org")
converter = Converter(cache)
schema_filename = "./telemetry_schema.json"
schema_data = open(schema_filename)
schema = json.load(schema_data)
storage = persist.StorageLayout(schema, "./data")

@app.route('/', methods=['GET', 'POST'])
def licese_and_registration_please():
    return "Telemetry HTTP Server v0.0"

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
    # TODO: make 'storage' do this:
    dimensions = [today, reason, appName, appUpdateChannel, appVersion, appBuildID]
    return submit(id, today, dimensions)

@app.route('/submit/telemetry/<id>', methods=['POST'])
def submit_without_dims(id):
    today = date.today().strftime("%Y%m%d")
    return submit(id, today)

def submit(id, today, dimensions=None):
    json = request.data
    try:
        validate_body(json)
        if dimensions is not None:
            validate_dims(dimensions)
        converted, payload_dims = converter.convert_json(json, today)
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
            dimensions = [today]
        storage.write_invalid(id, json, dimensions, err)
        # 400 BAD REQUEST
        return "Bad Request", 400
    except KeyError, err:
        storage.write_invalid(id, json, dimensions, err)
        return "Bad Request JSON", 400
    return "wtf...", 500

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0', port=8080)
