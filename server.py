from datetime import date
from flask import json
from flask import Flask
from flask import request
from revision_cache import RevisionCache
from convert import Converter
import persist

app = Flask(__name__)
cache = RevisionCache("./histogram_cache", "hg.mozilla.org")
converter = Converter(cache)
schema_data = open("./telemetry_schema.json")
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
    return json.dumps(histograms)

def validate_body(json):
    return True

def validate_dims(dimensions):
    if dimensions[1] in ["idle-daily","saved-session"]:
        return True
    else:
        raise ValueError("invalid reason")

#http://<bagheera_host>/submit/telemetry/<id>/<reason>/<appName>/<appVersion>/<appUpdateChannel>/<appBuildID>
@app.route('/submit/telemetry/<id>/<reason>/<appName>/<appVersion>/<appUpdateChannel>/<appBuildID>', methods=['POST'])
def submit(id, reason, appName, appVersion, appUpdateChannel, appBuildID):
    today = date.today().strftime("%Y%m%d")
    # TODO: make 'storage' do this:
    dimensions = [today, reason, appName, appUpdateChannel, appVersion, appBuildID]
    json = request.data
    try:
        validate_body(json)
        validate_dims(dimensions)
        obj, payload_dims = converter.convert_json(json, today)
        storage.write(id, json, dimensions)
        # 201 CREATED
        return "Created", 201
    except ValueError, err:
        storage.write_invalid(id, json, dimensions, err)
        # 400 BAD REQUEST
        return "Bad Request", 400
    except KeyError, err:
        storage.write_invalid(id, json, dimensions, err)
        return "Bad Request JSON", 400

    return "wtf...", 500

if __name__ == '__main__':
    app.debug = True
    app.run()
