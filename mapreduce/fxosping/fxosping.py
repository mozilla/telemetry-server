import csv
import io
import json

def safe_key(pieces):
    output = io.BytesIO()
    writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
    writer.writerow(pieces)
    return output.getvalue().strip()

def split_key(key):
    f = io.StringIO(unicode(key))
    reader = csv.reader(f, quoting=csv.QUOTE_MINIMAL)
    return reader.next()

def map(key, dims, value, context):
    data = json.loads(value)
    reason, appName, appUpdateChannel, appVersion, appBuildID, submission_date = dims

    def genkey(*names):
        return safe_key((submission_date, ) + names)

    def dataval(key):
        return data.get(key, 'unknown')

    def strval(d, key):
        return d.get(key, 'unknown') or 'unknown'

    hours = -1
    time_to_ping = 'unknown'
    if 'pingTime' in data and 'activationTime' in data:
        # Time to ping in hours
        hours = float(int(data['pingTime']) - int(data['activationTime'])) / (60 * 60 * 1000)
        time_to_ping = '%d' % round(hours)

    context.write(genkey('os', strval(data, 'deviceinfo.os')), 1)
    context.write(genkey('software', strval(data, 'deviceinfo.software')), 1)
    context.write(genkey('time_to_ping', time_to_ping), 1)
    if hours != -1:
        context.write(genkey('median_time_to_ping'), hours)

    context.write(genkey('resolution', dataval('screenWidth'), dataval('screenHeight')), 1)
    context.write(genkey('pixel_ratio', dataval('devicePixelRatio')), 1)
    context.write(genkey('locale', strval(data, 'locale')), 1)
    context.write(genkey('hardware', strval(data, 'deviceinfo.hardware')), 1)
    context.write(genkey('model', strval(data, 'deviceinfo.product_model')), 1)
    context.write(genkey('firmware_revision', strval(data, 'deviceinfo.firmware_revision')), 1)
    context.write(genkey('ping_count'), 1)
    context.write(genkey('update_channel', appUpdateChannel), 1)

    icc = data.get('icc')
    if icc:
        mnc = strval(icc, 'mnc')
        mcc = strval(icc, 'mcc')
        spn = strval(icc, 'spn')
        context.write(genkey('icc', mnc, mcc, spn), 1)
    else:
        context.write(genkey('icc', 'unknown', 'unknown', 'unknown'), 1)

    network = data.get('network')
    if network:
        mnc = strval(network, 'mnc')
        mcc = strval(network, 'mcc')
        operator = strval(network, 'operator')
        context.write(genkey('network', mnc, mcc, operator), 1)
    else:
        context.write(genkey('network', 'unknown', 'unknown', 'unknown'), 1)

def setup_reduce(context):
    context.field_separator = ','

def median(values):
    s = sorted(values)
    length = len(s)
    if length % 2 == 0:
        return (s[length / 2] + s[length / 2 - 1]) / 2.0

    return s[length / 2]

def reduce(key, values, context):
    key_parts = split_key(key)
    key_name = key_parts[1]

    if key_name == 'median_time_to_ping':
        context.write(key, median(values))
        return

    context.write(key, sum(values))
