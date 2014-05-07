import json

def map(key, dims, value, context):
    data = json.loads(value)
    reason, appName, appUpdateChannel, appVersion, appBuildID, submission_date = dims

    def dataval(key):
        return data.get(key, 'unknown')

    def strval(d, key):
        if not d:
            return 'unknown'
        return d.get(key, 'unknown') or 'unknown'

    hours = -1
    time_to_ping = 'unknown'
    if 'pingTime' in data and 'activationTime' in data:
        # Time to ping in hours
        hours = float(int(data['pingTime']) - int(data['activationTime'])) / (60 * 60 * 1000)
        time_to_ping = '%d' % round(hours)

    context.write(key, submission_date)
    context.write(key, strval(data, 'deviceinfo.os'))
    context.write(key, strval(data, 'deviceinfo.software'))
    context.write(key, time_to_ping)
    context.write(key, dataval('screenWidth'))
    context.write(key, dataval('screenHeight'))
    context.write(key, dataval('devicePixelRatio'))
    context.write(key, strval(data, 'locale'))
    context.write(key, strval(data, 'deviceinfo.hardware'))
    context.write(key, strval(data, 'deviceinfo.product_model'))
    context.write(key, strval(data, 'deviceinfo.firmware_revision'))
    context.write(key, appUpdateChannel)

    icc = data.get('icc')
    context.write(key, strval(icc, 'mnc'))
    context.write(key, strval(icc, 'mcc'))
    context.write(key, strval(icc, 'spn'))

    network = data.get('network')
    context.write(key, strval(network, 'mnc'))
    context.write(key, strval(network, 'mcc'))
    context.write(key, strval(network, 'operator'))

    info = data.get('info')
    context.write(key, strval(info, 'geoCountry'))

def setup_reduce(context):
    context.field_separator = ','

def reduce(key, values, context):
    context.writecsv(values)
