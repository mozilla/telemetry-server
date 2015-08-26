#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import simplejson as json

def parse_heka_record(record):
    result = json.loads(record.message.payload)
    result["meta"] = {
        # TODO: uuid, logger, severity, env_version, pid
        "Timestamp": record.message.timestamp,
        "Type":      record.message.type,
        "Hostname":  record.message.hostname,
    }

    for field in record.message.fields:
        name = field.name.split('.')
        value = field.value_string
        if field.value_type == 1:
            value = field.value_bytes
        elif field.value_type == 2:
            value = field.value_integer
        elif field.value_type == 3:
            value = field.value_double
        elif field.value_type == 4:
            value = field.value_bool

	if len(name) == 1:  # Treat top-level meta fields as strings
	    result["meta"][name[0]] = value[0] if len(value) else ""
	else:
	    _add_field(result, name, value)

    return result


def _add_field(container, keys, value):
    if len(keys) == 1:
        blob = value[0] if len(value) else ""
        container[keys[0]] = _lazyjson(blob)
        return

    key = keys.pop(0)
    container[key] = container.get(key, {})
    _add_field(container[key], keys, value)


def _lazyjson(content):
    if not isinstance(content, basestring):
        raise ValueError("Argument must be a string.")

    if content.startswith("{"):
        default = {}
    elif content.startswith("["):
        default = []
    else:
        try:
            return float(content) if '.' in content or 'e' in content.lower() else int(content)
        except:
            return content

    class WrapperType(type(default)):
        pass

    def wrap(method_name):
        def _wrap(*args, **kwargs):
            if not hasattr(WrapperType, '__cache__'):
                setattr(WrapperType, '__cache__', json.loads(content))

            cached = WrapperType.__cache__
            method = getattr(cached, method_name)
            return method(*args[1:], **kwargs)

        return _wrap

    wrapper = WrapperType(default)
    for k, v in type(default).__dict__.iteritems():
        if k == "__doc__":
            continue
        else:
            setattr(WrapperType, k, wrap(k))
    return wrapper
