Payload Format
==============

Version 1
---------

The standard format expected by the server is Version 1.

Schema version 1 is pretty minimal, and means this:
- top-level contains a `ver` field with a value of 1.
- top-level contains an `info` sub-object containing at minimum: app version, app name, update channel, build id, reason (using the expected field names).
- if a top-level object called `histograms` is present, we additionally require a `revision` field in the `info` object so we know what version of the source to validate against.

A minimal version 1 payload looks like:
```js
{
  "ver": 1,
  "info": {
    "appName": "Firefox",
    "appVersion": "37",
    "appUpdateChannel": "release",
    "appBuildID": "20150327030201",
    "reason": "saved-session"
  }
}
```

A minimal version 1 payload with `histograms` looks like:

```js
{
  "ver": 1,
  "info": {
    "appName": "Firefox",
    "appVersion": "37",
    "appUpdateChannel": "release",
    "appBuildID": "20150327030201",
    "reason": "saved-session",
    "revision": "https://hg.mozilla.org/mozilla-central/rev/44ae8462d6ab"
  },
  "histograms": {
    "CYCLE_COLLECTOR_NEED_GC": {
      "sum_squares_hi": 0,
      "sum_squares_lo": 1,
      "sum": 1,
      "values": {
        "2": 0,
        "1": 1,
        "0": 3116
      },
      "histogram_type": 2,
      "bucket_count": 3,
      "range": [1, 2]
    }
  }
}
```

Version 2
---------

Version 2 is the same as version 1, but contains `histograms` converted to the [compact histogram format](StorageFormat.md)

Example:
```js
{
  "ver": 2,
  "info": {
    "appName": "Firefox",
    "appVersion": "37",
    "appUpdateChannel": "release",
    "appBuildID": "20150327030201",
    "reason": "saved-session",
    "revision": "https://hg.mozilla.org/mozilla-central/rev/44ae8462d6ab"
  },
  "histograms": {
    "CYCLE_COLLECTOR_NEED_GC": [3116, 1, 0, 1, -1, -1, 1, 0]
  }
}
```

Version 3
---------

Version 3 is a special case for FxOS 1.3 devices that used a non-standard format.
See [Bug 969101](https://bugzilla.mozilla.org/show_bug.cgi?id=969101#c37) for more details.


Version 4
---------

Version 4 is the "Unified" Telemetry format, and is described by the [Mozilla Source Tree Docs](https://ci.mozilla.org/job/mozilla-central-docs/Tree_Documentation/toolkit/components/telemetry/telemetry/common-ping.html)

Version 4 contains a top-level `version` field rather than `ver`, among many other changes from previous versions.

When the server validates a v4 payload, it expects the following:
- Top level field called "version" with a value of 4
- Top level field called "type" with a string value
- Top level field called "application" with an object value
- Top level field called "payload" with an object value
- If "type" is `main` or `saved-session`, there are further requirements:
  - `payload/info` should be present (object)
  - `payload/histograms` should be present (object)
  - If "type" is `saved-session`, `payload/environment` should be present (object)
  - `payload/info/revision` should point to the mercurial revision of the build (and hence Histograms.json)
  - `payload/histograms` should not contain any invalid histograms per the spec in [Histograms.json](http://hg.mozilla.org/releases/mozilla-release/raw-file/tip/toolkit/components/telemetry/Histograms.json)

Example minimal version 4 payload:
```js
{
  "type": "awesome-data",
  "id": <generated UUID>,
  "creationDate": "2015-06-03T13:21:58Z",
  "version": 4,
  "application": {
    architecture: "x86",
    buildId: "20150610999999",
    name: "Firefox",
    version: "41.0a1",
    vendor: "Mozilla",
    platformVersion: "41.0a1",
    xpcomAbi: "x86-msvc",
    channel: "default"
  },
  "payload": {
    ... awesome data here ...
  }
}
```

Version 5
---------

Version 5 is a Unified (version 4) payload converted to be compatible with version 1 and 2 payloads.

This primarily involves restoring the top-level `info` and `ver` fields, but
there are several other tweaks in the `convert_saved_session` function of [telemetry/convert.py](https://github.com/mozilla/telemetry-server/blob/master/telemetry/convert.py) as well.
