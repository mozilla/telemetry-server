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
