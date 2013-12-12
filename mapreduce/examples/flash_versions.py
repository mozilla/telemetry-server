# Flash Versions export, ported from:
#   https://github.com/mozilla-metrics/telemetry-toolbox
import json
import traceback

def map(k, d, v, cx):
    try:
        j = json.loads(v)
        info = j.get("info", {})
        if "OS" not in info:
            return
        if "appName" not in info:
            return

        os = info["OS"]
        appName = info["appName"]

        # Keep [Metro]Firefox documents on windows only
        if appName == "Firefox" or appName == "MetroFirefox":
            if os != "WINNT":
                return
        # Also keep all Fennec documents.
        elif appName != "Fennec":
            return

        out_dims = [appName]
        for f in ["appVersion", "appUpdateChannel"]:
            out_dims.append(info.get(f, "NA"))
        out_dims.append(os)
        for f in ["version", "flashVersion"]:
            out_dims.append(info.get(f, "NA"))

        cx.write(",".join([str(i) for i in out_dims]), 1)
    except Exception as e:
        cx.write(",".join(["Error", str(e), traceback.format_exc()] + d), 1)

def setup_reduce(cx):
    cx.field_separator = ","

def reduce(k, v, cx):
    cx.write(k, sum(v))

combine = reduce
