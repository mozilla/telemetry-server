-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
sample input
------------
{"url":"/submit/sample","duration_ms":0.547324,"code":200,"size":4819,"level":"info","message":"OK","timestamp":"2013-09-10T20:43:17.217Z"}

Injected Heka message
---------------------
Timestamp: 2013-09-10 20:43:17.216999936 +0000 UTC
Type: telemetry.server
Hostname: trink-x230
Pid: 0
UUID: 2be3ed98-89e8-4bd0-a7c4-9aebe8747a8b
Logger: TelemetryServerInput
Payload:
EnvVersion:
Severity: 6
Fields: [
name:"message" value_string:"OK"
name:"code" value_type:DOUBLE value_double:200
name:"url" value_string:"/submit/sample"
name:"duration" value_type:DOUBLE representation:"ms" value_double:0.547324
name:"size" value_type:DOUBLE representation:"B" value_double:4819 ]
--]]

require "cjson"

local dt = require "date_time"
local syslog = require "syslog"

local metadata = {
    duration = {value=0, representation="ms"},
    size = {value=0, representation="B"},
}

local msg = {
    Timestamp = nil,
    Type = "telemetry.server",
    Severity = nil,
    Fields = nil
}

function process_message()
    json = cjson.decode(read_message("Payload"))
    if not json then return -1 end

    local t = lpeg.match(dt.rfc3339, json.timestamp)
    if not t then return -1 end
    msg.Timestamp = dt.time_to_ns(t)
    json.timestamp = nil

    msg.Severity = lpeg.match(syslog.severity, json.level)
    json.level = nil

    metadata.duration.value = json.duration_ms
    json.duration = metadata.duration
    json.duration_ms = nil

    metadata.size.value = json.size
    json.size = metadata.size

    msg.Fields = json
    if not pcall(inject_message, msg) then return -1 end

    return 0
end
