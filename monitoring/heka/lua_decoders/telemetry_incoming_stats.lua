-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
sample input
------------
{"task": "Reader", "channel": "ALL", "bad_records": 9, "start_time": "2014-02-23T19:51:17.793986Z", "bytes_uncompressed": 6833422, "end_time": "2014-02-23T19:51:20.609707Z", "bytes_written": 3854340, "records_read": 100, "duration": 2.815721, "records_written": 91, "bytes_read": 1504183, "bad_records.uuid_only_path": 8, "bad_records.conversion_error": 1}

Injected Heka message
---------------------
Timestamp: 2014-02-23 11:51:17.793986048 -0800 PST
Type: telemetry.incoming_stats
Hostname: trink-x230
Pid: 0
UUID: bb8105b4-e71b-45c7-a33e-a9fad3445d0b
Logger: Reader
Payload:
EnvVersion:
Severity: 7
Fields: [
name:"bytes_read" value_type:DOUBLE representation:"B" value_double:1.504183e+06
name:"end_time" value_type:DOUBLE representation:"time_ns" value_double:1.393185080609707e+18
name:"bad_records.conversion_error" value_type:DOUBLE value_double:1
name:"bad_records" value_type:DOUBLE value_double:9
name:"bad_records.uuid_only_path" value_type:DOUBLE value_double:8
name:"records_written" value_type:DOUBLE value_double:91
name:"duration" value_type:DOUBLE representation:"s" value_double:2.815721
name:"bytes_written" value_type:DOUBLE representation:"B" value_double:3.85434e+06
name:"channel" value_string:"ALL"
name:"bytes_uncompressed" value_type:DOUBLE representation:"B" value_double:6.833422e+06
name:"records_read" value_type:DOUBLE value_double:100 ]
--]]

require "cjson"
local dt = require "date_time"

local metadata = {
    duration = {value=0, representation="s"},
    end_time = {value=0, representation="time_ns"},
    bytes_read = {value=0, representation="B"},
    bytes_written = {value=0, representation="B"},
    bytes_uncompressed = {value=0, representation="B"}
}

local msg = {
    Timestamp = nil,
    Type = "telemetry.incoming_stats",
    Fields = nil
}

function process_message()
    local ok, json = pcall(cjson.decode, read_message("Payload"))
    if not ok then return -1 end

    local t = lpeg.match(dt.rfc3339, json.start_time)
    if not t then return -1 end
    msg.Timestamp = dt.time_to_ns(t)
    json.start_time = nil

    t = lpeg.match(dt.rfc3339, json.end_time)
    if not t then return -1 end
    metadata.end_time.value = dt.time_to_ns(t)
    json.end_time = metadata.end_time

    msg.Logger = json.task
    json.task = nil

    if json.duration then
        metadata.duration.value = json.duration
        json.duration = metadata.duration
    end

    if json.bytes_read then
        metadata.bytes_read.value = json.bytes_read
        json.bytes_read = metadata.bytes_read
    end

    if json.bytes_written then
        metadata.bytes_written.value = json.bytes_written
        json.bytes_written = metadata.bytes_written
    end

    if json.bytes_uncompressed then
        metadata.bytes_uncompressed.value = json.bytes_uncompressed
        json.bytes_uncompressed = metadata.bytes_uncompressed
    end

    msg.Fields = json
    if not pcall(inject_message, msg) then return -1 end

    return 0
end
