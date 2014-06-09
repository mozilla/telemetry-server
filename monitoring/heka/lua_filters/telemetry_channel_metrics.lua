-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

require "circular_buffer"
require "string"

local rows = read_config("rows") or 1440
local sec_per_row = read_config("sec_per_row") or 60
local REQUESTS    = 1
local TOTAL_SIZE  = 2

channels = {}

local function add_channel(channel)
    local c = circular_buffer.new(rows, 2, sec_per_row, true)
    c:set_header(REQUESTS, "Requests")
    c:set_header(TOTAL_SIZE, "Total Size", "KiB")
    channels[channel] = c
    return c
end

all = add_channel("ALL")

function process_message ()
    local ts = read_message("Timestamp")
    if not all:add(ts, REQUESTS, 1) then return 0 end -- outside the buffer

    local rs = read_message("Fields[size]")
    if rs then
        rs = rs / 1024
    else
        rs = 0
    end
    all:add(ts, TOTAL_SIZE, rs)

    local url = read_message("Fields[url]")
    local channel = url:match("^/submit/telemetry/[^/]+/[^/]+/[^/]+/[^/]+/([^/]+)")
    if not channel then return 0 end
    if channel ~= "release" and channel ~= "beta" and channel ~= "aurora" and channel ~= "nightly" then
        channel = "other"
    end

    local c = channels[channel]
    if not c then
        c = add_channel(channel)
    end
    c:add(ts, REQUESTS, 1)
    c:add(ts, TOTAL_SIZE, rs)

    return 0
end

function timer_event(ns)
    for k, v in pairs(channels) do
        inject_payload("cbuf", k, v:format("cbuf"))
        inject_payload("cbufd", k, v:format("cbufd"))
    end
end
