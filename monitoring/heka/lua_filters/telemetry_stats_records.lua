-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

require "circular_buffer"
require "string"

local rows = read_config("rows") or 1440
local sec_per_row = read_config("sec_per_row") or 60
local RECORDS_READ  = 1
local BAD_RECORDS   = 2

loggers = {}

local function add_channel(logger, channel)
    local c = circular_buffer.new(rows, 2, sec_per_row, true)
    c:set_header(RECORDS_READ   , "Records Read")
    c:set_header(BAD_RECORDS    , "Bad Records")
    logger[channel] = c
    return c
end

function process_message ()
    local logger = read_message("Logger")
    local l = loggers[logger]
    if not l then
        l = {}
        loggers[logger] = l
    end

    local channel = read_message("Fields[channel]")
    local c = l[channel]
    if not c then
        c = add_channel(l, channel)
    end

    local ts = read_message("Timestamp")
    if not c:add(ts, RECORDS_READ, read_message("Fields[records_read]")) then
        return 0 -- outside the buffer
    end

    c:add(ts, BAD_RECORDS, read_message("Fields[bad_records]"))

    return 0
end

function timer_event(ns)
    for k, v in pairs(loggers) do
        for m, n in pairs(v) do
            local title = string.format("%s.%s", k, m)
            inject_payload("cbuf", title, n:format("cbuf"))
            inject_payload("cbufd", title, n:format("cbufd"))
        end
    end
end
