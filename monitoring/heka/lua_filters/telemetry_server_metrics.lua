-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

require "circular_buffer"

local rows        = 1440
local sec_per_row = 60

request = circular_buffer.new(rows, 4, sec_per_row, true)
local SUCCESS       = request:set_header(1, "Success"     , "count")
local FAILURE       = request:set_header(2, "Failure"     , "count")
local REQUEST_SIZE  = request:set_header(3, "Request Size", "B")
local REQUEST_TIME  = request:set_header(4, "Request Time", "ms")

function process_message ()
    local ts = read_message("Timestamp")
    if not request:add(ts, REQUEST_TIME, read_message("Fields[duration]")) then
        return 0 -- outside the buffer
    end

    request:add(ts, REQUEST_SIZE, read_message("Fields[size]"))

    if sc == read_message("Fields[code]") then
        request:add(ts, SUCCESS, 1)
    else
        request:add(ts, FAILURE, 1)
    end

    return 0
end

function timer_event(ns)
    local title = "Request Statistics"
    inject_message(request:format("cbuf"), title)
    inject_message(request:format("cbufd"), title)
end
