-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

local rows        = 1440
local sec_per_row = 60

request = circular_buffer.new(rows, 4, sec_per_row, true)
local SUCCESS           = request:set_header(1, "Success"     , "count")
local FAILURE           = request:set_header(2, "Failure"     , "count")
local AVG_REQUEST_SIZE  = request:set_header(3, "Request Size", "B" , "avg")
local AVG_REQUEST_TIME  = request:set_header(4, "Request Time", "ms", "avg")

sums = circular_buffer.new(rows, 3, sec_per_row)
local REQUESTS     = sums:set_header(1, "Requests"    , "count")
local REQUEST_SIZE = sums:set_header(2, "Request Size", "B")
local REQUEST_TIME = sums:set_header(3, "Request Time", "ms")

function process_message ()
    local ts = read_message("Timestamp")
    local sc = tonumber(read_message("Fields[code]"))
    local rd = tonumber(read_message("Fields[duration]"))
    local rs = tonumber(read_message("Fields[size]"))

    local cnt = sums:add(ts, REQUESTS, 1)
    if not cnt or not sc then return 0 end -- outside the buffer, invalid record

    local t = sums:add(ts, REQUEST_SIZE, rs)
    request:set(ts, AVG_REQUEST_SIZE, t/cnt)
    t = sums:add(ts, REQUEST_TIME, rd)
    request:set(ts, AVG_REQUEST_TIME, t/cnt)

    if sc == 200 then
        request:add(ts, SUCCESS, 1)
    else
        request:add(ts, FAILURE, 1)
    end

    return 0
end

function timer_event(ns)
    -- advance the buffers so the graphs will continue to advance without new data
    -- request:add(ns, 1, 0) 
    -- sums:add(ns, 1, 0)

    local title = "Request Statistics"
    inject_message(request:format("cbuf"), title)
    inject_message(request:format("cbufd"), title)
end
