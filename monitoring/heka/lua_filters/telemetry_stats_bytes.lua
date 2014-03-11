-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

require "circular_buffer"
require "string"

local title = "Throughput"
local rows = read_config("rows") or 1440
local sec_per_row = read_config("sec_per_row") or 60
local BYTES_READ            = 1
local BYTES_WRITTEN         = 2
local BYTES_UNCOMPRESSED    = 3

bytes = circular_buffer.new(rows, 3, sec_per_row, true)
bytes:set_header(BYTES_READ         , "Bytes Read"          , "B")
bytes:set_header(BYTES_WRITTEN      , "Bytes Written"       , "B")
bytes:set_header(BYTES_UNCOMPRESSED , "Bytes Uncompressed"  , "B")

function process_message ()
    local ts = read_message("Timestamp")
    if not bytes:add(ts, BYTES_READ, read_message("Fields[bytes_read]")) then
        return 0 -- outside the buffer
    end

    bytes:add(ts, BYTES_WRITTEN, read_message("Fields[bytes_written]"))
    bytes:add(ts, BYTES_UNCOMPRESSED, read_message("Fields[bytes_uncompressed]"))

    return 0
end

function timer_event(ns)
    inject_message(bytes:format("cbuf"), title)
    inject_message(bytes:format("cbufd"), title)
end
