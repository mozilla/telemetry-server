-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

require "circular_buffer"
require "string"

local rows = read_config("rows") or 1440
local sec_per_row = read_config("sec_per_row") or 60
local TOTAL_ERRORS = 1

errors = {}

local function add_error(name)
    local e = circular_buffer.new(rows, 1, sec_per_row, true)
    e:set_header(TOTAL_ERRORS, "Total Errors")
    errors[name] = e
    return e
end

local f = {type = 0, name = "", value = 0, representation = "", count = 0, key = ""}

function process_message ()
    local ts = read_message("Timestamp")
    while true do
        f.type, f.name, f.value, f.representation, f.count = read_next_field()
        if not f.type then break end

        local name = f.name:match("^bad_records\.(%S+)")
        if name then
            local e = errors[name]
            if not e then
                e = add_error(name)
            end
            if not e:add(ts, TOTAL_ERRORS, f.value) then break end -- outside the buffer
        end
    end

    return 0
end

function timer_event(ns)
    for k, v in pairs(errors) do
        inject_payload("cbuf", k, v:format("cbuf"))
        inject_payload("cbufd", k, v:format("cbufd"))
    end
end
