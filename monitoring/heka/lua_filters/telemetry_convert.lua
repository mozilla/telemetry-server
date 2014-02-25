-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

require "circular_buffer"
require "string"
require "table"

metrics = {}

local rows = read_config("rows") or 1440
local sec_per_row = read_config("sec_per_row") or 60

local function discover_fields()
    local t = {fields = {}, cbuf = nil}
    local h = {type = 0, name = "", value = 0, representation = "", count = 0, key = ""}
    while true do
        h.type, h.name, h.value, h.representation, h.count = read_next_field()
        if not h.type then break end
        h.key = string.format("Fields[%s]", h.name)
        table.insert(t.fields, h)
    end

    local cnt = #t.fields
    if cnt > 0 then
        t.cbuf = circular_buffer.new(rows, cnt, sec_per_row)
        for i, v in ipairs(t.fields) do
            if v.representation ~= "" then
                t.cbuf:set_header(i, v.name, v.representation)
            else
                t.cbuf:set_header(i, v.name)
            end
        end
        return t
    end
    return nil
end


function process_message ()
    local logger = read_message("Logger")
    local l = metrics[logger]
    if not l then
        l = discover_fields()
        if not l then
            return -1
        end
        metrics[logger] = l
    end

    local ts = read_message("Timestamp")
    for i, v in ipairs(l.fields) do
        local value = read_message(v.key)
        if value then
            if not l.cbuf:add(ts, i, value) then
                break
            end
        end
    end
    return 0
end


function timer_event(ns)
    for k, v in pairs(metrics) do
        inject_message(v.cbuf, k)
    end
end
