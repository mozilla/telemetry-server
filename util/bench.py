#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import time
from contextlib import contextmanager
@contextmanager
def bench(label):
    start = time.clock()
    try:
        yield
    finally:
        duration = time.clock() - start
        print label, "Elapsed time:", duration, "seconds"

import re
from string import maketrans
x = "hello"
trantab = maketrans("\r\n", "  ")
eols = re.compile('[\r\n]')
input2 = "FFFFFFFFFFFFFFFFFFFFFFFF   FFFFF" * 3000
input = "FFFFFFFF\rFFFFFFF\nFF  FFF   \r\r\n\n" * 3000

with bench("Translate (with eols)"):
    for i in range(10000):
        if "\r" in input or "\n" in input:
            x = input.translate(trantab)

with bench("Replace (with eols)"):
    for i in range(10000):
        if "\r" in input or "\n" in input:
            x = input.replace("\r", " ").replace("\n", " ")

with bench("Regex (with eols)"):
    for i in range(10000):
        if "\r" in input or "\n" in input:
            x, count = eols.subn(" ", input)

with bench("Translate (no eols)"):
    for i in range(10000):
        if "\r" in input2 or "\n" in input2:
            x = input2.translate(trantab)

with bench("Replace (no eols)"):
    for i in range(10000):
        if "\r" in input2 or "\n" in input2:
            x = input2.replace("\r", " ").replace("\n", " ")

with bench("Regex (no eols)"):
    for i in range(10000):
        if "\r" in input2 or "\n" in input2:
            x, count = eols.subn(" ", input2)
