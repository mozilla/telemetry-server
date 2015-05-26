#!/bin/bash
# FIXME: external dependencies caused histogram_tools.py to fail on 'tip'
#        after bug 968923. Fetching a specific revision temporarily.
#wget -c http://hg.mozilla.org/mozilla-central/raw-file/tip/toolkit/components/telemetry/histogram_tools.py -O histogram_tools.py
wget -c http://hg.mozilla.org/mozilla-central/raw-file/72940b27aeaa/toolkit/components/telemetry/histogram_tools.py -O histogram_tools.py
