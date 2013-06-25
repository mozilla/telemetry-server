"""                                                                             
This Source Code Form is subject to the terms of the Mozilla Public             
License, v. 2.0. If a copy of the MPL was not distributed with this             
file, You can obtain one at http://mozilla.org/MPL/2.0/.                        
"""

import os
import revision_cache
import shutil

cache_dir = "/tmp/histogram_revision_cache"
assert not os.path.exists(cache_dir)

def check_one(repo, rev):
    rcache = revision_cache.RevisionCache(cache_dir, 'hg.mozilla.org')
    filename = "%s/%s/%s/Histograms.json" % (cache_dir, repo, rev)
    assert not os.path.exists(filename)
    bad = rcache.fetch_disk(repo, rev)
    assert bad is None
    remote = rcache.fetch_server(repo, rev)
    assert "A11Y_INSTANTIATED_FLAG" in remote
    good = rcache.fetch_disk(repo, rev)
    assert "A11Y_INSTANTIATED_FLAG" in good
    assert os.path.exists(filename)

check_one('mozilla-central', '26cb30a532a1')
check_one('mozilla-aurora',  'a4de5411f118')
check_one('mozilla-beta',    '53c447ff5fd3')
check_one('mozilla-release', '0488055e9f9f')

shutil.rmtree(cache_dir)
