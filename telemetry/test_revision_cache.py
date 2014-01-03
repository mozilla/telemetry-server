# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import os
import revision_cache
import shutil

cache_dir = "/tmp/histogram_revision_cache"
assert not os.path.exists(cache_dir)

def check_one_url(url):
    print "checking url", url
    dummy = revision_cache.RevisionCache(cache_dir, 'hg.mozilla.org')
    repo, rev = dummy.revision_url_to_parts(url)
    return check_one(repo, rev)

def check_one(repo, rev):
    print "checking repo:", repo, "rev:", rev
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

try:
    check_one('mozilla-central', '26cb30a532a1')
    check_one('releases/mozilla-aurora',  'a4de5411f118')
    check_one('releases/mozilla-beta',    '53c447ff5fd3')
    check_one('releases/mozilla-release', '0488055e9f9f')

    check_one_url('http://hg.mozilla.org/releases/mozilla-release/rev/e55e45536133')
    check_one_url('http://hg.mozilla.org/releases/mozilla-beta/rev/abf43438122e')
    check_one_url('http://hg.mozilla.org/releases/mozilla-aurora/rev/98feaf977ea2')
    check_one_url('http://hg.mozilla.org/mozilla-central/rev/e42dce3209da')
    check_one_url('http://hg.mozilla.org/projects/date/rev/614052b6cbcc')
    check_one_url('http://hg.mozilla.org/try/rev/1c2a29db9d88')
    check_one_url('http://hg.mozilla.org/integration/b2g-inbound/rev/9b6f43676952')
    check_one_url('http://hg.mozilla.org/projects/elm/rev/ff718cdbd54b')
    check_one_url('http://hg.mozilla.org/projects/ux/rev/b7d620677157')
    check_one_url('http://hg.mozilla.org/integration/fx-team/rev/dadec41b7cbc')
    check_one_url('http://hg.mozilla.org/integration/mozilla-inbound/rev/85cad21c5d48')
    check_one_url('http://hg.mozilla.org/releases/mozilla-esr24/rev/bba10d0ca256')
finally:
    shutil.rmtree(cache_dir)
