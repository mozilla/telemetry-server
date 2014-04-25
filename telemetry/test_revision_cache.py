# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import os
import revision_cache
import shutil
import unittest

class TestRevisionCache(unittest.TestCase):
    def setUp(self):
        test_dir = self.get_test_dir()
        assert not os.path.exists(test_dir)
        os.makedirs(test_dir)

    def tearDown(self):
        shutil.rmtree(self.get_test_dir())

    def get_test_dir(self):
        return "/tmp/histogram_revision_cache"

    def check_one_url(self, url):
        #print "checking url", url
        dummy = revision_cache.RevisionCache(self.get_test_dir(), 'hg.mozilla.org')
        repo, rev = dummy.revision_url_to_parts(url)
        return self.check_one(repo, rev)

    def check_one(self, repo, rev):
        #print "checking repo:", repo, "rev:", rev
        rcache = revision_cache.RevisionCache(self.get_test_dir(), 'hg.mozilla.org')
        filename = "%s/%s/%s/Histograms.json" % (self.get_test_dir(), repo, rev)
        self.assertFalse(os.path.exists(filename))
        bad = rcache.fetch_disk(repo, rev)
        self.assertIs(bad, None)
        remote = rcache.fetch_server(repo, rev)
        self.assertIn("A11Y_INSTANTIATED_FLAG", remote)
        good = rcache.fetch_disk(repo, rev)
        self.assertIn("A11Y_INSTANTIATED_FLAG", good)
        self.assertTrue(os.path.exists(filename))

    def test_repo_rev(self):
        self.check_one('mozilla-central', '26cb30a532a1')
        self.check_one('releases/mozilla-aurora',  'a4de5411f118')
        self.check_one('releases/mozilla-beta',    '53c447ff5fd3')
        self.check_one('releases/mozilla-release', '0488055e9f9f')

    def test_urls(self):
        self.check_one_url('http://hg.mozilla.org/releases/mozilla-release/rev/e55e45536133')
        self.check_one_url('http://hg.mozilla.org/releases/mozilla-beta/rev/abf43438122e')
        self.check_one_url('http://hg.mozilla.org/releases/mozilla-aurora/rev/98feaf977ea2')
        self.check_one_url('http://hg.mozilla.org/mozilla-central/rev/e42dce3209da')
        self.check_one_url('http://hg.mozilla.org/projects/date/rev/614052b6cbcc')
        # Try urls have a limited shelf life... we'd have to fetch a fresh one to test.
        #self.check_one_url('http://hg.mozilla.org/try/rev/1c2a29db9d88')
        #self.check_one_url('http://hg.mozilla.org/try/rev/c83206ac7356')
        self.check_one_url('http://hg.mozilla.org/integration/b2g-inbound/rev/9b6f43676952')
        self.check_one_url('http://hg.mozilla.org/projects/elm/rev/ff718cdbd54b')
        self.check_one_url('http://hg.mozilla.org/projects/ux/rev/b7d620677157')
        self.check_one_url('http://hg.mozilla.org/integration/fx-team/rev/dadec41b7cbc')
        self.check_one_url('http://hg.mozilla.org/integration/mozilla-inbound/rev/85cad21c5d48')
        self.check_one_url('http://hg.mozilla.org/releases/mozilla-esr24/rev/bba10d0ca256')
        self.check_one_url('https://hg.mozilla.org/mozilla-central/rev/ecf20a2484b6')

if __name__ == "__main__":
    unittest.main()
