# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import os
import revision_cache
import shutil
import unittest
import json

class TestRevisionCache(unittest.TestCase):
    def setUp(self):
        test_dir = self.get_test_dir()
        assert not os.path.exists(test_dir)
        os.makedirs(test_dir)

    def tearDown(self):
        shutil.rmtree(self.get_test_dir())

    def get_test_dir(self):
        return "/tmp/histogram_revision_cache"

    def check_one_url(self, url, cache=None):
        #print "checking url", url
        if cache is None:
            dummy = revision_cache.RevisionCache(self.get_test_dir(), 'hg.mozilla.org')
        else:
            dummy = cache
        repo, rev = dummy.revision_url_to_parts(url)
        return self.check_one(repo, rev, cache)

    def check_one(self, repo, rev, cache=None):
        #print "checking repo:", repo, "rev:", rev
        if cache is None:
            rcache = revision_cache.RevisionCache(self.get_test_dir(), 'hg.mozilla.org')
        else:
            rcache = cache
        filename = "%s/%s/%s/Histograms.json" % (self.get_test_dir(), repo, rev)
        self.assertFalse(os.path.exists(filename))
        bad = rcache.fetch_disk(repo, rev)
        self.assertIs(bad, None)
        remote = rcache.fetch_server(repo, rev)
        self.assertIn("A11Y_INSTANTIATED_FLAG", remote)
        good = rcache.fetch_disk(repo, rev)
        self.assertIn("A11Y_INSTANTIATED_FLAG", good)
        self.assertTrue(os.path.exists(filename))
        return good

    def test_repo_rev(self):
        self.check_one('mozilla-central', '26cb30a532a1')
        self.check_one('releases/mozilla-aurora',  'a4de5411f118')
        self.check_one('releases/mozilla-beta',    '53c447ff5fd3')
        self.check_one('releases/mozilla-release', '0488055e9f9f')

    def test_urls(self):
        self.check_one_url('https://hg.mozilla.org/releases/mozilla-release/rev/e55e45536133')
        self.check_one_url('https://hg.mozilla.org/releases/mozilla-beta/rev/abf43438122e')
        self.check_one_url('https://hg.mozilla.org/releases/mozilla-aurora/rev/98feaf977ea2')
        self.check_one_url('https://hg.mozilla.org/mozilla-central/rev/e42dce3209da')
        self.check_one_url('https://hg.mozilla.org/projects/date/rev/614052b6cbcc')
        # Try urls have a limited shelf life... we'd have to fetch a fresh one to test.
        #self.check_one_url('https://hg.mozilla.org/try/rev/1c2a29db9d88')
        #self.check_one_url('https://hg.mozilla.org/try/rev/c83206ac7356')
        self.check_one_url('https://hg.mozilla.org/integration/b2g-inbound/rev/9b6f43676952')
        self.check_one_url('https://hg.mozilla.org/projects/elm/rev/ff718cdbd54b')
        self.check_one_url('https://hg.mozilla.org/projects/ux/rev/b7d620677157')
        self.check_one_url('https://hg.mozilla.org/integration/fx-team/rev/dadec41b7cbc')
        self.check_one_url('https://hg.mozilla.org/integration/mozilla-inbound/rev/85cad21c5d48')
        self.check_one_url('https://hg.mozilla.org/releases/mozilla-esr24/rev/bba10d0ca256')
        self.check_one_url('https://hg.mozilla.org/mozilla-central/rev/ecf20a2484b6')

    def test_revision_url_to_parts(self):
        rcache = revision_cache.RevisionCache(self.get_test_dir(), 'hg.mozilla.org')
        repo, rev = rcache.revision_url_to_parts("https://hg.mozilla.org/releases/mozilla-release/rev/e55e45536133")
        self.assertEquals(repo, "releases/mozilla-release")
        self.assertEquals(rev, "e55e45536133")

    def test_bad_revision_url_to_parts(self):
        rcache = revision_cache.RevisionCache(self.get_test_dir(), 'hg.mozilla.org')
        with self.assertRaises(ValueError):
            repo, rev = rcache.revision_url_to_parts("arglebargle")

    def test_get_revision(self):
        rcache = revision_cache.RevisionCache(self.get_test_dir(), 'hg.mozilla.org')

        # Check one that's pre-cached:
        pc_repo = 'releases/mozilla-release'
        pc_rev = '0488055e9f9f'
        ref = self.check_one(pc_repo, pc_rev, rcache)
        self.assertNotIn(pc_repo, rcache._repos)

        revision = rcache.get_revision(pc_repo, pc_rev)
        self.assertEqual(revision, ref)
        self.assertIn(pc_repo, rcache._repos)
        self.assertIn(pc_rev, rcache._repos[pc_repo])
        # This time it should be cached:
        revision = rcache.get_revision(pc_repo, pc_rev)
        self.assertEqual(revision, ref)

        # Fetch another one that's not cached
        nc_repo = 'releases/mozilla-beta'
        nc_rev = '53c447ff5fd3'
        self.assertNotIn(nc_repo, rcache._repos)
        revision = rcache.get_revision(nc_repo, nc_rev)
        self.assertIn("A11Y_INSTANTIATED_FLAG", revision)
        self.assertIn(nc_repo, rcache._repos)
        self.assertIn(nc_rev, rcache._repos[nc_repo])

    def test_get_revision_noparse(self):
        rcache = revision_cache.RevisionCache(self.get_test_dir(), 'hg.mozilla.org')
        # Fetch one that's not cached
        repo = 'releases/mozilla-beta'
        rev = '53c447ff5fd3'
        self.assertNotIn(repo, rcache._repos)
        revision = rcache.get_revision(repo, rev, parse=False)
        self.assertIn("A11Y_INSTANTIATED_FLAG", revision)
        self.assertIn(repo, rcache._repos)
        self.assertIn(rev, rcache._repos[repo])

        # parse=False should give us a raw string. Parse it as json and then
        # make sure the same key is present.
        parsed = json.loads(revision)
        self.assertIn("A11Y_INSTANTIATED_FLAG", parsed)

if __name__ == "__main__":
    unittest.main()
