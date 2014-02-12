# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

try:
    import simplejson as json
except ImportError:
    import json
import sys
import os
import re
import urllib2
import telemetry.util.files as fu

# TODO:
# [ ] Pre-fetch (and cache) all revisions of Histograms.json using something like:
#      http://hg.mozilla.org/mozilla-central/log/tip/toolkit/components/telemetry/Histograms.json
#      http://hg.mozilla.org/releases/mozilla-aurora/log/tip/toolkit/components/telemetry/Histograms.json
#      http://hg.mozilla.org/releases/mozilla-beta/log/tip/toolkit/components/telemetry/Histograms.json
#      http://hg.mozilla.org/releases/mozilla-release/log/tip/toolkit/components/telemetry/Histograms.json
#     then link other repository revisions to the relevant Histograms.json revision.
class RevisionCache:
    """A class for fetching and caching revisions of a file in mercurial"""

    def __init__(self, cache_dir, server):
        self._cache_dir = cache_dir
        self._server = server
        self._repos = dict()
        self._hist_filename = "Histograms.json"
        self._hist_filepath = "toolkit/components/telemetry/" + self._hist_filename
        self._valid_revisions = re.compile('^(http[s]?://[^/]+)/(.+)/rev/([0-9a-f]+)/?$')

    # TODO:
    #  [ ] deal with 'tip' and other named revisions / tags (fetch from source
    #      with no local cache?)
    def get_revision(self, repo, revision, parse=True):
        if repo not in self._repos:
            self._repos[repo] = dict()

        cached_repo = self._repos[repo]

        cached_revision = None
        if revision not in cached_repo:
            # Fetch it from disk cache
            cached_revision = self.fetch_disk(repo, revision, parse)
            if cached_revision:
                cached_repo[revision] = cached_revision
            else:
                # Fetch it from the server
                cached_revision = self.fetch_server(repo, revision, parse)
                if cached_revision:
                    cached_repo[revision] = cached_revision
        else:
            cached_revision = cached_repo[revision]
        return cached_revision

    # Returns (repository name, revision)
    def revision_url_to_parts(self, revision_url):
        m = self._valid_revisions.match(revision_url)
        if m:
            #sys.stderr.write("Matched\n")
            return (m.group(2), m.group(3))
        else:
            #sys.stderr.write("Did not Match: %s\n" % revision_url)
            raise ValueError("Invalid revision URL: %s" % revision_url)
        #return (None, None)

    def get_histograms_for_revision(self, revision_url, parse=True):
        # revision_url is like
        #    http://hg.mozilla.org/releases/mozilla-aurora/rev/089956e907ed
        # and path should be like
        #    toolkit/components/telemetry/Histograms.json
        # to produce a full URL like
        #    http://hg.mozilla.org/releases/mozilla-aurora/raw-file/089956e907ed/toolkit/components/telemetry/Histograms.json
        repo, revision = self.revision_url_to_parts(revision_url)
        return self.get_revision(repo, revision, parse)

    def fetch_disk(self, repo, revision, parse=True):
        filename = os.path.join(self._cache_dir, repo, revision, self._hist_filename)
        histograms = None
        try:
            f = open(filename, "r")
            if parse:
                histograms = json.load(f)
                # TODO: validate the resulting obj.
            else:
                histograms = f.read()
        except:
            # TODO: log an info / debug message
            #sys.stderr.write("INFO: failed to load '%s' from disk cache\n" % filename)
            pass
        return histograms

    def fetch_server(self, repo, revision, parse=True):
        url = '/'.join(('http:/', self._server, repo, 'raw-file', revision, self._hist_filepath))
        histograms = None
        try:
            response = urllib2.urlopen(url)
            histograms_json = response.read()
            # Bug 920169 - replace calculated values/constants with their
            #              actual values:
            histograms_json = histograms_json.replace('"JS::gcreason::NUM_TELEMETRY_REASONS"', "101")
            histograms_json = histograms_json.replace('"mozilla::StartupTimeline::MAX_EVENT_ID"', "12")
            histograms_json = histograms_json.replace('"80 + 1"', "81")
            if parse:
                histograms = json.loads(histograms_json)
                # TODO: validate the resulting obj.
            else:
                histograms = histograms_json
            self.save_to_cache(repo, revision, histograms_json)
        except:
            # TODO: better error handling
            # TODO: cache 404s so we don't keep trying them
            sys.stderr.write("INFO: failed to load '%s' from server\n" % url)
        return histograms

    def save_to_cache(self, repo, revision, contents):
        filename = os.path.join(self._cache_dir, repo, revision, "Histograms.json")
        try:
            fout = open(filename, 'w')
        except IOError:
            fu.makedirs_concurrent(os.path.dirname(filename))
            fout = open(filename, 'w')
        fout.write(contents)
        fout.close()
