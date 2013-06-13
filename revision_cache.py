import json
import sys
import os
import urllib2
import gzip

class RevisionCache:
   """A class for fetching and caching revisions of a file in mercurial"""

   def __init__(self, cache_dir, server):
      self._cache_dir = cache_dir
      self._server = server
      self._repos = dict()
      self._hist_filename = "Histograms.json"
      self._hist_filepath = "toolkit/components/telemetry/" + self._hist_filename

   def get_revision(self, repo, revision):
      if repo not in self._repos:
         self._repos[repo] = dict()

      cached_repo = self._repos[repo]

      cached_revision = None
      if revision not in cached_repo:
         # Fetch it from disk cache
         cached_revision = self.fetch_disk(repo, revision)
         if cached_revision:
            cached_repo[revision] = cached_revision
         else:
            # Fetch it from the server
            cached_revision = self.fetch_server(repo, revision)
            if cached_revision:
               cached_repo[revision] = cached_revision
      else:
         cached_revision = cached_repo[revision]
      return cached_revision

   def fetch_disk(self, repo, revision):
      filename = os.path.join(self._cache_dir, repo, revision, self._hist_filename)
      histograms = None
      try:
         f = open(filename, "r")
         histograms = json.load(f)
         # TODO: validate the resulting obj.
      except:
         # TODO: log an info / debug message
         sys.stderr.write("INFO: failed to load '%s' from disk cache\n" % filename)
      return histograms

   def fetch_server(self, repo, revision):
      url = '/'.join(('http:/', self._server, self.repo_to_path(repo), 'raw-file', revision, self._hist_filepath))
      histograms = None
      try:
         response = urllib2.urlopen(url)
         histograms_json = response.read()
         histograms = json.loads(histograms_json)
         # TODO: validate the resulting obj.
         self.save_to_cache(repo, revision, histograms_json)
      except:
         # TODO: better error handling
         sys.stderr.write("INFO: failed to load '%s' from server\n" % url)
      return histograms

   def repo_to_path(self, repo):
      if repo != "mozilla-central":
         return '/'.join(('releases', repo))
      return repo

   def save_to_cache(self, repo, revision, contents):
      filename = os.path.join(self._cache_dir, repo, revision, "Histograms.json")
      try:
         fout = open(filename, 'w')
      except IOError:
         os.makedirs(os.path.dirname(filename))
         fout = open(filename, 'w')
      fout.write(contents)
      fout.close()
