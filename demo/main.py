#!/usr/bin/env python
#
# Copyright 2012 cloudysunny14.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" This is a sample application that Lakshmi an web crawler.
"""

__author__ = """cloudysunny14@gmail.com (Kiyonari Harigae)"""

import time
import re

from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.ext import ndb
from google.appengine.ext import webapp

from lakshmi import pipelines
from lakshmi.datum import CrawlDbDatum

ENTITY_KIND = "lakshmi.datum.CrawlDbDatum"
#specified some urls
ROOT_URLS = ["http://cnn.com/", "http://cloudysunny14.blogspot.jp/"]


class AddRootUrlsHandler(webapp.RequestHandler):
  def get(self):
    self.response.headers['Content-Type'] = 'text/plain'
    insert_num = 0
    for url in ROOT_URLS:
      data = CrawlDbDatum(
          parent =ndb.Key(CrawlDbDatum, url),
          url=url,
          last_status=pipelines.UNFETCHED,
          crawl_depth=0)
      data.put()
      insert_num += 1

    self.response.out.write("SETTING %d ROOT URL IS SUCCESS"%insert_num)

def _htmlOutlinkParser(content):
  """htmlOutlinkParser for testing"""
  return re.findall(r'href=[\'"]?([^\'" >]+)', "".join(content))

class FetchStart(webapp.RequestHandler):
  def get(self):
    pipeline = pipelines.FetcherPipeline("FetcherPipeline",
        params={
          "entity_kind": ENTITY_KIND
        },
        parser_params={
          "text/html": "main._htmlOutlinkParser"
        },
        shards=8)
    pipeline.start()
    path = pipeline.base_path + "/status?root=" + pipeline.pipeline_id
    self.redirect(path)

class DeleteDatumHandler(webapp.RequestHandler):
  def get(self):
    self.response.headers['Content-Type'] = 'text/plain'
    kind_name = self.request.get("kind", "")
    try:
      while True:
        q = ndb.gql("SELECT __key__ FROM %s"%kind_name)
        assert q.count()
        ndb.delete_multi(q.fetch(200))
        time.sleep(0.5)
    except Exception, e:
      self.response.out.write(repr(e)+'\n')
      pass

class ScoreHandler(webapp.RequestHandler):
  def get(self):
    pipeline = pipelines.PageScorePipeline("PageScorePipeline",
        params={
          "entity_kind": "lakshmi.datum.CrawlDbDatum"
        },
        shards=8)
    pipeline.start()
    path = pipeline.base_path + "/status?root=" + pipeline.pipeline_id
    self.redirect(path)

application = webapp.WSGIApplication(
                                     [("/start", FetchStart),
                                     ("/add_data", AddRootUrlsHandler),
                                     ("/delete_all_data", DeleteDatumHandler),
                                     ("/score", ScoreHandler)],
                                     debug=True)
                                     
def main():
  run_wsgi_app(application)

if __name__ == "__main__":
  main()
