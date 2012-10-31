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
from google.appengine.api import search

from mapreduce import base_handler

from lakshmi import pipelines
from lakshmi import configuration
from lakshmi.datum import CrawlDbDatum

ENTITY_KIND = "lakshmi.datum.CrawlDbDatum"
#specified some urls
ROOT_URLS = ["http://www.python.org/"]


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
          "text/html": "main._htmlOutlinkParser",
          "application/rss+xml": "main._htmlOutlinkParser",
          "application/atom+xml": "main._htmlOutlinkParser",
          "text/xml": "main._htmlOutlinkParser"
        },
        shards=8)
    pipeline.start()
    path = pipeline.base_path + "/status?root=" + pipeline.pipeline_id
    self.redirect(path)

class DeleteDatumHandler(webapp.RequestHandler):
  def get(self):
    self.response.headers['Content-Type'] = 'text/plain'
    kind_name = self.request.get("kind", "")
    if len(kind_name)>0:
      try:
        while True:
          q = ndb.gql("SELECT __key__ FROM %s"%kind_name)
          assert q.count()
          ndb.delete_multi(q.fetch(200))
          time.sleep(0.5)
      except Exception, e:
        self.response.out.write(repr(e)+'\n')
        pass
    else:
      self.response.out.write("Not specified kind_name. Usage:/delete_all_data?kind=your_kind_name")

class ScorePipeline(base_handler.PipelineBase):
  def run(self, entity_type):
    output = yield pipelines.PageScorePipeline("PageScorePipeline",
        params={
          "entity_kind": entity_type
        },
        shards=8)
    yield RemoveIndex(output)
    
class RemoveIndex(base_handler.PipelineBase):
  def run(self, output):
    #Remove search index
    for index in search.list_indexes(fetch_schema=True):
      doc_index = search.Index(name=index.name)

      while True:
        # Get a list of documents populating only the doc_id field and extract the ids.
        document_ids = [document.doc_id for document in doc_index.list_documents(ids_only=True)]
        if not document_ids:
          break
        # Remove the documents for the given ids from the Index.
        doc_index.remove(document_ids)
    
class ScoreHandler(webapp.RequestHandler):
  def get(self):
    pipeline = ScorePipeline(ENTITY_KIND)
    pipeline.start()
    path = pipeline.base_path + "/status?root=" + pipeline.pipeline_id
    self.redirect(path)

class ReFetch(webapp.RequestHandler):
  def get(self):
    pipeline = pipelines.FetcherPipeline("FetcherPipeline",
        params={
          "entity_kind": ENTITY_KIND
        },
        parser_params=None,
        need_extract=False,
        shards=8)
    pipeline.start()
    path = pipeline.base_path + "/status?root=" + pipeline.pipeline_id
    self.redirect(path)

score_config_yaml = configuration.ScoreConfigYaml.create_default_config()

class CleanHandler(webapp.RequestHandler):
  def get(self):
    self.response.headers['Content-Type'] = 'text/plain'
    crawl_kind_name = self.request.get("crawldb_kind", "")
    fetched_kind_name = self.request.get("fetched_kind", "")
    if len(crawl_kind_name)>0 and len(fetched_kind_name)>0:
      adopt_score = score_config_yaml.score_config.adopt_score
      try:
        while True:
          target_status_list = [1,3]
          tmp_query = "SELECT __key__ FROM %s WHERE page_score <= :1 AND last_status IN:2"%crawl_kind_name
          q = ndb.gql(tmp_query, float(adopt_score), target_status_list)
          if not q.count():
            break
          ndb.delete_multi(q.fetch(200))
          time.sleep(0.5)

        path = "/delete_all_data?kind=%s"%fetched_kind_name
        self.redirect(path)
      except Exception, e:
        self.response.out.write(repr(e)+'\n')
        pass
    else:
      self.response.out.write("Not specified kind_name. Usage:/clean?crawldb_kind=kind_name&fetched_kind=kind_name")

application = webapp.WSGIApplication(
                                     [("/start", FetchStart),
                                     ("/add_data", AddRootUrlsHandler),
                                     ("/delete_all_data", DeleteDatumHandler),
                                     ("/score", ScoreHandler),
                                     ("/refetch", ReFetch),
                                     ("/clean", CleanHandler)],
                                     debug=True)
                                     
def main():
  run_wsgi_app(application)

if __name__ == "__main__":
  main()
