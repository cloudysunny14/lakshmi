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

import re
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.ext import ndb
from google.appengine.ext import webapp
from google.appengine.api import search

from mapreduce import base_handler

from lakshmi import pipelines
from lakshmi import configuration
from lakshmi.datum import CrawlDbDatum
from lakshmi.cooperate import cooperate

ENTITY_KIND = "lakshmi.datum.CrawlDbDatum"
#specified some urls
ROOT_URLS = ["http://www.python.org/"]

#Create Datum Handlers
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

#lakshmi pipeline Handlers
import random

def _htmlOutlinkParser(content):
  "htmlOutlinkParser for ramdam extracting"
  link_list = re.findall(r'href=[\'"]?([^\'" >]+)', content)
  while link_list:
    if link_list != []:
      index = random.randint(0, len(link_list) - 1)
      elem = link_list[index]
      link_list[index] = link_list[-1]
      del link_list[-1]
      yield elem
    else:
      yield link_list

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
        shards=16)
    pipeline.start()
    path = pipeline.base_path + "/status?root=" + pipeline.pipeline_id
    self.redirect(path)

class DeleteDatumHandler(webapp.RequestHandler):
  def get(self):
    pipeline = pipelines.CleanDatumPipeline("CleanAllDatumPipeline",
        params={
          "entity_kind": ENTITY_KIND
        },
        clean_all=True,
        shards=8)
    pipeline.start()
    path = pipeline.base_path + "/status?root=" + pipeline.pipeline_id
    self.redirect(path) 

class ScorePipeline(base_handler.PipelineBase):
  def run(self, entity_type):
    output = yield pipelines.PageScorePipeline("PageScorePipeline",
        params={
          "entity_kind": entity_type
        },
        shards=16)
    yield RemoveIndex(output)
    
class RemoveIndex(base_handler.PipelineBase):
  def run(self, output):
    #Remove search index
    for index in search.get_indexes(fetch_schema=True):
      doc_index = search.Index(name=index.name)

      while True:
        # Get a list of documents populating only the doc_id field and extract the ids.
        document_ids = [document.doc_id for document in doc_index.get_range(ids_only=True)]
        if not document_ids:
          break
        # Remove the documents for the given ids from the Index.
        doc_index.remove(document_ids)

class RemoveDoc(webapp.RequestHandler):
  def get(self):
    pipeline = RemoveIndex(None)
    pipeline.start()
    path = pipeline.base_path + "/status?root=" + pipeline.pipeline_id
    self.redirect(path)

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
        shards=16)
    pipeline.start()
    path = pipeline.base_path + "/status?root=" + pipeline.pipeline_id
    self.redirect(path)

score_config_yaml = configuration.ScoreConfigYaml.create_default_config()

class CleanHandler(webapp.RequestHandler):
  def get(self):
    pipeline = pipelines.CleanDatumPipeline("CleanDatumPipeline",
        params={
          "entity_kind": ENTITY_KIND
        },
        shards=8)
    pipeline.start()
    path = pipeline.base_path + "/status?root=" + pipeline.pipeline_id
    self.redirect(path)

#Cooperate Handlers
FETCHED_DATUM_ENTITY_KIND = "lakshmi.datum.FetchedDatum"
GS_BUCKET = "your_bucket_name"

class ExportHandler(webapp.RequestHandler):
  def get(self):
    pipeline = cooperate.ExportCloudStoragePipeline("ExportCloudStoragePipeline",
        input_entity_kind="lakshmi.datum.FetchedDatum",
        gs_bucket_name=GS_BUCKET,
        shards=3)
    pipeline.start()
    path = pipeline.base_path + "/status?root=" + pipeline.pipeline_id
    self.redirect(path)

application = webapp.WSGIApplication(
                                     [("/start", FetchStart),
                                     ("/add_data", AddRootUrlsHandler),
                                     ("/clean_all", DeleteDatumHandler),
                                     ("/score", ScoreHandler),
                                     ("/refetch", ReFetch),
                                     ("/clean", CleanHandler),
                                     ("/remove_doc", RemoveDoc),
                                     ("/export", ExportHandler)],
                                     debug=True)
                                     
def main():
  run_wsgi_app(application)

if __name__ == "__main__":
  main()
