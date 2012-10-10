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

import os
import unittest
import time

from google.appengine.ext import ndb
from google.appengine.api import apiproxy_stub
from mapreduce.lib import pipeline
from mapreduce import test_support
from testlib import testutil
from lakshmi import pipelines
from lakshmi.datum import CrawlDbDatum
from lakshmi.datum import FetchedDatum

def createMockCrawlDbDatum(url):
    """Create CrawlDbDatum mock data."""
    data = CrawlDbDatum(
        parent =ndb.Key(CrawlDbDatum, url),
        url=url,
        last_status=pipelines.UNFETCHED,
        crawl_depth=0)
    data.put()

class URLFetchServiceMockForUrl(apiproxy_stub.APIProxyStub):
  """Mock for google.appengine.api.urlfetch."""
  def __init__(self, service_name="urlfetch"):
    super(URLFetchServiceMockForUrl, self).__init__(service_name)
    self._return_values_dict = {}
 
  def set_return_values(self, return_value):
    url = return_value.get("url")
    print("specifiedURL" + url)
    self._return_values_dict[url] = return_value
    self._redirect_url = return_value.get("final_url")
 
  def _Dynamic_Fetch(self, request, response): 
    return_value_key = request.url()
    return_values = self._return_values_dict.get(return_value_key, "")
    response.set_content(return_values.get("content", ""))
    response.set_statuscode(return_values.get("status_code", 200))
    #Test for Accept-Language.
    language_contents = return_values.get("language_content")
    if language_contents:
      for header in request.header_list():
        if header.key().title().lower() == "accept-language":
          accept_language = header.value()
      for lang in language_contents.keys():
        if accept_language.find(lang) >= 0:
          response.set_content(language_contents[lang])
    
    for header_key, header_value in return_values.get("headers", {}).items():
      new_header = response.add_header()
      new_header.set_key(header_key)
      new_header.set_value(header_value)
    #Simulation of the redirect, if set final_url.
    if request.followredirects() and self._redirect_url is not None:
      response.set_finalurl(self._redirect_url)
    else:
      response.set_finalurl(request.url())
    response.set_contentwastruncated(return_values.get("content_was_truncated", False))
    if return_values.get("duration"):
      time.sleep(long(return_values.get("duration")))
    self.request = request
    self.response = response

class FetchPipelineEndtoEndTest(testutil.HandlerTestBase):
  """Test for FetchPipeline"""

  def setUp(self):
    testutil.HandlerTestBase.setUp(self, urlfetch_mock=URLFetchServiceMockForUrl())
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []
  
  def _send_mail(self, sender, subject, body, html=None):
    """Callback function for sending mail."""
    self.emails.append((sender, subject, body, html))

  def getResource(self, file_name):
    """ to get contents from resource"""
    path = os.path.join(os.path.dirname(__file__), "resource", file_name)
    return open(path)

  def testFetchEndToEnd(self):
    """Test for through of fetcher job"""
    createMockCrawlDbDatum("http://foo.com/bar.html")
    static_robots = "User-agent: test\nDisallow: /content_0\nDisallow: /content_1\nDisallow: /content_3"
    self.setReturnValue(url="http://foo.com/robots.txt",
        content=static_robots,
        headers={"Content-Length": len(static_robots)})
    #static resource is read from resource
    resource = self.getResource("karlie.html")
    static_content = resource.read()
    static_content_length = len(static_content)
    self.setReturnValue(url="http://foo.com/bar.html",
        content=static_content,
        headers={"Content-Length": static_content_length,
            "Content-Type": "text/html"})

    p = pipelines.FetcherPipeline("FetcherPipeline",
        params={
          "entity_kind": "lakshmi.datum.CrawlDbDatum"
        },
        shards=2)
    p.start()
    test_support.execute_until_empty(self.taskqueue)
    
    entities = CrawlDbDatum.fetch_crawl_db(ndb.Key(CrawlDbDatum, "http://foo.com/bar.html"))
    entity = entities[0]
    fetched_datums = FetchedDatum.fetch_fetched_datum(entity.key)
    fetched_datum = fetched_datums[0]
    self.assertTrue(fetched_datum!=None)
    qry = CrawlDbDatum.query(CrawlDbDatum.last_status == pipelines.UNFETCHED)
    crawl_db_datums = qry.fetch()
    self.assertTrue(len(crawl_db_datums)>0)
    for crawl_db_datum in crawl_db_datums:
      self.assertEquals(1, crawl_db_datum.crawl_depth)

if __name__ == "__main__":
  unittest.main()
