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
import re

from google.appengine.api import apiproxy_stub
from mapreduce.lib import pipeline
from mapreduce import test_support
from testlib import testutil
from lakshmi import pipelines
from lakshmi.datum import CrawlDbDatum
from lakshmi.datum import FetchedDatum

def createMockCrawlDbDatum(url):
    """Create CrawlDbDatum mock data."""
    CrawlDbDatum.get_or_insert(url,
        url=url, last_status=pipelines.UNFETCHED, crawl_depth=0)
    
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

def _htmlOutlinkParser(content):
  "htmlOutlinkParser for testing"
  return re.findall(r'href=[\'"]?([^\'" >]+)', content)

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
    resource = self.getResource("cloudysunny14.html")
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
        parser_params={
          "text/html": __name__ + "._htmlOutlinkParser"
        },
        shards=2)
    p.start()
    test_support.execute_until_empty(self.taskqueue)
    
    entities = CrawlDbDatum.query(CrawlDbDatum.url=="http://foo.com/bar.html").fetch()
    entity = entities[0]
    fetched_datum = FetchedDatum.get_by_id(entity.url)
    self.assertTrue(fetched_datum is not None)
    qry = CrawlDbDatum.query(CrawlDbDatum.last_status == pipelines.UNFETCHED)
    crawl_db_datums = qry.fetch()
    self.assertEquals(10,len(crawl_db_datums))
    for crawl_db_datum in crawl_db_datums:
      self.assertEquals(1, crawl_db_datum.crawl_depth)

def _parserNotOutlinks(cotent):
  return None

class FetchPipelineWithSpecifiedParserTest(testutil.HandlerTestBase):
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
    createMockCrawlDbDatum("http://foo.com/bar.txt")
    static_robots = "User-agent: test\nDisallow: /content_0\nDisallow: /content_1\nDisallow: /content_3"
    self.setReturnValue(url="http://foo.com/robots.txt",
        content=static_robots,
        headers={"Content-Length": len(static_robots),
          "content-type": "text/plain"})
    
    static_content = "test"
    static_content_length = len(static_content)
    self.setReturnValue(url="http://foo.com/bar.txt",
        content=static_content,
        headers={"Content-Length": static_content_length,
            "Content-Type": "text/plain"})
    p = pipelines.FetcherPipeline("FetcherPipeline",
        params={
          "entity_kind": "lakshmi.datum.CrawlDbDatum"
        },
        parser_params={
          "text/plain": __name__ + "._parserNotOutlinks"
        },
        shards=2)
    p.start()
    test_support.execute_until_empty(self.taskqueue)

class FetchPipelineFilteredDomainTest(testutil.HandlerTestBase):
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
    createMockCrawlDbDatum("http://appengine.google.com/bar.txt")
    static_robots = "User-agent: test\nDisallow: /content_0\nDisallow: /content_1\nDisallow: /content_3"
    self.setReturnValue(url="http://appengine.google.com/robots.txt",
        content=static_robots,
        headers={"Content-Length": len(static_robots)})
    resource = self.getResource("cloudysunny14.html")
    static_content = resource.read()
    static_content_length = len(static_content)
    self.setReturnValue(url="http://appengine.google.com/bar.txt",
        content=static_content,
        headers={"Content-Length": static_content_length,
            "Content-Type": "text/plain"})
    p = pipelines.FetcherPipeline("FetcherPipeline",
        params={
          "entity_kind": "lakshmi.datum.CrawlDbDatum"
        },
        parser_params={
          "text/html": __name__ + "._htmlOutlinkParser"
        },
        shards=2)
    p.start()
    test_support.execute_until_empty(self.taskqueue)
    entities = CrawlDbDatum.query(CrawlDbDatum.url=="http://appengine.google.com/bar.txt").fetch()
    entity = entities[0]
    fetched_datum = FetchedDatum.get_by_id(entity.url)
    #Domain filter is replace to extract outlinks job for issue #20
    self.assertTrue(fetched_datum is not None)    

def _htmlFixOutlinkParser(content):
  "htmlOutlinkParser for testing"
  return ["http://appengine.google.com/cloudysunny14/", "http://appengine.google.com/cloudysunny14/tag/content",
      "http://appengine.google.com/cloudysunny14/dummy_content", "http://anothercontent.com/cloudysunny14"] 

class FetchPipelineFilteredURLTest(testutil.HandlerTestBase):
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
    createMockCrawlDbDatum("http://content/cloudysunny14/")
    static_robots = "User-agent: test\nDisallow: /content_0\nDisallow: /content_1\nDisallow: /content_3"
    self.setReturnValue(url="http://content/robots.txt",
        content=static_robots,
        headers={"Content-Length": len(static_robots)})
    resource = self.getResource("cloudysunny14.html")
    static_content = resource.read()
    static_content_length = len(static_content)
    self.setReturnValue(url="http://content/cloudysunny14/",
        content=static_content,
        headers={"Content-Length": static_content_length,
            "Content-Type": "text/html"})
    p = pipelines.FetcherPipeline("FetcherPipeline",
        params={
          "entity_kind": "lakshmi.datum.CrawlDbDatum"
        },
        parser_params={
          "text/html": __name__ + "._htmlFixOutlinkParser"
        },
        shards=2)
    p.start()
    test_support.execute_until_empty(self.taskqueue)
    
    qry = CrawlDbDatum.query(CrawlDbDatum.last_status == pipelines.UNFETCHED)
    crawl_db_datums = qry.fetch()
    self.assertEquals(1,len(crawl_db_datums))

if __name__ == "__main__":
  unittest.main()
