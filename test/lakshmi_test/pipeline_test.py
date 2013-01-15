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

import unittest
import os
import re

from google.appengine.ext import ndb

from mapreduce.lib import files
from mapreduce.lib import pipeline
from mapreduce.lib.files import file_service_pb
from mapreduce import input_readers
from testlib import testutil
from mapreduce import test_support
from lakshmi import pipelines
from lakshmi.datum import CrawlDbDatum
from lakshmi.datum import FetchedDbDatum
from lakshmi.datum import ContentDbDatum
from mapreduce.lib.files import records

def createMockCrawlDbDatum(domain_count, url_count, isExtracted):
    """Create CrawlDbDatum mock data."""
    for d in range(domain_count):
      for n in range(url_count):
        url = "http://hoge_%d.com/content_%d" % (d, n)
        extracted_url = None
        if isExtracted:
          extracted_url = "http://hoge_%d.com"%(d)
        datum = CrawlDbDatum(
            parent =ndb.Key(CrawlDbDatum, url),
            url=url,
            extract_domain_url=extracted_url,
            last_status=pipelines.UNFETCHED)
        datum.put()

def createMockFetchedDatum(url, html_text, status):
  """Create FetchedDatum mock data."""
  key = ndb.Key(CrawlDbDatum, url)
  crawl = CrawlDbDatum.get_or_insert(url, parent=key,
      url=url, last_status=status)
  if status != pipelines.UNFETCHED:
    fetched_datum = FetchedDbDatum(parent=crawl.key,
        url=url, fetched_url = url,
        fetched_content = html_text, content_type="text/html")
    fetched_datum.put()

def createLinkDatum(parent_url, url):
  """Create Link CrawlDbDatum mock data."""
  key = ndb.Key(CrawlDbDatum, parent_url)
  CrawlDbDatum.get_or_insert(url, parent=key,
      url=url, last_status=pipelines.UNFETCHED)
        
class ExactDomainPilelineTest(testutil.HandlerTestBase):
  """Tests for ExactDomainPileline."""
  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []
  
  def _send_mail(self, sender, subject, body, html=None):
    """Callback function for sending mail."""
    self.emails.append((sender, subject, body, html))
  
  def testSuccessfulRun(self):
    createMockCrawlDbDatum(2, 6, False)
    
    p = pipelines._ExactDomainMapreducePipeline("ExactDomainMapreducePipeline",
                                                 params={
                                                         "entity_kind": "lakshmi.datum.CrawlDbDatum",
                                                         },
                                                 shard_count=3)
    p.start()
    test_support.execute_until_empty(self.taskqueue)
    finished_map = pipelines._RobotsFetchPipeline.from_id(p.pipeline_id)
    
    # Can open files
    file_paths = finished_map.outputs.default.value
    self.assertTrue(len(file_paths) > 0)
    self.assertTrue(file_paths[0].startswith("/blobstore/"))
    
    for file_path in file_paths:
      blob_key = files.blobstore.get_blob_key(file_path)
      reader = input_readers.BlobstoreLineInputReader(blob_key, 0, 100)
      u = 0
      for content in reader:
        self.assertTrue(content[1]!=None)
        u += 1
    
    self.assertEqual(2, u)

    query = CrawlDbDatum.query(CrawlDbDatum.extract_domain_url=="http://hoge_0.com")
    entities = query.fetch()
    for entity in entities:
      self.assertEquals("http://hoge_0.com", entity.extract_domain_url)
    
class RobotFetcherPipelineTest(testutil.HandlerTestBase):
  """Tests for RobotFetcherPipelineTest."""

  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []
  
  def _send_mail(self, sender, subject, body, html=None):
    """Callback function for sending mail."""
    self.emails.append((sender, subject, body, html))
    
  def createMockData(self, url_count, shard):
    blob_keys = []
    for num in range(shard):
      file_name = "myblob_%d" % num
      urls = "\n".join(["http://test_url_%d.com" % i for i in range(url_count)])
      file_path = files.blobstore.create("text/plain", file_name)
      with files.open(file_path, 'a') as fp:
        fp.write(urls)
      files.finalize(file_path)
      blob_key = files.blobstore.get_blob_key(file_path)
      file_name = files.blobstore.get_file_name(blob_key)
      blob_keys.append(str(file_name))
    return blob_keys

  def testSuccessfulRun(self):
    blob_keys = self.createMockData(3, 2)
    static_content = "User-agent: *\nDisallow: /search\nDisallow: /sdch\nDisallow: /groups"
    self.setReturnValue(content=static_content,
                        headers={"Content-Length": len(static_content),
                                 "Content-Type": "text/html"})
    p = pipelines._RobotsFetchPipeline("RobotsFetchPipeline", blob_keys, 2)
    p.start()
    
    test_support.execute_until_empty(self.taskqueue)
    finished_map = pipelines._RobotsFetchPipeline.from_id(p.pipeline_id)
    
    # Can open files
    file_list = finished_map.outputs.default.value
    self.assertTrue(len(file_list) > 0)
    reader = input_readers.RecordsReader(file_list, 0)
    for binary_record in reader:
      proto = file_service_pb.KeyValue()
      proto.ParseFromString(binary_record)
      key = proto.key()
      value = proto.value()
      self.assertTrue(key is not None)
      self.assertTrue(value is not None)
  
  def createInvalidMockData(self):
    blob_keys = []
    url = "invalidScheme://test_url.com"
    file_path = files.blobstore.create("text/plain", url)
    with files.open(file_path, 'a') as fp:
      fp.write(url)
    files.finalize(file_path)
    blob_key = files.blobstore.get_blob_key(file_path)
    file_name = files.blobstore.get_file_name(blob_key)
    blob_keys.append(str(file_name))

    return blob_keys

  def testFetchError(self):
    blob_keys = self.createInvalidMockData()
    static_content = "User-agent: *\nDisallow: /search\nDisallow: /sdch\nDisallow: /groups"
    self.setReturnValue(content=static_content,
                        headers={"Content-Length": len(static_content),
                                 "Content-Type": "text/html"})
    p = pipelines._RobotsFetchPipeline("RobotsFetchPipeline", blob_keys, 2)
    p.start()
    
    test_support.execute_until_empty(self.taskqueue)
    finished_map = pipelines._RobotsFetchPipeline.from_id(p.pipeline_id)
    
    # Can open files
    file_list = finished_map.outputs.default.value
    self.assertTrue(len(file_list) > 0)
    reader = input_readers.RecordsReader(file_list, 0)
    for binary_record in reader:
      proto = file_service_pb.KeyValue()
      proto.ParseFromString(binary_record)
      key = proto.key()
      value = proto.value()
      self.assertEquals("invalidScheme://test_url.com", key)
      self.assertEquals("User-agent: *\nDisallow: /", value)

class FetchSetsBufferPipelineTest(testutil.HandlerTestBase):
  """Tests for FetchSetsBufferPipeline."""
  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []

  def _send_mail(self, sender, subject, body, html=None):
    """Callback function for sending mail."""
    self.emails.append((sender, subject, body, html))
  
  def createMockData(self, data):
    """Create mock data for FetchSetsBufferPipeline"""
    input_file = files.blobstore.create()
    with files.open(input_file, "a") as f:
      with records.RecordsWriter(f) as w:
        key = str(data[0])
        value = str(data[1])
        proto = file_service_pb.KeyValue()
        proto.set_key(key)
        proto.set_value(value)
        w.write(proto.Encode())

    files.finalize(input_file)
    input_file = files.blobstore.get_file_name(
        files.blobstore.get_blob_key(input_file))
    
    return input_file
  
  def testSuccessfulRun(self):
    file_name1 = self.createMockData(("http://hoge_0.com", "User-agent: test\nDisallow: /content_0\nDisallow: /content_1\nDisallow: /content_3"))
    file_name2 = self.createMockData(("http://hoge_1.com", "User-agent: test\nAllow: /content_0\nAllow: /content_1\nDisallow: /content_3"))
    createMockCrawlDbDatum(2, 6, True)
    p = pipelines._FetchSetsBufferPipeline("FetchSetsBufferPipeline", [file_name1, file_name2])
    p.start()
    test_support.execute_until_empty(self.taskqueue)
    finished_map = pipelines._FetchSetsBufferPipeline.from_id(p.pipeline_id)
    
    # Can open files
    file_paths = finished_map.outputs.default.value
    self.assertTrue(len(file_paths) > 0)
    self.assertTrue(file_paths[0].startswith("/blobstore/"))

    reader = input_readers.RecordsReader(file_paths, 0)
    for binary_record in reader:
      proto = file_service_pb.KeyValue()
      proto.ParseFromString(binary_record)
      key = proto.key()
      value = proto.value()
      self.assertTrue(key is not None)
      self.assertTrue(value is not None)

class FetchPagePipelineTest(testutil.HandlerTestBase):
  """Tests for FetchPipelineTest."""
  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []

  def _send_mail(self, sender, subject, body, html=None):
    """Callback function for sending mail."""
    self.emails.append((sender, subject, body, html))

  def createMockData(self, data):
    """Create mock data for FetchSetsBufferPipeline"""
    input_file = files.blobstore.create()
    with files.open(input_file, "a") as f:
      with records.RecordsWriter(f) as w:
        key = str(data[0])
        value = str(data[1])
        proto = file_service_pb.KeyValue()
        proto.set_key(key)
        proto.set_value(value)
        w.write(proto.Encode())

    files.finalize(input_file)
    input_file = files.blobstore.get_file_name(
        files.blobstore.get_blob_key(input_file))

    return input_file

  def testSuccessfulRun(self):
    createMockCrawlDbDatum(2, 2, True)
    file_name1 = self.createMockData(("http://hoge_0.com/content_0", True))
    file_name2 = self.createMockData(("http://hoge_1.com/content_0", False))
    static_content = "<html><body>TestContent</body></html>"
    self.setReturnValue(content=static_content,
                        headers={"Content-Length": len(static_content),
                                 "Content-Type": "text/html"})
    p = pipelines._FetchPagePipeline("FetchPipeline", [file_name1, file_name2], 2)
    p.start()
    test_support.execute_until_empty(self.taskqueue)
    finished_map = pipelines._FetchPagePipeline.from_id(p.pipeline_id)
    
    # Can open files
    file_paths = finished_map.outputs.default.value
    self.assertTrue(len(file_paths) > 0)
    self.assertTrue(file_paths[0].startswith("/blobstore/"))
    
    
    entities = CrawlDbDatum.query(CrawlDbDatum.url=="http://hoge_0.com/content_0").fetch()
    entity = entities[0]
    fetched_datum = FetchedDbDatum.query(ancestor=entity.key).fetch()
    self.assertTrue(fetched_datum is not None)

def _htmlOutlinkParser(url, content):
  """htmlOutlinkParser for testing"""
  return re.findall(r'href=[\'"]?([^\'" >]+)', "".join(content))

class ExtractOutlinksPipelineTest(testutil.HandlerTestBase):
  """Test for ExtractOutlinksPipelineTest. """
  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []

  def getResource(self, file_name):
    """ to get contents from resource"""
    path = os.path.join(os.path.dirname(__file__), "resource", file_name)
    return open(path)

  def _send_mail(self, sender, subject, body, html=None):
    """Callback function for sending mail."""
    self.emails.append((sender, subject, body, html))

  def createMockDataLine(self, data):
    file_name = "myblob_01"
    file_path = files.blobstore.create("text/plain", file_name)
    with files.open(file_path, 'a') as fp:
      fp.write(data)
    files.finalize(file_path)
    blob_key = files.blobstore.get_blob_key(file_path)
    file_name = files.blobstore.get_file_name(blob_key)
    return file_name

  def testSuccessfulRun(self):
    """Test extract outlinks by UDF."""
    resource_neg = self.getResource("cloudysunny14.html")
    static_content = resource_neg.read()
    createMockFetchedDatum("http://cloudysunny14.html", static_content, pipelines.FETCHED)
    file_name = self.createMockDataLine("http://cloudysunny14.html\n")
    p = pipelines._ExtractOutlinksPipeline("ExtractOutlinksPipeline",
        file_names=[file_name],
        parser_params={
          "text/html": __name__+"._htmlOutlinkParser"
        }) 
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    entities = CrawlDbDatum.query(CrawlDbDatum.url=="http://cloudysunny14.html").fetch()
    entity = entities[0]
    fetched_datum = FetchedDbDatum.query(ancestor=entity.key).fetch()
    self.assertTrue(fetched_datum!=None)
    qry = CrawlDbDatum.query(CrawlDbDatum.last_status == pipelines.UNFETCHED)
    crawl_db_datums = qry.fetch()
    self.assertTrue(len(crawl_db_datums)==0)

class FetchContentPipelineTest(testutil.HandlerTestBase):
  """Tests for FetchContentPipeline."""
  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []

  def getResource(self, file_name):
    """ to get contents from resource"""
    path = os.path.join(os.path.dirname(__file__), "resource", file_name)
    return open(path)
  
  def _send_mail(self, sender, subject, body, html=None):
    """Callback function for sending mail."""
    self.emails.append((sender, subject, body, html))
  
  def createMockData(self, data):
    """Create mock data for FetchContentPipeline"""
    input_file = files.blobstore.create()
    with files.open(input_file, "a") as f:
      with records.RecordsWriter(f) as w:
        key = str(data[0])
        value = str(data[1])
        proto = file_service_pb.KeyValue()
        proto.set_key(key)
        proto.set_value(value)
        w.write(proto.Encode())

    files.finalize(input_file)
    input_file = files.blobstore.get_file_name(
        files.blobstore.get_blob_key(input_file))
    
    return input_file
  
  def testSuccessfulRun(self):
    file_name1 = self.createMockData(("https://developers.google.com/appengine/", "http://k.yimg.jp/images/top/sp/logo.gif"))
    file_name2 = self.createMockData(("https://developers.google.com/appengine/", "/appengine/images/slide1.png"))
    datum = CrawlDbDatum(
        parent =ndb.Key(CrawlDbDatum, "https://developers.google.com/appengine/"),
        url="https://developers.google.com/appengine/",
        extract_domain_url="https://developers.google.com",
        last_status=pipelines.UNFETCHED)
    datum.put()
    resource = self.getResource("slide1.png")
    static_content = resource.read()
    self.setReturnValue(content=static_content,
                        headers={"Content-Length": len(static_content),
                                 "Content-Type": "image/png"})
    p = pipelines._FetchContentPipeline("FetchContentPipeline", [file_name1, file_name2])
    p.start()
    test_support.execute_until_empty(self.taskqueue)
    finished_map = pipelines._FetchSetsBufferPipeline.from_id(p.pipeline_id)
    
    # Can open files
    file_paths = finished_map.outputs.default.value
    self.assertTrue(len(file_paths) > 0)
    self.assertTrue(file_paths[0].startswith("/blobstore/"))

    reader = input_readers.RecordsReader(file_paths, 0)
    for binary_record in reader:
      proto = file_service_pb.KeyValue()
      proto.ParseFromString(binary_record)
      key = proto.key()
      value = proto.value()
      self.assertTrue(key is not None)
      self.assertTrue(value is not None)

    query = CrawlDbDatum.query(CrawlDbDatum.url=="https://developers.google.com/appengine/")
    crawl_db_datums = query.fetch()
    self.assertTrue(len(crawl_db_datums)>0)
    key = crawl_db_datums[0].key
    content_datums = ContentDbDatum.query(ancestor=key).fetch()
    self.assertEqual(2, len(content_datums))

class CleanPipelineTest(testutil.HandlerTestBase):
  """Test for CleanPipelineTest. """
  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []

  def getResource(self, file_name):
    """ to get contents from resource"""
    path = os.path.join(os.path.dirname(__file__), "resource", file_name)
    return open(path)

  def _send_mail(self, sender, subject, body, html=None):
    """Callback function for sending mail."""
    self.emails.append((sender, subject, body, html))

  def testSuccessfulRun(self):
    """Test clean pipeline."""
    createMockFetchedDatum("http://foo.html", "Content", pipelines.FETCHED)
    createMockFetchedDatum("http://bar.html", "Content", pipelines.SKIPPED)
    createMockFetchedDatum("http://baz.html", "Content", pipelines.UNFETCHED)
    p = pipelines.CleanDatumPipeline("CleanDatumPipeline",
        params={
          "entity_kind": "lakshmi.datum.CrawlDbDatum",
        },
        shards=3) 
    p.start()
    test_support.execute_until_empty(self.taskqueue)

    entities = CrawlDbDatum.query(CrawlDbDatum.url== "http://foo.html").fetch()
    self.assertEquals(0, len(entities))
    entities = CrawlDbDatum.query(CrawlDbDatum.url== "http://bar.html").fetch()
    self.assertEquals(0, len(entities))
    entities = CrawlDbDatum.query(CrawlDbDatum.url=="http://baz.html").fetch()
    self.assertEquals(1, len(entities))

if __name__ == "__main__":
  unittest.main()
