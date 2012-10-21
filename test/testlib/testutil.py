#!/usr/bin/env python
#
# Copyright 2010 Google Inc.
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

"""Test utilities for mapreduce framework.
"""

# Disable "Invalid method name"
# pylint: disable-msg=C6409

# os_compat must be first to ensure timezones are UTC.
# Disable "unused import" and "invalid import order"
# pylint: disable-msg=W0611
from google.appengine.tools import os_compat
# pylint: enable-msg=W0611

import time
from testlib import mox
import os
import shutil
import tempfile
import unittest
from urlparse import urlparse

from google.appengine.api import apiproxy_stub_map
from google.appengine.api import apiproxy_stub
from google.appengine.api.files import file_service_stub
from google.appengine.api.blobstore import blobstore_stub
from google.appengine.api import datastore_file_stub
from google.appengine.api import queueinfo
from google.appengine.api.blobstore import file_blob_storage
from google.appengine.api.memcache import memcache_stub
from google.appengine.api.taskqueue import taskqueue_stub
from google.appengine.api import urlfetch_stub
from google.appengine.api.search import simple_search_stub
from lakshmi import configuration

class URLFetchServiceMock(apiproxy_stub.APIProxyStub):
  """Mock for google.appengine.api.urlfetch."""
  def __init__(self, service_name="urlfetch"):
    super(URLFetchServiceMock, self).__init__(service_name)
 
  def set_return_values(self, return_value):
    self._return_values = return_value
    self._redirect_url = return_value.get("final_url")
 
  def _Dynamic_Fetch(self, request, response):
    return_values = self._return_values
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

    parsed_uri = urlparse(request.url())
    if parsed_uri.scheme == "http" or parsed_uri.scheme == "https":
      self.request = request
      self.response = response
    else:
      raise Exception("Invalid Url.")
    
class MatchesDatastoreConfig(mox.Comparator):
  """Mox comparator for MatchesDatastoreConfig objects."""

  def __init__(self, **kwargs):
    self.kwargs = kwargs

  def equals(self, config):
    """Check to see if config matches arguments."""
    if self.kwargs.get("deadline", None) != config.deadline:
      return False
    if self.kwargs.get("force_writes", None) != config.force_writes:
      return False
    return True

  def __repr__(self):
    return "MatchesDatastoreConfig(%s)" % self.kwargs


class MatchesUserRPC(mox.Comparator):
  """Mox comparator for UserRPC objects."""

  def __init__(self, **kwargs):
    self.kwargs = kwargs

  def equals(self, rpc):
    """Check to see if rpc matches arguments."""
    if self.kwargs.get("deadline", None) != rpc.deadline:
      return False
    return True

  def __repr__(self):
    return "MatchesUserRPC(%s)" % self.kwargs


class HandlerTestBase(unittest.TestCase):
  """Base class for all webapp.RequestHandler tests."""

  MAPREDUCE_URL = "/_ah/lakshmi/kickoffjob_callback"

  def setUp(self, urlfetch_mock=URLFetchServiceMock()):
    unittest.TestCase.setUp(self)
    self.mox = mox.Mox()

    self.appid = "testapp"
    self.version_id = "1.23456789"
    os.environ["APPLICATION_ID"] = self.appid
    os.environ["CURRENT_VERSION_ID"] = self.version_id
    os.environ["HTTP_HOST"] = "localhost"

    self.memcache = memcache_stub.MemcacheServiceStub()
    self.taskqueue = taskqueue_stub.TaskQueueServiceStub()
    self.taskqueue.queue_yaml_parser = (
        lambda x: queueinfo.LoadSingleQueue(
            "queue:\n"
            "- name: default\n"
            "  rate: 10/s\n"
            "- name: crazy-queue\n"
            "  rate: 2000/d\n"
            "  bucket_size: 10\n"))
    self.datastore = datastore_file_stub.DatastoreFileStub(
        self.appid, "/dev/null", "/dev/null")

    self.blob_storage_directory = tempfile.mkdtemp()
    blob_storage = file_blob_storage.FileBlobStorage(
        self.blob_storage_directory, self.appid)
    self.blobstore_stub = blobstore_stub.BlobstoreServiceStub(blob_storage)
    self.file_service = self.createFileServiceStub(blob_storage)
    self._urlfetch_mock = urlfetch_mock
    self.search_service = simple_search_stub.SearchServiceStub()
    
    apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
    apiproxy_stub_map.apiproxy.RegisterStub("taskqueue", self.taskqueue)
    apiproxy_stub_map.apiproxy.RegisterStub("memcache", self.memcache)
    apiproxy_stub_map.apiproxy.RegisterStub("datastore_v3", self.datastore)
    apiproxy_stub_map.apiproxy.RegisterStub("blobstore", self.blobstore_stub)
    apiproxy_stub_map.apiproxy.RegisterStub("file", self.file_service)
    apiproxy_stub_map.apiproxy.RegisterStub("urlfetch", self._urlfetch_mock)
    apiproxy_stub_map.apiproxy.RegisterStub("search", self.search_service)

  def createFileServiceStub(self, blob_storage):
    return file_service_stub.FileServiceStub(blob_storage)

  def tearDown(self):
    try:
      self.mox.VerifyAll()
    finally:
      self.mox.UnsetStubs()
      shutil.rmtree(self.blob_storage_directory)
    unittest.TestCase.tearDown(self)

  def assertTaskStarted(self, queue="default"):
    tasks = self.taskqueue.GetTasks(queue)
    self.assertEquals(1, len(tasks))
    self.assertEquals(tasks[0]["url"], self.MAPREDUCE_URL)
    
  def setReturnValue(self, **kwargs):
    """ set the return value."""
    self._urlfetch_mock.set_return_values(kwargs)

class FetchTestBase(unittest.TestCase):
  """Base class for fetcher."""
    
  def setUp(self):
    unittest.TestCase.setUp(self)
    self.tempdir = tempfile.mkdtemp()
    # Regist API Proxy
    apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
    # Regist urlfetch stub
    self._urlfetch_mock = URLFetchServiceMock()
    urlfetch_stub.URLFetchServiceStub()
    apiproxy_stub_map.apiproxy.RegisterStub("urlfetch", 
                                             self._urlfetch_mock)
    
  def setReturnValue(self, **kwargs):
    """ set the return value."""
    self._urlfetch_mock.set_return_values(kwargs)
    
  def tearDown(self):
    unittest.TestCase.tearDown(self)
    
class FetcherPolicyUtil(object):
  """ Utilty for create fetcherPolicy. """
  @classmethod
  def createDefaultFetcherPolicy(cls):
    """ to get default fetcher policy """
    path = os.path.join(os.path.dirname(__file__), "resource", "fetcher_policy.yaml")
    fetcher_policy_yaml = configuration.parse_fetcher_policy_yaml(open(path))
    return fetcher_policy_yaml
    
  
