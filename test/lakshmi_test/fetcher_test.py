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

from testlib import testutil
from lakshmi import fetchers
from lakshmi import configuration
from lakshmi import errors
from google.appengine.api import apiproxy_stub_map
from google.appengine.api import urlfetch_stub


class SimpleHttpFetcherTest(testutil.FetchTestBase):
  
  def assertLR(self, resultL, resultR):
    self.assertEquals(resultL, resultR)
  
  def getCustomFetcherPolicy(self, file_name):
    """To get custom fetcher policy """
    yaml_path = os.path.join(os.path.dirname(__file__), "resource", file_name)
    fetcher_policy_yaml = configuration.parse_fetcher_policy_yaml(open(yaml_path))
    return fetcher_policy_yaml
  
  def getResource(self, file_name):
    """To get contents from resource"""
    path = os.path.join(os.path.dirname(__file__), "resource", file_name)
    return open(path)
  
  def testSlowServerTermination(self):
    #use 20KBytes. And the duration is 2 seconds, thus 10KBytes/Sec
    self.setReturnValue(headers={"Content-Length": 20000,
                                 "Content-Type": "text/html"}, duration=2)
    fetcher_policy_yaml = configuration.FetcherPolicyYaml.create_default_policy()
    #set min_response_rate of 20KByte/Sec
    fetcher_policy_yaml.fetcher_policy.min_response_rate = 20000 
    simple_http_fetcher = fetchers.SimpleHttpFetcher(1,
                                   fetcher_policy_yaml.fetcher_policy)
    url = "http://static_resource/simple-page.html"
    self.assertRaises(errors.AbortedFetchError, 
                      simple_http_fetcher.get, url)
  
  def testNotTerminatingSlowServer(self):
    #Return server 1Kbyte at 2K byte/sec.
    self.setReturnValue(headers={"Content-Length": 5000,
                                 "Content-Type": "text/html"}, duration=0.25)
    fetcher_policy_yaml = configuration.FetcherPolicyYaml.create_default_policy()
    fetcher_policy_yaml.fetcher_policy.min_response_rate = configuration.NO_MIN_RESPONSE_RATE
    simple_http_fetcher = fetchers.SimpleHttpFetcher(1,
                                   fetcher_policy_yaml.fetcher_policy)
    url = "http://static_resource/simple-page.html"
    simple_http_fetcher.get(url)
  
  def testLargeContent(self):
    #Test for should be truncate.
    fetcher_policy_yaml = configuration.FetcherPolicyYaml.create_default_policy()
    content_size = fetcher_policy_yaml.fetcher_policy.max_content_size[0]
    max_content_size = int(content_size.size)
    self.setReturnValue(headers={"Content-Length": max_content_size*2,
                                 "Content-Type": "text/html"})
    simple_http_fetcher = fetchers.SimpleHttpFetcher(1,
                                   fetcher_policy_yaml.fetcher_policy)
    url = "http://static_resource/simple-page.html"
    result = simple_http_fetcher.get(url)
    self.assertTrue(result.get("content_length") <= max_content_size, "Should be truncate")
    
  def testTruncationWithKeepAlive(self):
    fetcher_policy_yaml = self.getCustomFetcherPolicy("fetcher_policy_sizes.yaml")
    resource = self.getResource("cloudysunny14.html")
    static_content = resource.read()
    static_content_length = len(static_content)
    self.setReturnValue(content=static_content,
                        headers={"Content-Length": static_content_length,
                                 "Content-Type": "text/html"})
    simple_http_fetcher = fetchers.SimpleHttpFetcher(1,
                                   fetcher_policy_yaml.fetcher_policy)
    url = "http://static_resource/cloudysunny14.html"
    result_left = simple_http_fetcher.get(url)
    result_right = simple_http_fetcher.get(url)
    self.assertEqual(1000, result_left.get("content_length"))
    self.assertEqual(1000, result_right.get("content_length"))
    map(self.assertLR, result_left.get("content"), result_right.get("content"))
    
    resource = self.getResource("mining.png")
    static_content = resource.read()
    static_content_length = len(static_content)
    self.setReturnValue(content=static_content,
                        headers={"Content-Length": static_content_length,
                                 "Content-Type": "image/png"})
    url = "http://static_resource/mining.png"
    result = simple_http_fetcher.get(url)
    self.assertTrue(result.get("content_length") > 1000)
    
  def testContentTypeHeader(self):
    fetcher_policy_yaml = configuration.FetcherPolicyYaml.create_default_policy()
    resource = self.getResource("cloudysunny14.html")
    static_content = resource.read()
    static_content_length = len(static_content)
    self.setReturnValue(content=static_content,
                        headers={"Content-Length": static_content_length,
                                 "Content-Type": "text/html"})
    simple_http_fetcher = fetchers.SimpleHttpFetcher(1,
                                   fetcher_policy_yaml.fetcher_policy)
    url = "http://static_resource/cloudysunny14.html"
    result = simple_http_fetcher.get(url)
    header = result.get("headers")
    content_type = header["Content-Type"]
    self.assertTrue(content_type!=None)
    self.assertEquals("text/html", content_type)
  
  def redirectUrl(self):
    return "http://static_resource/redirect"
  
  def testRedirectHandling(self):
    fetcher_policy_yaml = configuration.FetcherPolicyYaml.create_default_policy()
    self.setReturnValue(headers={"Content-Length": 20000,
                                 "Content-Type": "text/html"},
                        final_url=self.redirectUrl)
    url = "http://static_resource/base"
    simple_http_fetcher = fetchers.SimpleHttpFetcher(1,
                                   fetcher_policy_yaml.fetcher_policy)
    result = simple_http_fetcher.get(url)
    self.assertTrue("http://static_resource/redirect", result.get("fetched_url"))
    
  def testRedirectPolicy(self):
    fetcher_policy_yaml = self.getCustomFetcherPolicy("fetcher_policy_redirect_none.yaml")
    self.setReturnValue(headers={"Content-Length": 20000,
                                 "Content-Type": "text/html"},
                        status_code=301,
                        final_url=self.redirectUrl)
    url = "http://static_resource/base"
    simple_http_fetcher = fetchers.SimpleHttpFetcher(1,
                                   fetcher_policy_yaml.fetcher_policy)
    self.assertRaises(errors.RedirectError, 
                      simple_http_fetcher.get, url)
    
  def testAcceptLanguage(self):
    fetcher_policy_yaml = self.getCustomFetcherPolicy("fetcher_policy.yaml")
    self.setReturnValue(headers={"Content-Length": 20000,
                                 "Content-Type": "text/html"},
                        status_code=200,
                        language_content={"en": "English",
                                          "ja": "Japanese"},
                        final_url=self.redirectUrl)
    url = "http://static_resource/simple-page.html"
    simple_http_fetcher = fetchers.SimpleHttpFetcher(1,
                                   fetcher_policy_yaml.fetcher_policy)
    result = simple_http_fetcher.get(url)
    self.assertTrue("English", result.get("content"))
    
  def testMimeTypeFiltering(self):
    fetcher_policy_yaml = self.getCustomFetcherPolicy("fetcher_policy.yaml")
    self.setReturnValue(headers={"Content-Length": 20000,
                                 "Content-Type": "text/xml"},
                        status_code=200,
                        final_url=self.redirectUrl)
    url = "http://static_resource/simple-page.html"
    simple_http_fetcher = fetchers.SimpleHttpFetcher(1,
                                   fetcher_policy_yaml.fetcher_policy)
    self.assertRaises(errors.AbortedFetchError, 
                      simple_http_fetcher.get, url)
  
  def testMimeTypeFilteringNoContentType(self):
    fetcher_policy_yaml = self.getCustomFetcherPolicy("fetcher_policy.yaml")
    self.setReturnValue(headers={"Content-Length": 20000},
                        status_code=200,
                        final_url=self.redirectUrl)
    url = "http://static_resource/simple-page.html"
    simple_http_fetcher = fetchers.SimpleHttpFetcher(1,
                                   fetcher_policy_yaml.fetcher_policy)
    simple_http_fetcher.get(url)
  
  def testMimeTypeFilteringWithCharset(self):
    fetcher_policy_yaml = self.getCustomFetcherPolicy("fetcher_policy.yaml")
    self.setReturnValue(headers={"Content-Length": 20000,
                                 "Content-Type": "text/html; charset=UTF-8"},
                        status_code=200,
                        final_url=self.redirectUrl)
    url = "http://cloudysunny14.blogspot.jp/"
    simple_http_fetcher = fetchers.SimpleHttpFetcher(1,
                                   fetcher_policy_yaml.fetcher_policy)
    simple_http_fetcher.get(url)
  
class SimpleHttpFetcherRealTest(unittest.TestCase):
  def setUp(self):
    unittest.TestCase.setUp(self)
    # Regist API Proxy
    apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
    # Regist urlfetch stub
    stub = urlfetch_stub.URLFetchServiceStub()
    apiproxy_stub_map.apiproxy.RegisterStub("urlfetch", 
                                             stub)
  
  def testRealFetch(self):
    fetcher_policy_yaml = configuration.FetcherPolicyYaml.create_default_policy()
    #set min_response_rate of 20KByte/Sec
    simple_http_fetcher = fetchers.SimpleHttpFetcher(1,
                                   fetcher_policy_yaml.fetcher_policy)
    url = "http://cloudysunny14.blogspot.jp/"
    result = simple_http_fetcher.get(url)
    self.assertTrue(result is not None)

  def tearDown(self):
    unittest.TestCase.tearDown(self)

if __name__ == "__main__":
  unittest.main()    
