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

from google.appengine.ext import ndb

class CrawlDbDatum(ndb.Model):
  """Holds accumulated state of crawl execution.
  CrawlDbDatum is stored in datastore.
  
  Properties:
    url: the url for fetch
    last_fetched: last time of fetch
    last_updated: last time of update
    last_status: the status of last fetch
    crawl_depth: the crawl depth"""
    
  url = ndb.StringProperty()
  extract_domain_url = ndb.StringProperty()
  last_fetched = ndb.DateTimeProperty(verbose_name=None,
                             auto_now=True,
                             auto_now_add=True)
  last_updated = ndb.DateTimeProperty(verbose_name=None,
                             auto_now=True,
                             auto_now_add=True)
  last_status = ndb.IntegerProperty()
  crawl_depth = ndb.IntegerProperty()
  
  @classmethod
  def kind(cls):
    return "CrawlDbDatum"
  
  @classmethod
  def fetch_crawl_db(cls, ancestor_key):
    return cls.query(ancestor=ancestor_key).fetch()
  
class FetchedDatum(ndb.Model):
  """Hold the fetched result.
  FetchedDatum is stored in datastore.
  
  Properties:
    url: base url.
    fetched_url: the fetched url.
    fetch_time: the time of fetch.
    content_text: the text type of content.
    content_binary: the binary type of content.
    content_type: the content type.
    content_size: the content size.
    response_rate: the response rate.
    http_headers: the responsed HTTP header.
  """
  url = ndb.StringProperty()
  fetched_url = ndb.StringProperty()
  fetch_time = ndb.FloatProperty()
  content_text = ndb.TextProperty()
  content_binary = ndb.BlobProperty()
  content_type = ndb.StringProperty()
  content_size = ndb.IntegerProperty()
  response_rate = ndb.IntegerProperty()
  http_headers = ndb.TextProperty()

  @classmethod
  def kind(cls):
    return "FetchedDatum"

  @classmethod
  def fetch_fetched_datum(cls, ancestor_key):
    return cls.query(ancestor=ancestor_key).fetch()
