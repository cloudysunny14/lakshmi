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
  This entity holds status of seed urls.
  last_status property is using as criteria for fetch target.
  Entity's ancestor key is setting target url.

  Properties:
    url: the url for fetch
    last_fetched: last time of fetch
    last_updated: last time of update
    last_status: the status of last fetch
  """
  #reason of indexed=False is saving the datastore write operation.
  url = ndb.StringProperty()
  extract_domain_url = ndb.StringProperty()
  last_fetched = ndb.DateTimeProperty(verbose_name=None,
                             indexed=False)
  last_updated = ndb.DateTimeProperty(verbose_name=None,
                             auto_now=True,
                             auto_now_add=True,
                             indexed=False)
  last_status = ndb.IntegerProperty()
  
  @classmethod
  def kind(cls):
    return "CrawlDbDatum"

  @classmethod
  @ndb.transactional
  def insert_or_fail(cls, id_or_keyname, **kwds):
    entity = cls.get_by_id(id=id_or_keyname, parent=kwds.get('parent'))
    if entity is None:
      entity = cls(id=id_or_keyname, **kwds)
      entity.put()
      return entity
    return None
  
class FetchedDbDatum(ndb.Model):
  """Hold the fetched result.
  FetchedDbDatum is stored in datastore,
  whitch entity is fetched content of target page,
  that could store text content only.
  Also holds other status of fetch.

  Properties:
    url: base url.
    fetched_url: the fetched url.
    fetch_time: the time of fetch.
    fetched_content: the html content of page.
    content_type: the content type.
    content_size: the content size.
    response_rate: the response rate.
    http_headers: the responsed HTTP header.
  """
  url = ndb.StringProperty(indexed=False)
  fetched_url = ndb.StringProperty(indexed=False)
  fetch_time = ndb.FloatProperty(indexed=False)
  fetched_content = ndb.TextProperty(indexed=False)
  content_type = ndb.StringProperty(indexed=False)
  content_size = ndb.IntegerProperty(indexed=False)
  response_rate = ndb.IntegerProperty(indexed=False)
  http_headers = ndb.TextProperty(indexed=False)

  @classmethod
  def kind(cls):
    return "FetchedDbDatum"

class ContentDbDatum(ndb.Model):
  """
  Hold the links of page.
  LinkDbDatum is stored in datastore,
  This entity's ancestor key is setting target page's url.
  Fetched contents are Storing to blobstore.

  Properties:
    fetched_url: the url of fetched.
    stored_url: the url content is stored.
    content_type: content type.
    content_size: the size of content.
    http_headers: the http headers.
  """
  fetched_url = ndb.StringProperty(indexed=False)
  stored_url = ndb.StringProperty(indexed=False)
  content_type = ndb.StringProperty(indexed=False)
  content_size = ndb.IntegerProperty(indexed=False)
  http_headers = ndb.TextProperty(indexed=False)

  @classmethod
  def kind(cls):
    return "ContentDbDatum"

class LinkDbDatum(ndb.Model):
  """This model is sample. Hold the links of page.
  LinkDbDatum is stored in datastore,
  which entity is extracted links from page.
  
  Note:this entity is storing in your definition function.

  Properties:
    link_url: the url of link.
  """
  link_url = ndb.StringProperty(indexed=False)

  @classmethod
  def kind(cls):
    return "LinkDbDatum"
