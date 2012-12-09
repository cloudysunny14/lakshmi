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

class ContentsDatum(ndb.Model):
  """Holds imported contents. 

  Properties:
    url: the url of content
    content: the body of content
    description: the description of content,
      that it like so score of contents or 
      json value of description etc.
  """
  url = ndb.StringProperty()
  content = ndb.TextProperty(indexed=False)
  description = ndb.TextProperty(indexed=False) 
  
  @classmethod
  def kind(cls):
    return "ContentsDatum"

  @classmethod
  @ndb.transactional
  def insert_or_fail(cls, id_or_keyname, **kwds):
    entity = cls.get_by_id(id=id_or_keyname, parent=kwds.get('parent'))
    if entity is None:
      entity = cls(id=id_or_keyname, **kwds)
      entity.put()
      return entity
    return None
