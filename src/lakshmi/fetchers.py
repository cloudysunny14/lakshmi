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

import time
import logging
import httplib
import StringIO
import gzip
import re

from google.appengine.api import urlfetch
from lakshmi import errors
from lakshmi import configuration


DEFAULT_CHARSET = "utf-8,ISO-8859-1;q=0.7,*;q=0.7"
DEFAULT_ACCEPT_ENCODING = "x-gzip, gzip"
DEFAULT_ACCEPT = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
TEXT_MIME_TYPES = ("text/html",
                  "text/plain",
                  "application/x-asp", 
                  "application/xhtml+xml",
                  "application/vnd.wap.xhtml+xml")

DEFAULT_BROWSER_VERSION = "Mozilla/5.0"
CRAWLER_VERSION = "1.0"

REDIRECT_STATUSES = frozenset([
  httplib.MOVED_PERMANENTLY,
  httplib.FOUND,
  httplib.SEE_OTHER,
  httplib.TEMPORARY_REDIRECT,
])

class FetcherBase(object):
  """Abstract base class for fetchers"""
  
  def get(self, score_url_datum):
    """To do HTTP GET request
    
    Args:
      score_url_datum: datum of score url
      
    Returns:
      return results of HTTP GET request
    """
    raise NotImplementedError("get() not implemented in %s" % self.__class__)
  
  def abort(self):
    """ Abort Job """
    raise NotImplementedError("abort() not implemented in %s" % self.__class__)
  
def _create_user_agent(agent_name, email_address, web_address):
  """Create UserAgent from fetcher_policy
  Like Mozilla/5.0 (compatible; mycrawler/1.0; +http://www.mydomain.com; mycrawler@mydomain.com)
  """
  return "%s (compatible; %s/%s; +%s; %s)" % (DEFAULT_BROWSER_VERSION, agent_name, 
                                            CRAWLER_VERSION, web_address, email_address)
  
def _create_headers(fetcher_policy):
  """ Create HTTP headers. """
  return {"User-Agent": _create_user_agent(fetcher_policy.agent_name,
                                           fetcher_policy.email_address,
                                           fetcher_policy.web_address),
             "Accept-Language": fetcher_policy.accept_language,
             "Accept-Charset": DEFAULT_CHARSET,
             "Accept-Encoding": DEFAULT_ACCEPT_ENCODING,
             "Accept": DEFAULT_ACCEPT}
  
def _get_mime_type(content_type):
  """Parse mime type.
  
  Args:
    content_type: content-type of response.
  
  Returns:
    returns string parsed to a media type.
    If content_type is None, returns none value."""
  if content_type is None:
    return None
  
  if ";" in content_type:
    i = content_type.index(";")
    content_type = content_type[:i]
  fields = content_type.split("/")
  for i in range(len(fields)):
    fields[i] = fields[i].strip().lower()
  mime_type = "/".join(fields)
  return mime_type
  
def _get_max_content_size(mime_type, fetcher_policy):
  """ Get the max content-size of mime_type from fetcher_policy """
  content_sizes = fetcher_policy.max_content_size
  size = _get_content_size(content_sizes, mime_type)
  if not size:
    size = _get_content_size(content_sizes, "default")
  return int(size)

def _get_content_size(configured_content_sizes, content_type):
  """Get content size by content type. 
    If not set contents size by content type 
    returns default value at configuration.DEFAULT_MAX_CONTENT_SIZE """
  size = 0
  if len(configured_content_sizes)>0:
    for content_size in configured_content_sizes:
      if content_size.content_type == content_type:
        size = content_size.size
  if content_type == "default" and not size:
    size = configuration.DEFAULT_MAX_CONTENT_SIZE
    
  return size

class SimpleHttpFetcher(FetcherBase):
  """To fetch the web pages."""

  def __init__(self, max_shards, fetcher_policy=None ):
    """Initializes a SimpleHttpFetcher class.
  
    Args:
      max_shards: number of shard for web crawl process
      fetcher_policy: definition of policy for fetches
    
    Returns:
      The result of fetches """
    self._max_shards = max_shards
    self._fetcher_policy = fetcher_policy
    self._time = time.time

  def get(self, fetch_url):
    """To do HTTP GET request. 
    Args:
      fetch_url: url for fetche.
      
    Returns:
      Return results of HTTP GET request 
    
    Raises:
      HttpFetchError: if HTTP Status is errored.
      AbortedFetchException: if Content-Type is not valid.
      RedirectError: if redirect_mode is FOLLOW_NONE, and HTTP status is
        redirected.
    """
    
    self._read_start_time = self._time()
    is_follow_redirects = self._fetcher_policy.redirect_mode == configuration.FOLLOW_ALL
    result = urlfetch.fetch(url = fetch_url,
                            headers = _create_headers(self._fetcher_policy),
                            follow_redirects = is_follow_redirects,
                            deadline=float(self._fetcher_policy.request_timeout))
    headers = result.headers
    #Convert to lowercase
    for k, v in headers.items():
      headers[k.lower()] = v

    status_code = result.status_code
    logging.debug("status code: %d Content-Length: %s, Location: %s" % (status_code,
                  headers.get("content-length", ""),
                  headers.get("location", "")))
    
    #Fetch error was occurred.
    #If redirect_mode is FOLLOW_NONE, and HTTP status is redirected raises RedirectError.
    #If follow_redirects parameter set true, the service will follow the redirect.
    if status_code < 200 and status_code >= 300:
      raise errors.HttpFetchError("%s error fetching %s, %s" % (fetch_url,
                                  status_code,
                                  headers))
    elif status_code in REDIRECT_STATUSES and not is_follow_redirects:
      raise errors.RedirectError("RedirectMode disallowed redirect: %s, %s" % (self._fetcher_policy.redirect_mode,
                                  status_code))
      
    fetched_url = result.final_url
    if fetched_url is None:
      fetched_url = fetch_url
    #Get the mime_type and Check if we should abort due to mime-type filtering.  
    mime_type = _get_mime_type(headers.get("content-type"))
    mime_types = (self._fetcher_policy.valid_mime_types).split(",")
    if mime_types and len(mime_types):
      if mime_type and mime_type not in mime_types:
        raise errors.AbortedFetchError("%s Invalid mime-type: %s" % (fetch_url,
                                            mime_type ))
    
    #encoding if accept gzip
    content = result.content
    encoding = headers.get("content-encoding")
    if encoding == "gzip" or encoding == "x-gzip":
      f = StringIO.StringIO(content)
      c = gzip.GzipFile(fileobj=f)
      content = c.read()

    #Figure out how much data we want to try to fetch.
    max_content_size = _get_max_content_size(mime_type, self._fetcher_policy)
    target_length = max_content_size
    truncated = False
    content_length = headers.get("content-length")
    if content_length is not None:
      if int(content_length) > target_length or result.content_was_truncated:
        truncated = True
      else:
        target_length = int(content_length)
    else:
      content_length = "0"

    #Assume read time is at least one millisecond
    total_read_time = max(1, self._time() - self._read_start_time)
    #Abort if server response is slow.
    min_response_rate = int(self._fetcher_policy.min_response_rate)
    read_rate = int(content_length) / total_read_time
    if read_rate < min_response_rate:
      raise errors.AbortedFetchError("%s Slow response rate of %s bytes/sec" % (fetch_url,
                                          read_rate))
    logging.debug("FetchedURL:"+ fetch_url)
    #Get the content from response. 
    #Truncate content if target content size is lager than max content size
    content = content[:target_length]
    #Toss truncated image content.
    content_text = ""
    content_binary = None
    if mime_type and mime_type not in TEXT_MIME_TYPES:
      if truncated:
        raise errors.AbortedFetchError("%s Truncated image" % fetch_url)
      else:
        if re.search("\+xml", mime_type):
          content_text = content
        else:
          content_binary = content
    else:
      content_text = content

    return {"url": fetch_url,
            "fetched_url": fetched_url,
            "time": self._time(),
            "content_text": content_text,
            "content_binary": content_binary,
            "content_length": int(target_length),
            "mime_type": mime_type,
            "read_rate": int(read_rate),
            "headers": result.headers}
    
  def abort(self):
    """ Abort job 
    Actually try to abort """

