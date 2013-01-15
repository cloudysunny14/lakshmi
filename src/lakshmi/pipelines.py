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

import logging
import robotparser
import datetime
import re

from mapreduce.lib.files import file_service_pb

from urlparse import urlparse

from google.appengine.ext import ndb
from google.appengine.ext import blobstore
from google.appengine.api import memcache
from google.appengine.api import images
from google.appengine.api import files

from mapreduce import base_handler
from mapreduce import mapreduce_pipeline
from mapreduce import input_readers
from mapreduce import mapper_pipeline
from mapreduce.lib import pipeline
from mapreduce.lib import files
from mapreduce.lib.pipeline import common as pipeline_common
from mapreduce import output_writers
from mapreduce import util

from lakshmi import configuration
from lakshmi import fetchers
from lakshmi.datum import CrawlDbDatum
from lakshmi.datum import FetchedDbDatum
from lakshmi.datum import ContentDbDatum

#Define Fetch Status
UNFETCHED, FETCHED, FAILED, SKIPPED, EXPORTED = range(5) 

def getDomain(url):
  parsed_uri = urlparse(url)
  return '%s://%s' % (parsed_uri.scheme, parsed_uri.netloc)

def _extact_domain_map(entity_type):
  """Extract domain from url map function.
    
  Args:
    entity_type: The entity of crawl_db_datum.
    
  Returns:
    result: extracted domain name, value is none char
  """
  data = ndb.Model.to_dict(entity_type)
  extract_domain = ""
  fetch_status = data.get("last_status", 2) 
  if fetch_status == UNFETCHED:
    url = data.get("url")
    extract_domain = getDomain(url)
    entity_type.extract_domain_url = extract_domain
    entity_type.put()

  yield(extract_domain, "")

def _grouped_domain_reduce(key, values):
  """Grouping url reduce function."""
  cr = ""
  if(len(key)>0):
    cr = "\n"
  yield key + cr
  
class _ExactDomainMapreducePipeline(base_handler.PipelineBase):
  """Pipeline to execute exactDomain to fetch of MapReduce job.
  
  Args:
    job_name: job name as string.
    params: parameters for DatastoreInputReader,
      that params use to CrawlDbDatum.
    shard_count: shard count for mapreduce.

  Returns:
    file_names: output path of exact domains,
      that will generate to urls csv.
  """
  def run(self,
          job_name,
          params,
          shard_count):
    yield mapreduce_pipeline.MapreducePipeline(
        job_name,
        __name__ + "._extact_domain_map",
        __name__ + "._grouped_domain_reduce",
        "mapreduce.input_readers.DatastoreInputReader",
        "mapreduce.output_writers.BlobstoreOutputWriter",
        mapper_params=params,
        reducer_params={
            "mime_type": "text/plain",
        },
        shards=shard_count)

class _RobotsLineInputReader(input_readers.BlobstoreLineInputReader):
  """Reader that for robots fetch map job's files consists from line.
  
  This input reader behaver is same as BlobstoreLineInputReader,
  Override the split_input class method particular for RobotFetchJob.
  """
  
  @classmethod
  def split_input(cls, mapper_spec):
    """Returns a list of shard_count input_spec_shards for input_spec.

    Args:
      mapper_spec: The mapper specification to split from. Must contain
          'blob_keys' parameter with one or more blob keys.

    Returns:
      A list of BlobstoreInputReaders corresponding to the specified shards.
    """
    params = input_readers._get_params(mapper_spec)
    file_names = params[cls.BLOB_KEYS_PARAM]
    if isinstance(file_names, basestring):
      # This is a mechanism to allow multiple filenames (which do not contain
      # commas) in a single string. It may go away.
      file_names = file_names.split(",")

    blob_sizes = {}
    for file_name in file_names:
      blob_key = files.blobstore.get_blob_key(file_name)
      blob_key_str = str(blob_key)
      blob_info = blobstore.BlobInfo.get(blobstore.BlobKey(blob_key_str))
      blob_sizes[blob_key_str] = blob_info.size

    shard_count = min(cls._MAX_SHARD_COUNT, mapper_spec.shard_count)
    shards_per_blob = shard_count // len(file_names)
    if shards_per_blob == 0:
      shards_per_blob = 1

    chunks = []
    for blob_key, blob_size in blob_sizes.items():
      blob_chunk_size = blob_size // shards_per_blob
      for i in xrange(shards_per_blob - 1):
        chunks.append(input_readers.BlobstoreLineInputReader.from_json(
            {cls.BLOB_KEY_PARAM: blob_key,
             cls.INITIAL_POSITION_PARAM: blob_chunk_size * i,
             cls.END_POSITION_PARAM: blob_chunk_size * (i + 1)}))
      chunks.append(input_readers.BlobstoreLineInputReader.from_json(
          {cls.BLOB_KEY_PARAM: blob_key,
           cls.INITIAL_POSITION_PARAM: blob_chunk_size * (shards_per_blob - 1),
           cls.END_POSITION_PARAM: blob_size}))
    return chunks

  @classmethod
  def validate(cls, mapper_spec):
    """Validates mapper spec and all mapper parameters.

    Args:
      mapper_spec: The MapperSpec for this InputReader.

    Raises:
      BadReaderParamsError: required parameters are missing or invalid.
    """
    if mapper_spec.input_reader_class() != cls:
      raise input_readers.BadReaderParamsError("__RobotsLineInputReader:Mapper input reader class mismatch")
    params = input_readers._get_params(mapper_spec)
    if cls.BLOB_KEYS_PARAM not in params:
      raise input_readers.BadReaderParamsError("_RobotsLineInputReader:Must specify 'blob_keys' for mapper input")
    file_names = params[cls.BLOB_KEYS_PARAM]
    if isinstance(file_names, basestring):
      # This is a mechanism to allow multiple blob keys (which do not contain
      # commas) in a single string. It may go away.
      file_names = file_names.split(",")
    if len(file_names) > cls._MAX_BLOB_KEYS_COUNT:
      raise input_readers.BadReaderParamsError("_RobotsLineInputReader:Too many 'blob_keys' for mapper input")
    if not file_names:
      raise input_readers.BadReaderParamsError("_RobotsLineInputReader:No 'blob_keys' specified for mapper input")
    for file_name in file_names:
      blob_key = files.blobstore.get_blob_key(file_name)
      blob_key_str = str(blob_key)
      blob_info = blobstore.BlobInfo.get(blobstore.BlobKey(blob_key_str))
      if not blob_info:
        raise input_readers.BadReaderParamsError("_RobotsLineInputReader:Could not find blobinfo for key %s" %
                                   blob_key_str)

def _robots_fetch_map(data):
  """Map function of fetch robots.txt from page.

  Fetch robots.txt from Web Pages in specified url,
  Fetched result content will store to Blobstore,
  which will parse and set the score for urls.
  
  Args:
    data: key value data, that key is position, value is url.

  Returns:
    url: extract domain url.
    content: content of fetched from url's robots.txt
  """
  fetcher = fetchers.SimpleHttpFetcher(1, fetcher_policy_yaml.fetcher_policy)
  k, url = data
  logging.debug("data"+str(k)+":"+str(url))
  content = ""
  try:
    result = fetcher.get("%s/robots.txt" % str(url))
    content = result.get("content")
  except Exception as e:
    logging.warning("Robots.txt Fetch Error Occurs:" + e.message)
    content = "User-agent: *\nDisallow: /"

  yield (url, content)

class _RobotsFetchPipeline(base_handler.PipelineBase):
  """Pipeline to execute RobotFetch jobs.
  
  Args:
    job_name: job name as string.
    blob_keys: files which urls for fetch robots.txt are stored. 
    shards: number of shards.

  Returns:
    file_names: output path of fetch results.
  """
  def run(self,
          job_name,
          blob_keys,
          shards):
    yield mapreduce_pipeline.MapperPipeline(
      job_name,
      __name__ + "._robots_fetch_map",
      __name__ + "._RobotsLineInputReader",
      output_writer_spec=output_writers.__name__ + ".KeyValueBlobstoreOutputWriter" ,
      params={
            "blob_keys": blob_keys,
          },
      shards=shards)

fetcher_policy_yaml = configuration.FetcherPolicyYaml.create_default_policy()

def _makeFetchSetBufferMap(binary_record):
  """Map function of create fetch buffers,
  that output thus is one or more fetch url to fetch or skip.
  
  Arg:
    binary_record: key value data, that key is extract domain url,
      value is content from robots.txt.

  Returns:
    url: to fetch url.
    fetch_or_unfetch: the boolean value of fetch or unfetch,
      if sets true is fetch, false is skip.
  """
  proto = file_service_pb.KeyValue()
  proto.ParseFromString(binary_record)
  extract_domain_url = proto.key()
  content = proto.value()
  #Extract urls from CrawlDbDatum.
  try:
    query = CrawlDbDatum.query(CrawlDbDatum.extract_domain_url==extract_domain_url)
    crawl_datum_future = query.fetch_async()
  except Exception as e:
    logging.warning("Fetch error occurs from CrawlDbDatum" + e.message)

  can_fetch = False
  #Get the fetcher policy from resource.
  user_agent = fetcher_policy_yaml.fetcher_policy.agent_name
  rp = robotparser.RobotFileParser()
  try:
    rp.parse(content.split("\n").__iter__())
  except Exception as e:
    logging.warning("RobotFileParser raises exception:" + e.message) 
   
  for crawl_datum in crawl_datum_future.get_result():
    url = crawl_datum.url
    try:
      can_fetch = rp.can_fetch(user_agent, url)
    except Exception as e:
      logging.warning("RobotFileParser raises exception:" + e.message)
      url = ""
    
    yield (url, can_fetch)

class _FetchSetsBufferPipeline(base_handler.PipelineBase):
  """Pipeline to execute FetchSetsBuffer jobs.
  
  Args:
    job_name: job name as string.
    file_names: file names of fetch result of robots.txt.

  Returns:
    file_names: output path of fetch results.
  """
  def run(self,
          job_name,
          file_names):
    yield mapreduce_pipeline.MapperPipeline(
      job_name,
      __name__ + "._makeFetchSetBufferMap",
      "mapreduce.input_readers.RecordsReader",
      output_writer_spec=output_writers.__name__ + ".KeyValueBlobstoreOutputWriter" ,
      params={
            "files": file_names,
          },
      shards=len(file_names))

def _str2bool(v):
  return v.lower() in ("yes", "true", "t", "1")

def _fetchMap(binary_record):
  """Map function of create fetch result,
  that create FetchResulDatum entity, will be store to datastore. 

  Arg:
    binary_record: key value data, that key is url to fetch,
      value is boolean value of can be fetch.

  Returns:
    url: to fetch url.
    fetch_result: the result of fetch.
  """
  proto = file_service_pb.KeyValue()
  proto.ParseFromString(binary_record)
  url = proto.key()
  could_fetch = _str2bool(proto.value())
  result = UNFETCHED
  fetched_url = ""
  fetch_date = None
  #Fetch to CrawlDbDatum
  try:
    query = CrawlDbDatum.query(CrawlDbDatum.url==url)
    crawl_db_datum_future = query.fetch_async() 
  except Exception as e:
    logging.warning("Failed create key, caused by invalid url:" + url + ":" + e.message)
    could_fetch = False
  
  if could_fetch:
    #start fetch    
    fetcher = fetchers.SimpleHttpFetcher(1, fetcher_policy_yaml.fetcher_policy)
    try:
      fetch_result = fetcher.get(url)
      if fetch_result:
        #Storing to datastore
        crawl_db_datums = crawl_db_datum_future.get_result()
        fetche_datum = FetchedDbDatum(parent=crawl_db_datums[0].key,
            url=url, fetched_url = fetch_result.get("fetched_url"),
            fetch_time = fetch_result.get("time"), fetched_content = fetch_result.get("content"),
            content_type =  fetch_result.get("mime_type"),
            content_size = fetch_result.get("read_rate"),
            response_rate = fetch_result.get("read_rate"),
            http_headers = str(fetch_result.get("headers")))
        fetche_datum.put()
        #update time of last fetched 
        result = FETCHED
        fetch_date = datetime.datetime.now()
        fetched_url = ("%s\n"%url)
    except Exception as e:
      logging.warning("Fetch Page Error Occurs:" + e.message)
      result = FAILED
  else:
    result = FAILED
  
  #Update status to all datums.
  crawl_db_datums = crawl_db_datum_future.get_result()
  for datum in crawl_db_datums:
    datum.last_status = result
    datum.last_fetched = fetch_date
  ndb.put_multi(crawl_db_datums)

  yield fetched_url

class _FetchPagePipeline(base_handler.PipelineBase):
  """Pipeline to execute FetchPagePipeline jobs.
  
  Args:
    job_name: job name as string.
    file_names: file names of fetch result count and status 
    shards: number of shards.

  Returns:
    file_names: output path of fetch results.
  """
  def run(self,
          job_name,
          file_names,
          shards):
    yield mapreduce_pipeline.MapperPipeline(
      job_name,
      __name__ + "._fetchMap",
      "mapreduce.input_readers.RecordsReader",
      output_writer_spec=output_writers.__name__ + ".BlobstoreOutputWriter" ,
      params={
        "files": file_names,
      },
      shards=len(file_names))

_PARSER_PARAM_KEY = "PARSER_PARAM_KEY"

def _set_parser_param(key, params):
  memcache.set(key, params)

def _get_parser_param(key):
  return memcache.get(key) 

def _extract_content_urls_map(data):
  """Map function of extract outlinks from content.

  Function to be extracted and parsed to extract contents url with UDF.
  For example, You specified parser UDF for HTML, would like to
  fetch content from target page, and storing outlinks.
  implement default like this::

    def htmlParser(key, content):
      outlinks = re.findall(r'href=[\'"]?([^\'" >]+)', content)
      link_datums = []
      for link in outlinks:
        link_datum = LinkDbDatum(parent=key, link_url=link)
        link_datums.append(link_datum)
      ndb.put_multi_async(link_datums) 
      content_links = re.findall(r'src=[\'"]?([^\'" >]+)', content) 
      return content_links

  Note:Note:The above function to return the URL of the target of 
    url that will fetch in the next job(FetchContentPipeline)

  Args:
    data: key value data, that key is position, value is url.

  Returns:
    url: The page url.
  """
  k, url = data
  query = CrawlDbDatum.query(CrawlDbDatum.url==url)
  crawl_db_datum = query.fetch()
  key = crawl_db_datum[0].key
  fetched_datums = FetchedDbDatum.query(ancestor=key).fetch()
  fetched_datum = fetched_datums[0]
  content = None
  if fetched_datum is not None:
    content = fetched_datum.fetched_content
    mime_type = fetched_datum.content_type
    if content is not None:
      parsed_obj = None
      try:
        params = _get_parser_param(_PARSER_PARAM_KEY)
        parsed_obj = util.handler_for_name(params[mime_type])(key, content)
      except Exception as e:
        logging.warning("Can not handle for %s[params:%s]:%s"%(mime_type, params, e.message))
      if parsed_obj is not None:
        for content_urls in parsed_obj:
          yield (url, content_urls)
      
class _ExtractOutlinksPipeline(base_handler.PipelineBase):
  """Pipeline to execute ExtractOutlinksPipeline.

  Extract Content url from html,
  after the extract job,
  will fetch the content from extracted url in FetchContentPipeline

  Args:
    job_name: Name of job.
    blob_keys: input filenames, consists from results of fetch job.
    parser_params: params for extract outlink parser for each mime-types,
      the parser is user defined function for each mime-types, which returns 
      outlinks url list.
    shard_count: number of shards.
  """
  def run(self,
          job_name,
          file_names,
          parser_params,
          shard_count=8):
    _set_parser_param(_PARSER_PARAM_KEY, parser_params)
    yield mapreduce_pipeline.MapperPipeline(
      job_name,
      __name__ + "._extract_content_urls_map",
      __name__ + "._RobotsLineInputReader",
      output_writer_spec=output_writers.__name__ + ".KeyValueBlobstoreOutputWriter" ,
      params={
        "blob_keys": file_names,
      },
      shards=shard_count)

def _getCrawlDatum(crawl_db_datum_future):
  crawl_db_datums = crawl_db_datum_future.get_result()
  if len(crawl_db_datums)>0:
    return crawl_db_datums[0]
  return None

def _fetchContentMap(binary_record):
  """Map function of fetch content.
  Fetched content will store to blobstore.

  Arg:
    binary_record: key value data, that key is url of target page,
      value is url of target of fetch.

  Returns:
    url: fetched url.
  """
  proto = file_service_pb.KeyValue()
  proto.ParseFromString(binary_record)
  page_url = proto.key()
  target_url = proto.value()
  #Fetch to CrawlDbDatum
  try:
    query = CrawlDbDatum.query(CrawlDbDatum.url==page_url)
    crawl_db_datum_future = query.fetch_async() 
  except Exception as e:
    logging.warning("Failed create key, caused by invalid url:" + page_url + ":" + e.message)
  
  #start fetch    
  fetcher = fetchers.SimpleHttpFetcher(1, fetcher_policy_yaml.fetcher_policy)
  stored_url = None
  if re.match("^/", target_url):
    crawl_db_datum = _getCrawlDatum(crawl_db_datum_future)
    target_url = "%s%s" % (crawl_db_datum.extract_domain_url, target_url)

  try:
    fetch_result = fetcher.get(target_url)
    if fetch_result:
      #Storing to blobstore
      blob_io = files.blobstore.create(mime_type=fetch_result.get("mime_type"),
          _blobinfo_uploaded_filename=fetch_result.get("fetched_url"))
      with files.open(blob_io, 'a') as f:
        f.write(fetch_result.get("content"))
      files.finalize(blob_io)
      blob_key = files.blobstore.get_blob_key(blob_io)
      stored_url = images.get_serving_url(str(blob_key))
  except Exception as e:
    logging.warning("Fetch Error Occurs:" + e.message)

  #Put content to datastore.
  crawl_db_datum = _getCrawlDatum(crawl_db_datum_future)
  if crawl_db_datum and stored_url is not None:
    entity = ContentDbDatum(parent=crawl_db_datum.key,
          fetched_url=fetch_result.get("fetched_url"),
          stored_url=stored_url,
          content_type=fetch_result.get("mime_type"),
          content_size=fetch_result.get("content_length"),
          http_headers=str(fetch_result.get("headers")))
    entity.put()

  yield "%s:%s" % (target_url, stored_url)

class _FetchContentPipeline(base_handler.PipelineBase):
  """Pipeline to execute FetchContentPipeline.
  
  Fetch content from target pages.
  Passed records are urls to target of fetch.

  Args:
    job_name: job name as string.
    file_names: file names of stored target url records. 
    shards: number of shards

  Returns:
    file_names: output path of fetch results.
  """
  def run(self,
          job_name,
          file_names,
          shards=8):
    yield mapreduce_pipeline.MapperPipeline(
        job_name,
        __name__ + "._fetchContentMap",
        "mapreduce.input_readers.RecordsReader",
        output_writer_spec=output_writers.__name__ + ".BlobstoreOutputWriter" ,
        params={
        "files": file_names,
        },
        shards=shards)

class FetcherPipeline(base_handler.PipelineBase):
  """Pipeline to execute FetcherPipeline jobs.
  
  Args:
    job_name: job name as string.
    params: params for fetch job.
    parser_params: Params for extract outlink parser for each mime-types,
      The parser is user defined function for each mime-types, which returns 
      outlinks url list.
    shards: number of shard for fetch job.

  Returns:
    The list of filenames as string. Resulting files contain serialized
    file_service_pb.KeyValues protocol messages with all values collated
    to a single key.
  """
  def run(self,
          job_name,
          params,
          parser_params,
          shards=8):
    extract_domain_files = yield _ExactDomainMapreducePipeline(job_name,
        params=params,
        shard_count=shards)
    robots_files = yield _RobotsFetchPipeline(job_name, extract_domain_files, shards)
    fetch_set_buffer_files = yield _FetchSetsBufferPipeline(job_name, robots_files)
    fetch_files = yield _FetchPagePipeline(job_name, fetch_set_buffer_files, shards)
    outlinks_files = yield _ExtractOutlinksPipeline(job_name, fetch_files, parser_params, shards)
    results_files = yield _FetchContentPipeline(job_name, outlinks_files, shards)
    temp_files = [extract_domain_files, robots_files, fetch_set_buffer_files, fetch_files]
    with pipeline.After(results_files):
      all_temp_files = yield pipeline_common.Extend(*temp_files)
      yield mapper_pipeline._CleanupPipeline(all_temp_files)


def _clean_map(crawl_db_datum):
  """Delete entities map function.

  Delete unnecessary entities, also FetchedDbDatum. 

  Args:
    crawl_db_datum: The entity of crawl_db_datum.

  Returns:
    url_str: Deleted urls.
  """
  delete_keys = []
  clean_all = memcache.get(CLEAN_ALL_KEY)
  delete_fetched_datum =  FetchedDbDatum.get_by_id(crawl_db_datum.url)
  if delete_fetched_datum is not None:
    delete_keys.append(delete_fetched_datum.key)

  data = ndb.Model.to_dict(crawl_db_datum)
  fetch_status = data.get("last_status", 2)
  url=""
  clean_all = memcache.get(CLEAN_ALL_KEY)
  if clean_all:
    delete_keys.append(crawl_db_datum.key)
  else:
    if fetch_status in [FETCHED, SKIPPED, FAILED]:
      delete_keys.append(crawl_db_datum.key)

  ndb.delete_multi(delete_keys)

  yield(url+"\n")
  

CLEAN_ALL_KEY = "CLEAN_ALL"

class CleanDatumPipeline(base_handler.PipelineBase):
  """Pipeline to execute CleanDatum jobs.

  This pipeline is to delete unnecessary entity from datastore,
  that affect page scoring jobs to efficiency.
  
  Args:
    job_name: name of jobs.
    params: specifing name of the datums kind.
    shards: number of shards.

  Returns:
    file_names: The result files.
  """
  def run(self,
          job_name,
          params,
          clean_all=False,
          shards=8):
    memcache.set(key=CLEAN_ALL_KEY, value=clean_all)
    yield mapreduce_pipeline.MapperPipeline(
      job_name,
      __name__+"._clean_map",
      "mapreduce.input_readers.DatastoreInputReader",
      output_writer_spec=output_writers.__name__ + ".BlobstoreOutputWriter" ,
      params=params,
      shards=shards)

