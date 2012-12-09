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
"""Cooperate with Google Cloud Platform for mining fetched data.

Uses the App Engine MapReduce mapper pipeline to read entities
out of the App Engine Datastore, write processed entities into
Cloud Storage in CSV format, then starts another pipeline that
creates a other service's ingestion job.
"""

import logging
import re

from google.appengine.ext import ndb
from google.appengine.api import files

from mapreduce import base_handler
from mapreduce import mapreduce_pipeline

from lakshmi.cooperate.datum import ContentsDatum 

#Path of stored file names to cloud storage file.
TARGETS_FILE_PATH="/targets"

def clean_html(html):
  """
  Remove HTML markup from the given string.
  """
  # First we remove inline JavaScript/CSS:
  cleaned = re.sub(r"(?is)<(script|style).*?>.*?(</\1>)", "", html.strip())
  # Then we remove html comments. This has to be done before removing regular
  # tags since comments can contain '>' characters.
  cleaned = re.sub(r"(?s)<!--(.*?)-->[\n]?", "", cleaned)
  # Next we can remove the remaining tags:
  cleaned = re.sub(r"(?s)<.*?>", " ", cleaned)
  # Finally, we deal with whitespace
  cleaned = re.sub(r"&nbsp;", " ", cleaned)
  cleaned = re.sub(r"  ", " ", cleaned)
  cleaned = re.sub(r"  ", " ", cleaned)
  return cleaned.strip()

def _to_csv_map(entity_type):
  """Map function of parse contents and 
    create CSV format text.

  Args:
    entity_type: Export target of entity type.

  Returns:
    csv_format_text: the csv format string: url, content
  """
  data = ndb.Model.to_dict(entity_type)
  content = data.get("content_text", None)
  result = []
  if content is not None:
    cleaned_content = clean_html(content.replace(",", ""))
    result = filter(lambda l: " ".join(l.split()), cleaned_content.splitlines())

  csv_format_text = (",").join((data.get("url", ""), "".join(result)))
  yield("%s\n" % csv_format_text.encode("utf-8", "ignore"))

class ExportCloudStoragePipeline(base_handler.PipelineBase):
  """Pipeline to export to cloud storage.

  Args:
    job_name: job name as string.
    input_entity_kind: target of entity kind.
    gs_bucket_name: name of bucket for export.
    shards: shard count for mapreduce.

  Returns:
    file_names: output path of score results.
  """
  def run(self,
          job_name,
          input_entity_kind,
          gs_bucket_name,
          shards=8):
    file_names = yield mapreduce_pipeline.MapperPipeline(
        job_name,
        __name__ + "._to_csv_map",
        "mapreduce.input_readers.DatastoreInputReader",
        output_writer_spec="mapreduce.output_writers.FileOutputWriter",
        params={
          "input_reader":{
              "entity_kind": input_entity_kind,
              },
          "output_writer":{
              "filesystem": "gs",
              "gs_bucket_name": gs_bucket_name,
              "output_sharding":"none",
              }
        },
        shards=shards)
    yield SaveResultFileNames(file_names, gs_bucket_name)

class SaveResultFileNames(base_handler.PipelineBase):
  """Save the file names of stored to cloud storage,
    The format consists of a line format, the file name target.

  Args:
    file_names: stored file names.
    bucket_name: name of bucket of target file.
  """
  def run(self,
          file_names,
          bucket_name):
    #Save filenames to cloud storage
    #If already exists same filename, will override it.
    print("/gs/"+bucket_name+TARGETS_FILE_PATH)
    target_file = files.gs.create("/gs/"+bucket_name+TARGETS_FILE_PATH)
    #Append the filename to the Google Cloud Storage object.
    for file_name in file_names:
      with files.open(target_file, 'a') as fp:
        fp.write(file_name+"\n")
    files.finalize(target_file)

def _to_datastore_map(kv_content):
  """Map function of store to datastore.
    The kind of entity is defined in cooperate.models

  Args:
    content_csv: The csv format string of exported contents.

  Returns:
    url: The url string, that will key of inserted entity to datastore.
  """
  k, v = kv_content 
  parsed_csv = v.split(",")
  url = ""
  content = ""
  try:
    url = parsed_csv[0]
    content = parsed_csv[1]
  except Exception as e:
    logging.warning("Can't parse csv:" + v + ":" + e.message)

  ContentsDatum.insert_or_fail(
      url,
      parent=ndb.Key(ContentsDatum, url),
      url=url,
      content=content,
      description=None)
  yield url

class ImportCloudStoragePipeline(base_handler.PipelineBase):
  """Pipeline to import from cloud storage.
  
  Args:
    job_name: job name as string.
    gs_bucket_name: The bucket name of stored target files. 

  Returns:
    file_names: output path of score results.
  """
  def run(self,
          job_name,
          gs_bucket_name,
          shards=8):
    file_names = []
    try:
      with files.open("/gs/"+gs_bucket_name+TARGETS_FILE_PATH, 'r') as fp:
        targets = fp.read()
        file_names = targets.splitlines()
    except Exception:
      logging.warning("Can't find file:" + TARGETS_FILE_PATH)
    
    if len(file_names)>0:
      yield mapreduce_pipeline.MapperPipeline(
        job_name,
        __name__ + "._to_datastore_map",
        "lakshmi.cooperate.input_readers.GoogleStorageLineInputReader",
        output_writer_spec="mapreduce.output_writers.BlobstoreOutputWriter",
        params={
          "input_reader":{
            "file_paths": file_names,
            "gs_bucket_name": gs_bucket_name
          },
          "output_writer":{
            "mime_type": "text/plain"
          }
        },
        shards=shards)
