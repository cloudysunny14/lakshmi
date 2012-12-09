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

from google.appengine.api import files

from mapreduce.lib import pipeline
from testlib import testutil
from mapreduce import test_support
from lakshmi.datum import FetchedDatum
from lakshmi.cooperate import cooperate
from lakshmi.cooperate.datum import ContentsDatum

TEST_BUCKET_NAME = "test_bucket"

def createFetchedDatum():
  """Create fetched datum for test"""
  path = os.path.join(os.path.dirname(__file__), "resource", "cloudysunny14.html")
  resource = open(path)
  static_content = resource.read()
  FetchedDatum.get_or_insert("cloudysunny14.html",
      url="cloudysunny14.html", fetched_url = "cloudysunny14.html",
      content_text = static_content, content_type="text/html")

def createCloudStorageFiles(files_num, content):
  """Create cloudstorage file for test"""
  file_names = []
  for num in range(files_num):
    file_path = "/gs/"+TEST_BUCKET_NAME+"/contents_"+str(num)
    file_name =  files.gs.create(file_path)
    for text in content:
      with files.open(file_name, 'a') as fp:
        fp.write(text+"\n")
    files.finalize(file_name) 
    file_names.append(file_path)
  return file_names 

class ExportPipelineTest(testutil.HandlerTestBase):
  """Tests for ExportCloudStoragePipeline."""
  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []
    createFetchedDatum()
  
  def _send_mail(self, sender, subject, body, html=None):
    """Callback function for sending mail."""
    self.emails.append((sender, subject, body, html))
  
  def testSuccessfulRun(self):
    p = cooperate.ExportCloudStoragePipeline("ExportCloudStoragePipeline",
          input_entity_kind="lakshmi.datum.FetchedDatum",
          gs_bucket_name=TEST_BUCKET_NAME,
          shards=3)
    p.start()
    test_support.execute_until_empty(self.taskqueue)
    cooperate.ExportCloudStoragePipeline.from_id(p.pipeline_id)
    
    file_list = files.listdir("/gs/"+TEST_BUCKET_NAME)
    self.assertTrue(len(file_list) > 0)
    for file_name in file_list:
      self.assertTrue(file_name.startswith("/gs/"))

class ImportPipelineTest(testutil.HandlerTestBase):
  """Tests for ImportCloudStoragePipeline."""
  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []
    file_name_list = createCloudStorageFiles(3, ["URL1,Content1\nURL2,Content2",
        "URL3,Content3\nURL4,Content4", "URL5,Content5\nURL6,Content6"])
    file_name_lines = "\n".join(file_name_list)
    target_file =  files.gs.create("/gs/"+TEST_BUCKET_NAME+"/targets")
    with files.open(target_file, 'a') as fp:
      fp.write(file_name_lines)
    files.finalize(target_file)

  def _send_mail(self, sender, subject, body, html=None):
    """Callback function for sending mail."""
    self.emails.append((sender, subject, body, html))

  def testSuccessfulRun(self):
    p = cooperate.ImportCloudStoragePipeline("ImportCloudStoragePipeline",
          gs_bucket_name=TEST_BUCKET_NAME,
          shards=3)
    p.start()
    test_support.execute_until_empty(self.taskqueue)
    cooperate.ImportCloudStoragePipeline.from_id(p.pipeline_id)
    
    # Fetch from 
    entities = ContentsDatum.query().fetch()
    self.assertTrue(len(entities))

if __name__ == "__main__":
  unittest.main()

