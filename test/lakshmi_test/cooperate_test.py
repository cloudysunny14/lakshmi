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

from mapreduce import test_support
from mapreduce.lib import pipeline
from testlib import testutil
from lakshmi.cooperate import cooperate
from lakshmi_test import pipeline_test

class ExportCloudStoragePipelineTest(testutil.HandlerTestBase):
  """Tests for ExportCloudStoragePipeline."""
  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []
  
  def _send_mail(self, sender, subject, body, html=None):
    """Callback function for sending mail."""
    self.emails.append((sender, subject, body, html))
  
  def testSuccessfulRun(self):
    
    p = cooperate.ExportCloudStoragePipeline("ExportCloudStoragePipeline",
            params={
              "entity_kind": "lakshmi.datum.FetchedDatum",
            },
            shard_count=3)
    p.start()
    test_support.execute_until_empty(self.taskqueue)
    finished_map = cooperate.ExportCloudStoragePipeline.from_id(p.pipeline_id)
    
    # Can open files
    file_paths = finished_map.outputs.default.value
    self.assertTrue(len(file_paths) > 0)
    self.assertTrue(file_paths[0].startswith("/gs/"))
