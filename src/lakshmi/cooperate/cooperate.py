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

from mapreduce import base_handler

class ExportCloudStoragePipeline(base_handler.PipelineBase):
  """Pipeline to export to cloud storage.

  Args:
    job_name: job name as string.
    params: parameters for DatastoreInputReader,
      that params use to CrawlDbDatum.
    shard_count: shard count for mapreduce.

  Returns:
    file_names: output path of score results.
  """

  
