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

__all__ = [
    "BadYamlError",
    "MultipleDocumentsInFpYaml",
    "HttpFetchError",
    "AbortedFetchError",
    ]

class Error(Exception):
  """Base-class for exceptions in this module."""

class BadYamlError(Error):
  """Raise when the mapreduce.yaml file is invalid."""

class MultipleDocumentsInFpYaml(BadYamlError):
  """There's more than one document in fetch_policy.yaml file."""
  
class HttpFetchError(Error):
  """Raise when the HTTP Status is errored."""

class RedirectError(Error):
  """Raise when the HTTP Status is errored."""
  
class AbortedFetchError(Error):
  """Raise when need the fetch abort"""
  
