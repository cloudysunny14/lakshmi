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
import shutil
import tempfile

from lakshmi import configuration
from lakshmi import errors

class FetcherPolicyYamlTest(unittest.TestCase):
  """Testing fetcher_policy.yaml-related functionality."""

  def set_up_directory_tree(self, dir_tree_contents):
    """Create directory tree from dict of path:contents entries."""
    for full_path, contents in dir_tree_contents.iteritems():
      dir_name = os.path.dirname(full_path)
      if not os.path.isdir(dir_name):
        os.makedirs(dir_name)
      f = open(full_path, 'w')
      f.write(contents)
      f.close()

  def setUp(self):
    """Initialize temporary application variable."""
    self.tempdir = tempfile.mkdtemp()

  def tearDown(self):
    """Remove temporary application directory."""
    if self.tempdir:
      shutil.rmtree(self.tempdir)

  def testFindYamlFile(self):
    """Test if mapreduce.yaml can be found with different app/library trees."""
    test_conf = os.path.join(self.tempdir, "library_root", "lakshmi", "configuration.py")
    test_fetcher_policy_yaml = os.path.join(self.tempdir, "application_root",
                                       "fetcher_policy.yaml")
    test_dict = {
        test_conf: "test",
        test_fetcher_policy_yaml: "test",
    }
    self.set_up_directory_tree(test_dict)
    os.chdir(os.path.dirname(test_fetcher_policy_yaml))
    yaml_loc = configuration.find_fetcher_policy_yaml(conf_file=test_conf)
    self.assertEqual(("/private%s" % test_fetcher_policy_yaml), yaml_loc)

  def testFindYamlFileSameTree(self):
    """Test if fetcher_policy.yaml can be found with the same app/library tree."""
    test_conf = os.path.join(self.tempdir, "library_root", "lakshmi", "configuration.py")
    test_fetcher_policy_yaml = os.path.join(self.tempdir, "application_root",
                                       "fetcher_policy.yaml")
    test_dict = {
        test_conf: "test",
        test_fetcher_policy_yaml: "test",
    }
    self.set_up_directory_tree(test_dict)
    os.chdir(os.path.dirname(test_fetcher_policy_yaml))
    yaml_loc = configuration.find_fetcher_policy_yaml(conf_file=test_conf)
    self.assertEqual(("/private%s" % test_fetcher_policy_yaml), yaml_loc)

  def testParseEmptyFile(self):
    """Parsing empty mapreduce.yaml file."""
    self.assertRaises(errors.BadYamlError,
                      configuration.parse_fetcher_policy_yaml,
                      "")

  def testParse(self):
    """Parsing a single document in fetcher_policy.yaml."""
    fetcher_policy_yaml = configuration.parse_fetcher_policy_yaml(
        "fetcher_policy:\n"
        "  agent_name: test\n"
        "  email_address: test@domain.com\n"
        "  web_address: http://test.domain.com\n"
        "  min_response_rate: 0\n"
        "  max_content_size:\n"
        "  - content_type: default\n"
        "    size: 1000\n"
        "  crawl_end_time: 15000\n"
        "  crawl_delay: 0\n"
        "  max_redirects: 20\n"
        "  accept_language: en-us,en-gb,en;q=0.7,*;q=0.3\n"
        "  valid_mime_types: text/html\n"
        "  redirect_mode: follow_all\n"
        "  request_timeout: 20000\n"
        "  max_links_per_page: 10")

    self.assertTrue(fetcher_policy_yaml)
    self.assertTrue("test", fetcher_policy_yaml.fetcher_policy.agent_name)
    self.assertTrue("test@domain.com", fetcher_policy_yaml.fetcher_policy.email_address)
    self.assertTrue("http://test.domain.com", fetcher_policy_yaml.fetcher_policy.web_address)
    self.assertEquals("0", fetcher_policy_yaml.fetcher_policy.min_response_rate)
    self.assertTrue(fetcher_policy_yaml.fetcher_policy.max_content_size)
    max_content_sizes = fetcher_policy_yaml.fetcher_policy.max_content_size
    self.assertTrue(1, len(max_content_sizes))
    max_content_size = max_content_sizes[0]
    self.assertTrue("default", max_content_size.content_type)
    self.assertTrue("1000", max_content_size.size)
    self.assertEquals("15000", fetcher_policy_yaml.fetcher_policy.crawl_end_time)
    self.assertEquals("0", fetcher_policy_yaml.fetcher_policy.crawl_delay)
    self.assertEquals("20", fetcher_policy_yaml.fetcher_policy.max_redirects)
    self.assertEquals("en-us,en-gb,en;q=0.7,*;q=0.3",
                       fetcher_policy_yaml.fetcher_policy.accept_language)
    self.assertEquals("text/html", fetcher_policy_yaml.fetcher_policy.valid_mime_types)
    self.assertEquals("follow_all", fetcher_policy_yaml.fetcher_policy.redirect_mode)
    self.assertEquals("20000", fetcher_policy_yaml.fetcher_policy.request_timeout)
    self.assertEquals("10", fetcher_policy_yaml.fetcher_policy.max_links_per_page)

  def testParseMissingRequiredAttrs(self):
    """Test parsing with missing required attributes."""
    self.assertRaises(errors.BadYamlError,
                      configuration.parse_fetcher_policy_yaml,
                      "fetcher_policy:\n"
                      "  min_response_rate: 0\n"
                      "  max_content_size: 3000\n"
                      "  crawl_end_time: 15000\n" )

  def testBadValues(self):
    """Tests when some yaml values are of the wrong type."""
    self.assertRaises(errors.BadYamlError,
                      configuration.parse_fetcher_policy_yaml,
                      "fetcher_policy:\n"
                      "  min_response_rate: 0\n"
                      "  max_content_size: 3000\n"
                      "  crawl_end_time: 15000\n" 
                      "  accept_language: $$Invalid$$\n")

  def testMultipleDocuments(self):
    """Tests when multiple documents are present."""
    self.assertRaises(errors.BadYamlError,
                      configuration.parse_fetcher_policy_yaml,
                      "fetcher_policy:\n"
                      "  min_response_rate: 0\n"
                      "  max_content_size: 3000\n"
                      "  crawl_end_time: 15000\n" 
                      "---")

  def testToDict(self):
    """Tests encoding the FP document as JSON."""
    fp_yaml = configuration.parse_fetcher_policy_yaml(
        "fetcher_policy:\n"
        "  agent_name: test\n"
        "  email_address: test@domain.com\n"
        "  web_address: http://test.domain.com\n"
        "  min_response_rate: 0\n"
        "  max_content_size:\n"
        "  - content_type: default\n"
        "    size: 1000\n"
        "  - content_type: image/png\n"
        "    size: 5000\n"
        "  crawl_end_time: 15\n"
        "  crawl_delay: 0\n"
        "  max_redirects: 20\n"
        "  accept_language: en-us,en-gb,en;q=0.7,*;q=0.3\n"
        "  valid_mime_types: text/html\n"
        "  redirect_mode: follow_all\n"
        "  request_timeout: 20\n"
        "  max_links_per_page: 10\n")
    all_configs = configuration.FetcherPolicyYaml.to_dict(fp_yaml)
    self.assertEquals(
      {
            'agent_name': "test",
            'email_address': "test@domain.com",
            'web_address': "http://test.domain.com",
            'min_response_rate': "0",
            'max_content_size':[{"content_type": "default",
                                 "size": "1000"},
                                {"content_type": "image/png",
                                 "size": "5000"}],
            'crawl_end_time': "15",
            'crawl_delay': "0",
            'max_redirects': "20",
            'accept_language': "en-us,en-gb,en;q=0.7,*;q=0.3",
            'valid_mime_types': "text/html",
            'redirect_mode': "follow_all",
            'request_timeout': "20",
            'max_links_per_page': "10"
      }, all_configs)

class ScoreConfigYamlTest(unittest.TestCase):
  """Testing fetcher_policy.yaml-related functionality."""
  
  def set_up_directory_tree(self, dir_tree_contents):
    """Create directory tree from dict of path:contents entries."""
    for full_path, contents in dir_tree_contents.iteritems():
      dir_name = os.path.dirname(full_path)
      if not os.path.isdir(dir_name):
        os.makedirs(dir_name)
      f = open(full_path, 'w')
      f.write(contents)
      f.close()

  def setUp(self):
    """Initialize temporary application variable."""
    self.tempdir = tempfile.mkdtemp()

  def tearDown(self):
    """Remove temporary application directory."""
    if self.tempdir:
      shutil.rmtree(self.tempdir)

  def testFindYamlFile(self):
    """Test if mapreduce.yaml can be found with different app/library trees."""
    test_conf = os.path.join(self.tempdir, "library_root", "lakshmi", "configuration.py")
    test_score_config_yaml = os.path.join(self.tempdir, "application_root",
                                       "score_config.yaml")
    test_dict = {
        test_conf: "test",
        test_score_config_yaml: "test",
    }
    self.set_up_directory_tree(test_dict)
    os.chdir(os.path.dirname(test_score_config_yaml))
    yaml_loc = configuration.find_score_config_yaml(conf_file=test_conf)
    self.assertEqual(("/private%s" % test_score_config_yaml), yaml_loc)

  def testFindYamlFileSameTree(self):
    """Test if fetcher_policy.yaml can be found with the same app/library tree."""
    test_conf = os.path.join(self.tempdir, "library_root", "lakshmi", "configuration.py")
    test_score_config_yaml = os.path.join(self.tempdir, "application_root",
                                       "score_config.yaml")
    test_dict = {
        test_conf: "test",
        test_score_config_yaml: "test",
    }
    self.set_up_directory_tree(test_dict)
    os.chdir(os.path.dirname(test_score_config_yaml))
    yaml_loc = configuration.find_score_config_yaml(conf_file=test_conf)
    self.assertEqual(("/private%s" % test_score_config_yaml), yaml_loc)

  def testParseEmptyFile(self):
    """Parsing empty mapreduce.yaml file."""
    self.assertRaises(errors.BadYamlError,
                      configuration.parse_score_config_yaml,
                      "")

  def testParse(self):
    """Parsing a single document in score_config.yaml."""
    score_config_yaml = configuration.parse_score_config_yaml(
        "score_config:\n"
        "  score_query: Python,Google App Engine\n"
        "  adopt_score: 0.5\n")

    self.assertTrue(score_config_yaml)
    self.assertTrue("Python,Google App Engine", score_config_yaml.score_config.score_query)
    self.assertTrue("test@domain.com", score_config_yaml.score_config.adopt_score)

  def testParseMissingRequiredAttrs(self):
    """Test parsing with missing required attributes."""
    self.assertRaises(errors.BadYamlError,
                      configuration.parse_score_config_yaml,
                      "score_config:\n"
                      "  score_query: 0\n")

  def testBadValues(self):
    """Tests when some yaml values are of the wrong type."""
    self.assertRaises(errors.BadYamlError,
                      configuration.parse_fetcher_policy_yaml,
                      "score_config:\n"
                      "  score_query: 0\n"
                      "  adopt_score: $$Invalid$$\n")

  def testToDict(self):
    """Tests encoding the SC document as JSON."""
    sc_yaml = configuration.parse_score_config_yaml(
        "score_config:\n"
        "  score_query: Python,Google App Engine\n"
        "  adopt_score: 0.5\n")
    all_configs = configuration.ScoreConfigYaml.to_dict(sc_yaml)
    self.assertEquals(
      {
            'score_query': "Python,Google App Engine",
            'adopt_score': "0.5"
      }, all_configs)

class UrlFilterYamlTest(unittest.TestCase):
  """Testing url_filter.yaml-related functionality."""
  
  def set_up_directory_tree(self, dir_tree_contents):
    """Create directory tree from dict of path:contents entries."""
    for full_path, contents in dir_tree_contents.iteritems():
      dir_name = os.path.dirname(full_path)
      if not os.path.isdir(dir_name):
        os.makedirs(dir_name)
      f = open(full_path, 'w')
      f.write(contents)
      f.close()

  def setUp(self):
    """Initialize temporary application variable."""
    self.tempdir = tempfile.mkdtemp()

  def tearDown(self):
    """Remove temporary application directory."""
    if self.tempdir:
      shutil.rmtree(self.tempdir)

  def testFindYamlFile(self):
    """Test if url_filter.yaml can be found with different app/library trees."""
    test_conf = os.path.join(self.tempdir, "library_root", "lakshmi", "configuration.py")
    test_url_filter = os.path.join(self.tempdir, "application_root",
                                       "url_filter.yaml")
    test_dict = {
        test_conf: "test",
        test_url_filter: "test",
    }
    self.set_up_directory_tree(test_dict)
    os.chdir(os.path.dirname(test_url_filter))
    yaml_loc = configuration.find_url_filter_yaml(conf_file=test_conf)
    self.assertEqual(("/private%s" % test_url_filter), yaml_loc)

  def testFindYamlFileSameTree(self):
    """Test if url_filter.yaml can be found with the same app/library tree."""
    test_conf = os.path.join(self.tempdir, "library_root", "lakshmi", "configuration.py")
    test_url_filter_yaml = os.path.join(self.tempdir, "application_root",
                                       "url_filter.yaml")
    test_dict = {
        test_conf: "test",
        test_url_filter_yaml: "test",
    }
    self.set_up_directory_tree(test_dict)
    os.chdir(os.path.dirname(test_url_filter_yaml))
    yaml_loc = configuration.find_url_filter_yaml(conf_file=test_conf)
    self.assertEqual(("/private%s" % test_url_filter_yaml), yaml_loc)

  def testParseEmptyFile(self):
    """Parsing empty url_filter.yaml file."""
    self.assertRaises(errors.BadYamlError,
                      configuration.parse_url_filter_yaml,
                      "")

  def testParse(self):
    """Parsing a single document in url_filter.yaml."""
    url_filter_yaml = configuration.parse_url_filter_yaml(
        "domain_urlfilter:\n"
        "  - http://foo.com\n"
        "  - http://bar.com\n"
        "urlfilter:\n"
        "  - http://foo.com/bar/\n"
        "regex_urlfilter:\n"
        "  - (http://foo.com/tag/.+)\n"
        "  - (http://bar.com/category.+)\n")

    self.assertTrue(url_filter_yaml)
    self.assertTrue(str(["http://foo.com", "http://bar.com"]), str(url_filter_yaml.domain_urlfilter))
    self.assertTrue(str(["http://foo.com/bar/"]), str(url_filter_yaml.urlfilter))
    self.assertTrue(str(["(http://foo.com/tag/.+)", "(http://bar.com/category.+)"]), str(url_filter_yaml.regex_urlfilter))

  def testToDict(self):
    """Tests encoding the UF document as JSON."""
    uf_yaml = configuration.parse_url_filter_yaml(
        "domain_urlfilter:\n"
        "  - http://foo.com\n"
        "  - http://bar.com\n"
        "urlfilter:\n"
        "  - http://foo.com/bar/\n")
    all_configs = configuration.UrlFilterYaml.to_dict(uf_yaml)
    self.assertEquals(
      {
            "domain_urlfilter": ["http://foo.com", "http://bar.com"],
            "urlfilter": ["http://foo.com/bar/"],
            "regex_urlfilter": None
      }, all_configs)

if __name__ == "__main__":
  unittest.main()
