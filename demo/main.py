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

""" This is a sample application that Lakshmi an web crawler.
"""

__author__ = """cloudysunny14@gmail.com (Kiyonari Harigae)"""

import re
import os

from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.ext import ndb
from google.appengine.ext import webapp
from google.appengine.api import mail
from google.appengine.api import memcache

from mapreduce import base_handler

from lakshmi import pipelines
from lakshmi.datum import CrawlDbDatum
from lakshmi.datum import LinkDbDatum
from lakshmi.datum import ContentDbDatum
from lakshmi.datum import FetchedDbDatum
from lakshmi.cooperate import cooperate
from jinja2 import Environment, FileSystemLoader

ENTITY_KIND = "lakshmi.datum.CrawlDbDatum"
#specified some urls
def htmlParser(key, content):
  outlinks = re.findall(r'href=[\'"]?([^\'" >]+)', content)
  CrawlDbDatum
  link_datums = []
  for link in outlinks:
    link_datum = LinkDbDatum(parent=key, link_url=link)
    link_datums.append(link_datum)
  ndb.put_multi(link_datums) 
  content_links = re.findall(r'src=[\'"]?([^\'" >]+)', content)
  return content_links

class FecherJobPipeline(base_handler.PipelineBase):
  def run(self, email):
    memcache.set(key="email", value=email)
    yield pipelines.FetcherPipeline("FetcherPipeline",
        params={
          "entity_kind": ENTITY_KIND
        },
        parser_params={
          "text/html": "main.htmlParser",
          "application/rss+xml": "main.htmlParser",
          "application/atom+xml": "main.htmlParser",
          "text/xml": "main.htmlParser"
        },
        shards=4)

  def finalized(self):
    """Sends an email to admins indicating this Pipeline has completed.

    For developer convenience. Automatically called from finalized for root
    Pipelines that do not override the default action.
    """
    status = 'successful'
    if self.was_aborted:
      status = 'aborted'
    url = memcache.get("url")
    email = memcache.get("email")
    base_dir = os.path.realpath(os.path.dirname(__file__))
    # Configure jinja for internal templates
    env = Environment(
        autoescape=True,
        extensions=['jinja2.ext.i18n'],
        loader=FileSystemLoader(
            os.path.join(base_dir, 'templates')
          )
        )
    subject = "Your Fetcher Job is "+status
    crawl_db_datum = crawl_db_datums = CrawlDbDatum.query(CrawlDbDatum.url==url).fetch()
    crawl_db_datum = crawl_db_datums[0]
    content_db_datums = ContentDbDatum.query(ancestor=crawl_db_datum.key).fetch_async()
    fetched_db_datums = FetchedDbDatum.query(ancestor=crawl_db_datum.key).fetch()
    attachments = []
    if len(fetched_db_datums)>0:
      fetched_db_datum = fetched_db_datums[0]
      attachments.append(("fetched_content.html", fetched_db_datum.fetched_content))
    link_db_datums = LinkDbDatum.query(ancestor=crawl_db_datum.key).fetch_async()
    html = env.get_template("mail_template.html").render(url=url,
        contents=content_db_datums, links=link_db_datums)
    attachments.append(("sendmail.html", html))
    sender = "cloudysunny14@gmail.com"
    mail.send_mail(sender=sender, to=email, subject=subject,
        body="FetchResults", html=html, attachments=attachments)

class FetchStart(webapp.RequestHandler):
  def get(self):
    url = self.request.get("target", default_value=None)
    email = self.request.get("email", default_value=None)
    if url is None:
      url = memcache.get("url")
    else:
      memcache.set(key="url", value=url)
    if email is None:
      return

    data = CrawlDbDatum(
          parent =ndb.Key(CrawlDbDatum, url),
          url=url,
          last_status=pipelines.UNFETCHED)
    data.put()
    pipeline = FecherJobPipeline(email) 
    pipeline.start()
    path = pipeline.base_path + "/status?root=" + pipeline.pipeline_id
    self.redirect(path)

class DeleteDatumHandler(webapp.RequestHandler):
  def get(self):
    pipeline = pipelines.CleanDatumPipeline("CleanAllDatumPipeline",
        params={
          "entity_kind": ENTITY_KIND
        },
        clean_all=True,
        shards=8)
    pipeline.start()
    path = pipeline.base_path + "/status?root=" + pipeline.pipeline_id
    self.redirect(path) 

#Cooperate Handlers
FETCHED_DATUM_ENTITY_KIND = "lakshmi.datum.FetchedDatum"
GS_BUCKET = "your_backet_name"

class ExportHandler(webapp.RequestHandler):
  def get(self):
    pipeline = cooperate.ExportCloudStoragePipeline("ExportCloudStoragePipeline",
        input_entity_kind="lakshmi.datum.FetchedDatum",
        gs_bucket_name=GS_BUCKET,
        shards=3)
    pipeline.start()
    path = pipeline.base_path + "/status?root=" + pipeline.pipeline_id
    self.redirect(path)

application = webapp.WSGIApplication(
                                     [("/start", FetchStart),
                                     ("/clean_all", DeleteDatumHandler),
                                     ("/export", ExportHandler)],
                                     debug=True)
                                     
def main():
  run_wsgi_app(application)

if __name__ == "__main__":
  main()
