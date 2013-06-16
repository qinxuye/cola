#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Copyright (c) 2013 Qin Xuye <qin@qinxuye.me>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Created on 2013-6-17

@author: Chine
'''

import os
import urlparse

from cola.core.urls import UrlPatterns, Url
from cola.core.parsers import Parser
from cola.core.opener import MechanizeOpener
from cola.core.errors import DependencyNotInstalledError
from cola.core.config import Config
from cola.core.extractor import Extractor
from cola.core.extractor.utils import host_for_url
from cola.job import Job

try:
    from bs4 import BeautifulSoup
except ImportError:
    raise DependencyNotInstalledError('BeautifulSoup4')

try:
    from mongoengine import connect, DoesNotExist, \
                            Document, StringField, URLField
except ImportError:
    raise DependencyNotInstalledError('mongoengine')

get_user_conf = lambda s: os.path.join(os.path.dirname(os.path.abspath(__file__)), s)
user_conf = get_user_conf('test.yaml')
if not os.path.exists(user_conf):
    user_conf = get_user_conf('generic.yaml')
user_config = Config(user_conf)

starts = [start.url for start in user_config.job.starts]

mongo_host = user_config.job.mongo.host
mongo_port = user_config.job.mongo.port
db_name = user_config.job.db
connect(db_name, host=mongo_host, port=mongo_port)

collection_name = user_config.job.collection.replace(' ', '_')
meta = {}
if collection_name is not None:
    meta['collection'] = collection_name

class GenericDocument(Document):
    title = StringField()
    content = StringField()
    url = URLField()
    
    meta = meta
    
class GenericParser(Parser):
    def __init__(self, opener=None, url=None, **kw):
        super(GenericParser, self).__init__(opener=opener, url=url, **kw)
        
        self.store = self._bool(kw.get('store', 'n'))
        self.extract = self._bool(kw.get('extract', 'n'))
        self.logger = kw.get('logger')
        
    def _bool(self, value):
        if isinstance(value, bool):
            return value
        
        value = value.lower()
        if value == 'y':
            return True
        return False
    
    def parse(self, url=None):
        url = url or self.url
        html = self.opener.open(url)
        
        base_url = host_for_url(url)
        if base_url is not None:
            base_url = 'http://%s' % base_url
        extractor = Extractor(html, base_url=base_url)
        
        title = extractor.title()
        links = [node['href'] for node in extractor.content().find_all('a', href=True)]
        
        if self.store:
            if self.extract:
                html = extractor.extract()
            
            try:
                doc = GenericDocument.objects.get(url=url)
                doc.title = title
                doc.content = html
                doc.update(upsert=True)
            except DoesNotExist:
                doc = GenericDocument(title=title, content=html, url=url)
                doc.save()
            
        return links
    
def get_job():
    urls = []
    for pattern in user_config.job.patterns:
        url_pattern = Url(pattern.regex, pattern.name, GenericParser, 
                          store=pattern.store, extract=pattern.extract)
        urls.append(url_pattern)
    url_patterns = UrlPatterns(*urls)
    
    return Job(user_config.job.name, url_patterns, MechanizeOpener, starts,
               instances=user_config.job.instances, user_conf=user_config)
    
if __name__ == "__main__":
    from cola.worker.loader import load_job
    load_job(os.path.dirname(os.path.abspath(__file__)))