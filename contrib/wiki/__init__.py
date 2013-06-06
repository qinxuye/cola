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

Created on 2013-5-29

@author: Chine
'''

import os
import re
import urlparse

from cola.core.urls import UrlPatterns, Url
from cola.core.parsers import Parser
from cola.core.opener import MechanizeOpener
from cola.core.errors import DependencyNotInstalledError
from cola.core.config import Config
from cola.job import Job

try:
    from BeautifulSoup import BeautifulSoup
except ImportError:
    raise DependencyNotInstalledError('BeautifulSoup')

try:
    from dateutil.parser import parse
except ImportError:
    raise DependencyNotInstalledError('python-dateutil')

try:
    from mongoengine import connect, DoesNotExist, \
                            Document, StringField, DateTimeField
except ImportError:
    raise DependencyNotInstalledError('mongoengine')

user_conf = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'wiki.yaml')
user_config = Config(user_conf)

starts = [start.url for start in user_config.job.starts]

mongo_host = user_config.job.mongo.host
mongo_port = user_config.job.mongo.port
db_name = user_config.job.db
connect(db_name, host=mongo_host, port=mongo_port)

class WikiDocument(Document):
    title = StringField()
    content = StringField()
    last_update = DateTimeField()

class WikiParser(Parser):
    def __init__(self, opener=None, url=None, **kw):
        opener_cls = MechanizeOpener
        super(WikiParser, self).__init__(opener=opener_cls, url=url, **kw)
        
    def store(self, title, content, last_update):
        try:
            doc = WikiDocument.objects.get(title=title)
            if last_update > doc.last_update:
                doc.content = content
                doc.last_update = last_update
                doc.update(upsert=True)
        except DoesNotExist:
            doc = WikiDocument(title=title, content=content, last_update=last_update)
            doc.save()
            
    def _extract(self, soup):
        if soup.head is None:
            return None, None, None
        
        title = soup.head.title.text
        if '-' in title:
            title = title.split('-')[0].strip()
        content = soup.find('div', attrs={'id': 'mw-content-text', 'class': 'mw-content-ltr'})
        while content.table is not None:
            content.table.extract()
        content = content.text.split('Preprocessor', 1)[0]
        last_update = soup.find('li', attrs={'id': 'footer-info-lastmod'}).text
        if u'on' in last_update:
            last_update = last_update.rsplit(u'on', 1)[1].strip('.')
            last_update = parse(last_update)
        else:
            last_update = last_update.rsplit(u'于', 1)[1].strip(u'。')
            last_update = re.sub(r'\([^\)]+\)\s', '', last_update)
            last_update = last_update.replace(u'年', '-').replace(u'月', '-').replace(u'日', '')
            last_update = parse(last_update)
        
        return title, content, last_update
    
    def parse(self, url=None):
        url = url or self.url
        opener = self.opener()
        
        lang = url.strip('http://').split('.', 1)[0]
        
        br = opener.browse_open(url)
        html = br.response().read()
        soup = BeautifulSoup(html)
        
        title, content, last_update = self._extract(soup)
        if title is None:
            return []
        title = title + ' ' + lang
        self.store(title, content, last_update)
        
        def _is_same(out_url):
            return out_url.rsplit('#', 1)[0] == url
        
        links = []
        for link in br.links():
            if link.url.startswith('http://'):
                out_url = link.url
                if not _is_same(out_url):
                    links.append(out_url)
            else:
                out_url = urlparse.urljoin(link.base_url, link.url)
                if not _is_same(out_url):
                    links.append(out_url)
        return links

url_patterns = UrlPatterns(
    Url(r'^http://(zh|en).wikipedia.org/wiki/[^(:|/)]+$', 'wiki_page', WikiParser)
)

def get_job():
    return Job('wikipedia crawler', url_patterns, MechanizeOpener, starts,
               instances=user_config.job.instances, user_conf=user_config)
    
if __name__ == "__main__":
    from cola.worker.loader import load_job
    load_job(os.path.dirname(os.path.abspath(__file__)))