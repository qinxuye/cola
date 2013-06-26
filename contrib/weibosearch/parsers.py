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

Created on 2013-6-27

@author: Chine
'''

import urlparse
import urllib

from cola.core.parsers import Parser
from cola.core.errors import DependencyNotInstalledError

from bundle import WeiboSearchBundle
from storage import MicroBlog, DoesNotExist, Q

try:
    from bs4 import BeautifulSoup
except ImportError:
    raise DependencyNotInstalledError('BeautifulSoup4')

try:
    from dateutil.parser import parse
except ImportError:
    raise DependencyNotInstalledError('python-dateutil')

class WeiboSearchParser(Parser):
    def __init__(self, opener=None, url=None, bundle=None, **kwargs):
        super(WeiboSearchParser, self).__init__(opener=opener, url=url, **kwargs)
        self.bundle = bundle
        self.keyword = bundle.label
        
    def get_weibo(self, mid, keyword):
        try:
            weibo = getattr(MicroBlog, 'objects').get(Q(mid=mid) & Q(keyword=keyword))
            return weibo, True
        except DoesNotExist:
            weibo = MicroBlog(mid=mid, keyword=keyword)
            weibo.save()
            return weibo, False
        
    def parse(self, url=None):
        url = url or self.url
        
        br = self.opener.spynner_open(url)
        self.opener.wait_for_selector('div.feed_lists')
        html = br.html
        soup = BeautifulSoup(html)
        
        finished = False
        
        dls = soup.find_all('dl', attrs={'class': 'feed_list'}, mid=True)
        for dl in dls:
            mid = dl['mid']
            weibo, finished = self.get_weibo(mid, self.keyword)
            
            if finished:
                break
            
            weibo.content = dl.find('p', attrs={'node-type': 'feed_list_content'}).text.strip()
            is_forward = dl.get('isforward') == '1'
            if is_forward:
                weibo.forward = dl.find(
                    'dt', attrs={'node-type': 'feed_list_forwardContent'}).text.strip()
            p = dl.select('p.info.W_linkb.W_textb')[0]
            weibo.created = parse(p.find('a', attrs={'class': 'date'})['title'])
            likes = p.find('a', attrs={'action-type': 'feed_list_like'}).text
            if '(' not in likes:
                weibo.likes = 0
            else:
                weibo.likes = int(likes.strip().split('(', 1)[1].strip(')'))
            forwards = p.find('a', attrs={'action-type': 'feed_list_forward'}).text
            if '(' not in forwards:
                weibo.forwards = 0
            else:
                weibo.forwards = int(forwards.strip().split('(', 1)[1].strip(')'))
            comments = p.find('a', attrs={'action-type': 'feed_list_comment'}).text
            if '(' not in comments:
                weibo.comments = 0
            else:
                weibo.comments = int(comments.strip().split('(', 1)[1].strip(')'))
                
            weibo.save()
            
        pages = soup.find('div', attrs={'class': 'search_page'})
        if len(list(pages.children)) == 0:
            finished = True
        else:
            next_page = pages.find_all('a')[-1]
            if next_page.text.strip() == u'下一页':
                next_href = next_page['href']
                if not next_href.startswith('http://'):
                    next_href = urlparse.urljoin('http://s.weibo.com', next_href)
                    url, query = tuple(next_href.split('&', 1))
                    base, key = tuple(url.rsplit('/', 1))
                    key = urllib.unquote(key)
                    url = '/'.join((base, key))
                    next_href = '&'.join((url, query))
                return [next_href], []
            else:
                finished = True
        
        if finished:
            bundle = WeiboSearchBundle(self.keyword, force=True)
            return [], [bundle]
        return [], []