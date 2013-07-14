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

Created on 2013-6-16

@author: Chine
'''

import re

from cola.core.logs import get_logger

from cola.core.extractor.utils import absolute_url, beautiful_soup

__all__ = ['PreProcessor']

class Replacement(object):
    def __init__(self, desc, regex, replacement):
        self.desc = desc
        self.regex = regex
        self.replacement = replacement
    
    def apply(self, content):
        return self.regex.sub(self.replacement, content)
    
# a bunch of regexes to hack around lousy html
dodgy_regexes = (
    Replacement('javascript',
        regex=re.compile('<script.*?</script[^>]*>', re.DOTALL | re.IGNORECASE),
        replacement=''),

    Replacement('double double-quoted attributes',
        regex=re.compile('(="[^"]+")"+'),
        replacement='\\1'),

    Replacement('unclosed tags',
        regex = re.compile('(<[a-zA-Z]+[^>]*)(<[a-zA-Z]+[^<>]*>)'),
        replacement='\\1>\\2'),

    Replacement('unclosed (numerical) attribute values',
        regex = re.compile('(<[^>]*[a-zA-Z]+\s*=\s*"[0-9]+)( [a-zA-Z]+="\w+"|/?>)'),
        replacement='\\1"\\2'),
    )

# strip out a set of nuisance html attributes that can mess up rendering in RSS feeds
bad_attrs = ['width','height','style','[-a-z]*color','background[-a-z]*']
single_quoted = "'[^']+'"
double_quoted = '"[^"]+"'
non_space = '[^ "\'>]+'
htmlstrip = re.compile("<" # open
    "([^>]+) " # prefix
    "(?:%s) *" % ('|'.join(bad_attrs),) + # undesirable attributes
    '= *(?:%s|%s|%s)' % (non_space, single_quoted, double_quoted) + # value
    "([^>]*)"  # postfix
    ">"        # end
, re.I)

class PreProcessor(object):
    
    def __init__(self, html, base_url=None, logger=None):
        self.logger = logger
        if logger is None:
            self.logger = get_logger(name='cola_extractor')
        self.html = html
        self.base_url = base_url
        
    def _remove_crufy_html(self, html):
        for replacement in dodgy_regexes:
            html = replacement.apply(html)
        return html
            
    def _fix_absolute_links(self, base_url):
        for link in self.soup.find_all('a', href=True):
            link['href'] = absolute_url(link['href'], base_url)
    
    def _fix_absolute_images(self, base_url):
        for image in self.soup.find_all('img', src=True):
            image['src'] = absolute_url(image['src'], base_url)
            
    def _fix_references(self, base_url):
        self._fix_absolute_links(base_url)
        self._fix_absolute_images(base_url)
        
    def _normalize_space(self, s):
        return ' '.join(s.split())
    
    def get_title(self, soup):
        if soup.head is None or soup.head.title is None:
            title = ''
        else:
            title = soup.head.title.text
            title = self._normalize_space(title)
        return title
    
    def _clean_attributes(self, html):
        while htmlstrip.search(html):
            html = htmlstrip.sub('<\\1\\2>', html)
        return html
    
    def get_body(self, soup):
        for elem in soup.find_all(['script', 'link', 'style']):
            elem.extract()
        raw_html = unicode(soup.body or soup)
        cleaned = self._clean_attributes(raw_html)
        return beautiful_soup(cleaned)
    
    def process(self, base_url=None):
        self.html = self._remove_crufy_html(self.html)
        
        self.soup = beautiful_soup(self.html, self.logger)
        
        base_url = self.base_url or base_url
        if base_url is not None:
            self._fix_references(base_url)
            
        title = self.get_title(self.soup)
        body = self.get_body(self.soup)
        
        return title, body