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

Created on 2013-5-21

@author: Chine
'''

import re

class Url(object):
    def __init__(self, url_re, name, parser, **kw):
        self.url_re = re.compile(url_re, re.IGNORECASE)
        self.name = name
        self.parser = parser
        self.options = kw
        
    def match(self, url):
        return self.url_re.match(url) is not None
        
class UrlPatterns(object):
    def __init__(self, *urls):
        for url in urls:
            if not isinstance(url, Url):
                raise ValueError('urls must be Url instances')
        self.url_patterns = list(urls)
        
    def __add__(self, url_obj):
        if not isinstance(url_obj, Url):
            raise ValueError('url_obj must be an instance of Url')
        self.url_patterns.append(url_obj)
        return self
    
    def matches(self, urls, pattern_names=None):
        for url in urls:
            for pattern in self.url_patterns:
                if pattern_names is not None and \
                    pattern.name not in pattern_names:
                    continue
                if pattern.match(url):
                    yield url
                    break
                
    def get_parser(self, url, pattern_names=None, options=False):
        for pattern in self.url_patterns:
            if pattern.match(url):
                if pattern_names is not None and \
                    pattern.name not in pattern_names:
                    continue
                
                if options is True:
                    return pattern.parser, pattern.options
                return pattern.parser