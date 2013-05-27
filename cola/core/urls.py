#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2013-5-21

@author: Chine
'''

class UrlPattern(object):
    def __init__(self, url_re, name, parser):
        self.url_re = url_re
        self.name = name
        self.parser = parser
        
    def match(self, url):
        return self.url_re.match(url) is not None
        
class UrlPatterns(object):
    def __init__(self, *urls):
        for url in urls:
            if not isinstance(url, UrlPattern):
                raise ValueError('urls must be Url instances')
        self.url_patterns = list(urls)
        
    def __add__(self, url_obj):
        if not isinstance(url_obj, UrlPattern):
            raise ValueError('url_obj must be an instance of Url')
        self.url_patterns.append(url_obj)
        return self
    
    def matches(self, urls):
        for url in urls:
            for pattern in self.url_patterns:
                if pattern.match(url):
                    yield url
                    break