#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2013-5-21

@author: Chine
'''

class Url(object):
    def __init__(self, url_re, name, parser):
        self.url_re = url_re
        self.name = name
        self.parser = parser
        
class UrlCollections(object):
    def __init__(self, *urls):
        for url in urls:
            if not isinstance(url, Url):
                raise ValueError('urls must be Url instances')
        self.url_collections = list(urls)
        
    def __add__(self, url_obj):
        if not isinstance(url_obj, Url):
            raise ValueError('url_obj must be an instance of Url')
        self.url_collections.append(url_obj)
        return self