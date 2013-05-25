#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2013-5-26

@author: Chine
'''

class Job(object):
    def __init__(self, name, url_collections, starts,
                 is_bundle=False, instances=1, user_conf=None):
        self.name = name
        self.url_collections = url_collections
        self.starts = starts
        
        self.is_bundle = is_bundle
        self.instances = instances
        self.user_conf = user_conf
        
    def add_url_collections(self, url_colls):
        self.url_collections += url_colls
        
    def add_urlpattern(self, url_pattern):
        self.url_collections += url_pattern
        
    def set_userconf(self, conf):
        self.user_conf = conf