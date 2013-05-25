#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2013-5-21

@author: Chine
'''

class Parser(object):
    def __init__(self, opener, url=None):
        self.opener = opener
        if url is not None:
            self.url = url
        
    def parse(self, url=None):
        raise NotImplementedError