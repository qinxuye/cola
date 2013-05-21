#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2013-5-21

@author: Chine
'''

class Parser(object):
    def __init__(self, opener):
        self.opener = opener
        
    def parse(self, html=None):
        raise NotImplementedError