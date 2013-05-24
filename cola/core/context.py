#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2013-5-22

@author: Chine
'''

from cola.core.config import main_conf

class Context(object):
    def __init__(self, **kwargs):
        for k in main_conf:
            setattr(self, k, getattr(main_conf, k))
        for k, v in kwargs.iteritems():
            setattr(self, k, v)
            
    def update(self, sync_context):
        for k, v in sync_context.__dict__:
            if not k.startswith('_'):
                setattr(self, k, v)