#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2013-5-22

@author: Chine
'''

from cola.core.config import PropertyObject, Config
from cola.job.conf import main_conf

class Context(object):
    def __init__(self, user_conf=None, **user_defines):
        self.main_conf = main_conf
        if user_conf is not None:
            if isinstance(user_conf, str):
                self.user_conf = Config(user_conf)
            else:
                self.user_conf = user_conf
        else:
            self.user_conf = PropertyObject(dict())
        self.user_defines = PropertyObject(user_defines)
        
        dicts = PropertyObject({})
        for obj in (self.main_conf, self.user_conf, self.user_defines):
            dicts.update(obj)
        for k in dicts:
            if not k.startswith('_'):
                setattr(self, k, getattr(dicts, k))