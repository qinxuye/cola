#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2013-5-26

@author: Chine
'''

import re

from cola.core.errors import ConfigurationError
from cola.job.context import Context

JOB_NAME_RE = re.compile(r'(\w| )+')

class Job(object):
    def __init__(self, name, url_patterns, opener_cls, starts,
                 is_bundle=False, unit_cls=str,
                 instances=1, user_conf=None,
                 login_hook=None):
        self.name = name
        if not JOB_NAME_RE.match(name):
            raise ConfigurationError('Job name can only contain alphabet, number and space.')
        self.url_patterns = url_patterns
        self.opener_cls = opener_cls
        self.starts = starts
        
        self.is_bundle = is_bundle
        self.unit_cls = unit_cls
        self.instances = instances
        self.user_conf = user_conf
        self.login_hook = login_hook
        
        self.context = Context(user_conf=user_conf)
        
    def add_urlpattern(self, url_pattern):
        self.url_patterns += url_pattern
        
    def set_userconf(self, conf):
        self.user_conf = conf