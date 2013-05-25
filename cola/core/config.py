#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2013-5-16

@author: Chine
'''

from cola.core.errors import DependencyNotInstalledError

try:
    import yaml
except ImportError:
    raise DependencyNotInstalledError('pyyaml')

class PropertyObject(dict): 
    def __getattr__(self, name):
        if name not in self:
            return
        attr = self[name]
        if isinstance(attr, dict):
            return PropertyObject(attr)
        elif isinstance(attr, list):
            return [PropertyObject(itm) for itm in attr]
        else:
            return attr

class Config(object):
    def __init__(self, yaml_file):
        if isinstance(yaml_file, str):
            f = open(yaml_file)
        else:
            f = yaml_file
        try:
            self.conf = PropertyObject(yaml.load(f))
        finally:
            f.close()
            
    def __getattr__(self, name):
        return getattr(self.conf, name)
    
    def __getitem__(self, name):
        return getattr(self.conf, name)