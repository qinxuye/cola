#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2013-5-16

@author: Chine
'''

import os

import yaml

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
        f = open(yaml_file)
        try:
            self.conf = PropertyObject(yaml.load(f))
        finally:
            f.close()
            
    def __getattr__(self, name):
        return getattr(self.conf, name)
    
    def __getitem__(self, name):
        return getattr(self.conf, name)
    
conf_base_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'conf')
conf_path = lambda name: os.path.join(conf_base_path, name)

main_conf = Config(conf_path('main.yaml'))