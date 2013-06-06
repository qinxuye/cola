#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Copyright (c) 2013 Qin Xuye <qin@qinxuye.me>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

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