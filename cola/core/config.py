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

import os

from cola.core.errors import DependencyNotInstalledError

try:
    import yaml
except ImportError:
    raise DependencyNotInstalledError('pyyaml')

class PropertyObject(dict):
    def __init__(self, d=None):
        d = d or {}
        super(PropertyObject, self).__init__()
        self._update(d)
        
    def _set(self, k, v):
        if isinstance(v, dict):
            v = PropertyObject(v)
        elif isinstance(v, list):
            v = [PropertyObject(itm) for itm in v]
        
        if k not in self or type(self[k]) != type(v):
            self[k] = v
        elif isinstance(v, (PropertyObject, dict)):
            self[k].update(**v)
        elif isinstance(v, list):
            self[k].extend(v)
        else:
            self[k] = v
        
        setattr(self, k, v)
        
    def _update(self, d):
        for k, v in d.iteritems():
            if not k.startswith('_'):
                self._set(k, v)
                    
    def update(self, config=None, **kwargs):
        self._update(kwargs)
        if config is not None:
            if isinstance(config, dict):
                self._update(config)
            else:
                self._update(config.conf)

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
            
        for k, v in self.conf.iteritems():
            if not k.startswith('_'):
                if isinstance(v, dict):
                    v = PropertyObject(v)
                setattr(self, k, v)
    
    def __getitem__(self, name):
        return getattr(self, name)
    
conf_dir = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'conf')
main_conf = Config(os.path.join(conf_dir, 'main.yaml'))