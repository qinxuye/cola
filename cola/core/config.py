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
    """
    Wrapper of dict, providing the ability to get the key by the property.

    As an instance:

    >>> obj = PropertyObject({'k': 'v'})
    >>> obj.k
    'v'
    >>> obj.update(k={'sk': 'sv'})
    >>> obj
    {'k': {'sk': 'sv'}}
    >>> obj.k.sk
    'sv'

    Remember that do not directly set the key like:
    >>> obj['nk'] = 'nv'
    >>> obj.nk
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
    AttributeError: 'PropertyObject' object has no attribute 'nk'
    >>> obj.update(nk='nv')
    >>> obj.nk
    'nv'
    """
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
        
        setattr(self, k, self[k])
        
    def _update(self, d):
        for k, v in d.iteritems():
            if not k.startswith('_'):
                self._set(k, v)
                    
    def update(self, config=None, **kwargs):
        """
        Update by either dict or :class:`Config`.

        :param config: either dict or instance of :class:`Config`
        :param kwargs:
        """
        self._update(kwargs)
        if config is not None:
            if isinstance(config, dict):
                self._update(config)
            else:
                self._update(config.conf)
    
    def has(self, k):
        return hasattr(self, k)


class Config(object):
    """
    Read a yaml config file, and store the value
    which actually is the instance of :class:`PropertyObject`
    to the ``conf`` property.
    """
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


class ReadOnlyConfig(Config):

    __inited = False

    def __init__(self, config_or_yaml_file):
        if isinstance(config_or_yaml_file, Config):
            super(Config, self).__init__(config_or_yaml_file)
        else:
            for k in dir(config_or_yaml_file):
                if not k.startswith('_'):
                    setattr(self, k, getattr(config_or_yaml_file, k))

        self.__inited = True

    def __setattr__(self, key, value):
        if self.__inited:
            raise AttributeError("This is a read-only config")
        else:
            super(ReadOnlyConfig, self).__setattr__(key, value)
    
conf_dir = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'conf')
main_conf = Config(os.path.join(conf_dir, 'main.yaml'))