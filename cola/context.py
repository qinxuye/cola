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

Created on 2014-2-7

@author: Chine
'''

import os
import tempfile

from cola.core.config import PropertyObject, Config
from cola.core.utils import get_ip

conf_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'conf')
main_conf = Config(os.path.join(conf_dir, 'main.yaml'))

class Settings(object): 
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
        self.values = dicts
                
class Context(object):
    def __init__(self, is_master, master, local_mode=False, is_client=False, 
                 working_dir=None, mkdirs=False, addr=None, addrs=None):
        self.is_master = is_master
        self.is_client = is_client
        self.is_local_mode = local_mode
        
        self.working_dir = working_dir
        if self.working_dir is None:
            tmp = tempfile.gettempdir()
            self.working_dir = os.path.join(tmp, 'cola')
            if mkdirs and not os.path.exists(self.working_dir):
                os.makedirs(self.working_dir)
                
        self.addr = addr
        if self.addr is None:
            self.addr = get_ip()
        if ':' not in self.addr:
            if is_master:
                port = main_conf.master.port
            elif is_client:
                port = main_conf.client.port
            else:
                port = main_conf.worker.port
            self.addr = '%s:%s' % (self.addr, port)
        self.addrs = addrs
        if self.addrs is None:
            self.addrs = []