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

Created on 2014-5-2

@author: chine
'''

import os
try:
    import cPickle as pickle
except ImportError:
    import pickle

from cola.core.counter import Counter, MergeAggregator
from cola.core.rpc import client_call
from cola.core.utils import get_rpc_prefix

FUNC_PREFIX = 'counter_'
COUNTER_STATUS_FILENAME = 'counter.status'

class CounterServer(object):
    def __init__(self, working_dir, settings, 
                 rpc_server=None, app_name=None, dict_cls=dict):
        self.dir_ = working_dir
        self.settings = settings
        self.rpc_server = rpc_server
        self.app_name = app_name
        self.prefix = get_rpc_prefix(self.app_name, FUNC_PREFIX)
        self.dict_cls = dict_cls
        
        self.inc_counter = Counter(container=dict_cls())
        self.acc_counter = Counter(agg=MergeAggregator(), 
                                   container=dict_cls())
        self.load()
        
        self._register_rpc()
        
    def _register_rpc(self):
        if self.rpc_server is not None:
            self.rpc_server.register_function(self.inc, 'inc', 
                                              prefix=self.prefix)
            self.rpc_server.register_function(self.acc, 'acc',
                                              prefix=self.prefix)
            self.rpc_server.register_function(self.inc_merge, 'inc_merge',
                                              prefix=self.prefix)
            self.rpc_server.register_function(self.acc_merge, 'acc_merge',
                                              prefix=self.prefix)
    
    def shutdown(self):
        self.save()
    
    def load(self):
        save_file = os.path.join(self.dir_, COUNTER_STATUS_FILENAME)
        if os.path.exists(save_file):
            with open(save_file) as f:
                inc_counter_container, acc_counter_container = pickle.load(f)
                self.inc_counter.reset(self.dict_cls(inc_counter_container))
                self.acc_counter.reset(self.dict_cls(acc_counter_container))
                    
    def save(self):
        save_file = os.path.join(self.dir_, COUNTER_STATUS_FILENAME)
        with open(save_file, 'w') as f:
            t = (dict(self.inc_counter.container), dict(self.acc_counter.container))
            pickle.dump(t, f)
        
    def inc(self, group, item, val=1):
        self.inc_counter.inc(group, item, val=val)
        
    def acc(self, group, item, val):
        self.acc_counter.inc(group, item, val=val)
        
    def inc_merge(self, vals):
        counter = Counter(agg=self.inc_counter.agg, container=vals)
        self.inc_counter.merge(counter)
        
    def acc_merge(self, vals):
        counter = Counter(agg=self.acc_counter.agg, container=vals)
        self.acc_counter.merge(counter)
        
class CounterClient(object):
    def __init__(self, server, app_name=None):
        if isinstance(server, CounterServer):
            self.remote = False
        else:
            self.remote = True
        self.server = server
        self.app_name = app_name
        self.prefix = get_rpc_prefix(self.app_name, FUNC_PREFIX)
        
        self.inc_counter = Counter()
        self.acc_counter = Counter(agg=MergeAggregator())
        
    def local_inc(self, addr, instance_id, item, val=1):
        addr = '#'.join((addr, str(instance_id)))
        self.inc_counter.inc(addr, item, val=val)
        
    def global_inc(self, item, val=1):
        self.inc_counter.inc('global', item, val=val)
        
    def get_local_inc(self, addr, instance_id, item, default_val=None):
        addr = '#'.join((addr, str(instance_id)))
        return self.inc_counter.get(addr, item, default_val=default_val)
    
    def get_global_inc(self, item, default_val=None):
        return self.inc_counter.get('global', item, default_val=default_val)
        
    def local_acc(self, addr, instance_id, item, val):
        addr = '#'.join((addr, str(instance_id)))
        self.acc_counter.inc(addr, item, val=val)
        
    def global_acc(self, item, val):
        self.acc_counter.inc('global', item, val=val)
        
    def get_local_acc(self, addr, instance_id, item, default_val=None):
        addr = '#'.join((addr, str(instance_id)))
        return self.acc_counter.get(addr, item, default_val=default_val)
    
    def get_global_acc(self, item, default_val=None):
        return self.acc_counter.get('global', item, default_val=default_val)
        
    def sync(self):
        if self.remote:
            client_call(self.server, 'inc_merge', self.inc_counter.container)
            client_call(self.server, 'acc_merge', self.acc_counter.container)
        else:
            self.server.inc_merge(self.inc_counter.container)
            self.server.acc_merge(self.acc_counter.container)
        self.inc_counter.reset()
        self.acc_counter.reset()
    