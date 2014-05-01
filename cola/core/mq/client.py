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

Created on 2013-5-28

@author: Chine
'''

from random import shuffle
try:
    import cPickle as pickle
except ImportError:
    import pickle

from cola.core.utils import get_rpc_prefix
from cola.core.rpc import client_call
from cola.core.mq.distributor import Distributor

class MessageQueueClient(object):
    
    def __init__(self, addrs, app_name=None, copies=1):
        self.addrs = addrs
        self.distributors = Distributor(addrs, copies=copies)
        self.prefix = get_rpc_prefix(app_name, 'mq')
        
    def put(self, objs):
        addrs_objs, addrs_backup_objs = \
            self.distributors.distribute(objs)
        
        for addr, objs in addrs_objs.iteritems():
            client_call(addr, self.prefix+'batch_put', pickle.dumps(objs))
        for addr, m in addrs_backup_objs.iteritems():
            for b_addr, objs in m.iteritems():
                client_call(addr, self.prefix+'put_backup', b_addr, 
                            pickle.dumps(objs))
        
    def get(self, size=1, priority=0):
        size = max(size, 1)
        
        addrs = list(self.addrs)
        shuffle(addrs)
        
        results = []
        for addr in addrs:
            left = size - len(results)
            if left <= 0:
                break
            
            objs = pickle.loads(client_call(addr, self.prefix+'get', 
                                            left, priority))
            if objs is None:
                continue
            if not isinstance(objs, list):
                objs = [objs, ]
            results.extend(objs)
        
        if size == 1:
            if len(results) == 0:
                return
            return results[0]
        return results