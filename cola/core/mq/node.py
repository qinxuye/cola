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

Created on 2014-4-27

@author: chine
'''

import os
try:
    import cPickle as pickle
except ImportError:
    import pickle
from collections import defaultdict

from cola.core.rpc import client_call
from cola.core.mq.store import Store
from cola.core.mq.distributor import Distributor
    
MQ_STATUS_FILENAME = 'mq.status'

PRIORITY_STORE_FN = 'store'
BACKUP_STORE_FN = 'backup'
INCR_STORE_FN = 'inc'

CACHE_SIZE = 20

class LocalMessageQueueNode(object):
    def __init__(self, base_dir, rpc_server, addr, addrs,
                 copies=1, n_priorities=3, deduper=None):
        self.dir_ = base_dir
        self.rpc_server = rpc_server
        
        assert addr in addrs
        self.addr = addr
        self.addrs = addrs
        self.other_addrs = [n for n in self.addrs if n != self.addr]
        
        self.copies = max(min(len(self.nodes)-1, copies), 0)
        self.n_priorities = max(n_priorities, 1)
        self.deduper = deduper
        
        self.inited = False
        
    def init(self):
        if self.inited: return
        
        get_priority_store_dir = lambda priority: os.path.join(self.dir_, 
                                    PRIORITY_STORE_FN, str(priority))
        self.priority_stores = [Store(get_priority_store_dir(i), 
                                      deduper=self.deduper,
                                      mkdirs=True) \
                                for i in range(self.n_priorities)]
        

        backup_store_dir = os.path.join(self.dir_, BACKUP_STORE_FN)
        self.backup_stores = {}
        for backup_addr in self.other_addrs:
            backup_node_dir = backup_addr.replace(':', '_')
            backup_path = os.path.join(backup_store_dir, backup_node_dir)
            self.backup_stores[backup_addr] = Store(backup_path, 
                                                   size=512*1024, mkdirs=True)
            
        inc_store_dir = os.path.join(self.dir_, INCR_STORE_FN)
        self.inc_store = Store(inc_store_dir, mkdirs=True)
                
        self.inited = True
        
    def _register_rpc(self):
        if self.rpc_server:
            self.rpc_server.register_function(self.put, name='put', 
                                              prefix='mq')
            self.rpc_server.register_function(self.batch_put, name='batch_put', 
                                              prefix='mq')
            self.rpc_server.register_function(self.put_backup, name='put_backup',
                                              prefix='mq')
            self.rpc_server.register_function(self.get_proxy, name='get',
                                              prefix='mq')
        
    def put(self, objs, force=False, priority=0):
        self.init()
        
        priority = max(min(priority, self.n_priorities-1), 0)
        priority_store = self.priority_stores[priority]
        priority_store.put(objs, force=force)
        
    def batch_put(self, objs):
        self.init()
        
        puts = defaultdict(lambda:defaultdict(list))
        for obj in objs:
            priority = getattr(obj, 'priority', 0)
            force = getattr(obj, 'force', False)
            puts[priority][force].append(obj)
        
        for priority, m in puts.iteritems():
            for force, obs in m.iteritems():
                self.put(obs, force=force, priority=priority)
    
    def put_backup(self, node, objs, force=False):
        self.init()
        
        backup_store = self.backup_stores[node]
        backup_store.put(objs, force=force)
        
    def put_inc(self, objs, force=True):
        self.init()
        
        self.inc_store.put(objs, force=force)
        
    def get(self, size=1, priority=0):
        self.init()
        
        priority = max(min(priority, self.n_priorities-1), 0)
        priority_store = self.priority_stores[priority]
        return priority_store.get(size=size)
    
    def get_backup(self, node, size=1):
        self.init()
        
        backup_store = self.backup_stores[node]
        return backup_store.get(size=size)
    
    def get_inc(self, size=1):
        self.init()
        
        return self.inc_store.get(size=size)
    
    def shutdown(self):
        if not self.inited: return
        
        [store.shutdown() for store in self.priority_stores]
        for backup_store in self.backup_stores.values():
            backup_store.shutdown()
        self.inc_store.shutdown()
    
class MessageQueueNodeProxy(object):
    def __init__(self, base_dir, rpc_server, addr, addrs,
                 copies=1, n_priorities=3, deduper=None):
        self.dir_ = base_dir
        self.addr_ = addr
        self.mq_node = LocalMessageQueueNode(
            base_dir, rpc_server, addr, addrs, 
            copies=copies, n_priorities=n_priorities, deduper=deduper)
        self.distributor = Distributor(addrs, copies=copies)
        
        self.inited = False
        
    def init(self):
        if self.inited: return
        
        self.load()
        if not hasattr(self, 'caches'):
            self.caches = dict((node, []) for node in self.nodes)
        if not hasattr(self, 'caches_inited'):
            self.caches_inited = dict((node, False) for node in self.nodes)
        if not hasattr(self, 'backup_caches'):
            self.backup_caches = dict((node, {}) for node in self.nodes)
            for node in self.nodes:
                for other_node in [n for n in self.nodes if node != n]:
                    self.backup_caches[node][other_node] = []
            
        self.inited = True
        
    def load(self):
        save_file = os.path.join(self.dir_, MQ_STATUS_FILENAME)
        if not os.path.exists(save_file):
            return
        
        with open(save_file, 'r') as f:
            self.caches, self.caches_inited, self.backup_caches = pickle.loads(f.read())
    
    def save(self):
        if not self.inited:
            return
        
        save_file = os.path.join(self.dir_, MQ_STATUS_FILENAME)
        with open(save_file, 'w') as f:
            t = (self.caches, self.caches_inited, self.backup_caches)
            f.write(pickle.dumps(t))
            
    def _remote_or_local_put(self, addr, objs, force=False, priority=0):
        if addr == self.addr_:
            self.mq_node.put(objs, force=force, priority=priority)
        else:
            client_call(addr, 'mq_put', objs, force=force, priority=priority)
            
    def _remote_or_local_batch_put(self, addr, objs):
        if addr == self.addr_:
            self.mq_node.batch_put(objs)
        else:
            client_call(addr, 'mq_batch_put', objs)
            
    def _remote_or_local_get(self, addr, size=1, priority=0):
        if addr == self.addr_:
            self.mq_node.get(size=size, priority=priority)
        else:
            client_call(addr, 'mq_get', size=size, priority=priority)
            
    def _remote_or_local_put_backup(self, addr, backup_addr, objs, 
                                    force=False):
        if addr == self.addr_:
            self.mq_node.put_backup(backup_addr, objs, force=force)
        else:
            client_call(addr, 'mq_put_backup', backup_addr, objs, 
                        force=force)
                    
    def put(self, objects, flush=False):
        nodes_objs, backup_nodes_objs = \
            self.distributor.distribute(objects)
            
        for node, objs in nodes_objs.iteritems():
            self.caches[node].extend(objs)
            if not self.caches_inited[node] or \
                len(self.cache[node]) >= CACHE_SIZE or flush:
                self._remote_or_local_batch_put(node, self.caches[node])
                
                self.caches[node] = []
                self.caches_inited[node] = True
        
        for node, m in backup_nodes_objs.iteritems():
            for backup_node, objs in m.iteritems():
                self.backup_caches[node][backup_node].extend(objs)
            
            size = sum([len(obs) for obs in \
                            self.backup_caches[node].values()])
            if size >= CACHE_SIZE or flush:
                for backup_node, objs in m.iteritems():
                    self._remote_or_local_put_backup(
                        node, backup_node, objs)
            
    def get(self, priority=0):
        pass
    
    def add_node(self, addr):
        pass
    
    def remove_node(self, addr):
        pass
    
    def shutdown(self):
        if not self.inited: return
        
        self.mq_node.shutdown()
        self.save()
        
    def __enter__(self):
        return self
    
    def __exit__(self, type_, value, traceback):
        self.shutdown()