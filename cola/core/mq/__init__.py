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

Created on 2013-5-23

@author: Chine
'''

import os
import struct
import marshal
try:
    import cPickle as pickle
except ImportError:
    import pickle
from itertools import groupby
from operator import itemgetter

from cola.core.utils import get_ip, iterable
from cola.core.mq.hash_ring import HashRing
from cola.core.mq.store import Store
from cola.core.rpc import client_call

MQ_STATUS_FILENAME = 'mq.status'

PRIORITY_STORE_FN = 'store'
BACKUP_STORE_FN = 'backup'
INCR_STORE_FN = 'inc'

NEW_LINE_REPLACER = '@%#$'

CACHE_SIZE = 20

MARSHAL, PICKLE, STRING, UNICODE = 'm', 'p', 's', 'u'

class MessageQueue(object):
    def __init__(self, nodes, current_node=None, base_dir=None, rpc_server=None, 
                 copies=1, n_priorities=3, verify_exists_hook=None):
        self.dir_ = base_dir
        self.nodes = nodes
        self.current_node = current_node
        self.is_client = self.current_node is None and self.dir_ is None
        self.other_nodes = [node for node in self.nodes if node != self.current_node]
        self.rpc_server = rpc_server
        self.hash_ring = HashRing(self.nodes)
        
        self.copies = max(min(len(self.nodes)-1, copies), 0)
        self.n_priorities = n_priorities
        
        self.verify_exists_hook = verify_exists_hook
        
        self._register_rpc()
        self.inited = False
            
    def _register_rpc(self):
        if self.rpc_server:
            self.rpc_server.register_function(self.put_backup_proxy, 'put_backup')
            self.rpc_server.register_function(self.put_proxy, 'put')
            self.rpc_server.register_function(self.get_proxy, 'get')
            
    def init(self):
        if self.inited: return
        
        self.load()
        if not hasattr(self, 'caches'):
            self.caches = dict((node, []) for node in self.nodes)
        if not hasattr(self, 'caches_inited'):
            self.caches_inited = dict((node, False) for node in self.nodes)
        if not hasattr(self, 'backup_caches'):
            self.backup_caches = dict((node, []) for node in self.nodes)
        
        if not self.is_client:
            get_priority_store_dir = lambda priority: os.path.join(self.dir_, 
                                        PRIORITY_STORE_FN, str(priority))
            self.priority_stores = [Store(get_priority_store_dir(i), 
                                          verify_exists_hook=self.verify_exists_hook,
                                          mkdirs=True) \
                                    for i in range(self.n_priorities)]
            
    
            backup_store_dir = os.path.join(self.dir_, BACKUP_STORE_FN)
            self.backup_stores = {}
            for backup_node in self.other_nodes:
                backup_node_dir = backup_node.replace(':', '_')
                backup_path = os.path.join(backup_store_dir, backup_node_dir)
                self.backup_stores[backup_node] = Store(backup_path, 
                                                       size=512*1024, mkdirs=True)
                
            inc_store_dir = os.path.join(self.dir_, INCR_STORE_FN)
            self.inc_store = Store(inc_store_dir, mkdirs=True)
                
        self.inited = True
        
    def save(self):
        if self.is_client or not self.inited:
            return
        
        save_file = os.path.join(self.dir_, MQ_STATUS_FILENAME)
        with open(save_file, 'w') as f:
            t = (self.caches, self.caches_inited, self.backup_caches)
            f.write(pickle.dumps(t))
    
    def load(self):
        if self.is_client:
            return
        
        save_file = os.path.join(self.dir_, MQ_STATUS_FILENAME)
        if not os.path.exists(save_file):
            return
        
        with open(save_file, 'r') as f:
            self.caches, self.caches_inited, self.backup_caches = pickle.loads(f.read())
        
    def _stringfy(self, obj):
        if isinstance(obj, unicode):
            str_ = UNICODE + obj.encode('utf-8')
        elif isinstance(obj, str):
            str_ = STRING + obj
        else:
            try:
                str_ = MARSHAL + marshal.dumps(obj)
            except ValueError:
                str_ = PICKLE + pickle.dumps(obj)
        return str_.replace('\n', NEW_LINE_REPLACER)
    
    def _destringfy(self, src_str):
        if len(src_str) < 2:
            raise ValueError('String length must be at least 2.')
        
        str_ = src_str[1:].replace(NEW_LINE_REPLACER, '\n')
        t = src_str[0]
        if t == UNICODE:
            obj = str_.decode('utf-8')
        elif t == STRING:
            obj = str_
        elif t == MARSHAL:
            obj = marshal.loads(str_)
        elif t == PICKLE:
            obj = pickle.loads(str_)
        else:
            raise ValueError('String must contain a right type indicator.')
        return obj
        
    def _put(self, node, objs, force=False, priority=0):
        if node == self.current_node:
            if priority < 0: priority = 0
            if priority > self.n_priorities: priority = self.n_priorities-1
            self.priority_stores[priority].put(objs, force=force)
        else:
            client_call(node, 'put', objs, force, priority)
                
    def _put_backup(self, node, bknode_objs, force=False):
        if node == self.current_node:
            for backup_node, obj in bknode_objs:
                self.backup_stores[backup_node].put(obj, force=force)
        else:
            client_call(node, 'put_backup', bknode_objs, force)
                
    def _get(self, node, priority=0):
        if node == self.current_node:
            if priority < 0: priority = 0
            if priority > self.n_priorities: priority = self.n_priorities-1
            str_ = self.priority_stores[priority].get()
        else:
            str_ = client_call(node, 'get', priority)
        if str_ is not None:
            return self._destringfy(str_)
            
    def put_backup_proxy(self, bknode_objs, force=False):
        self.init()
        for backup_node, obj in bknode_objs:
            backup_store = self.backup_stores[backup_node]
            backup_store.put(obj, force=force)
        
    def put_proxy(self, objs, force=False, priority=0):
        self.init()
        if priority < 0: priority = 0
        if priority > self.n_priorities: priority = self.n_priorities-1
        priority_store = self.priority_stores[priority]
        priority_store.put(objs, force=force)
        
    def get_proxy(self, priority=0):
        self.init()
        if priority < 0: priority = 0
        if priority > self.n_priorities: priority = self.n_priorities-1
        priority_store = self.priority_stores[priority]
        return priority_store.get()
        
    def put_inc(self, obj, force=True):
        self.init()
        inc_store = self.inc_store
        inc_store.put(self._stringfy(obj), force=force)
        
    def _put_priorities_objs(self, node, obj_priorities, force=False):
        sorted_v = sorted(obj_priorities, key=itemgetter(1))
        for priority, v in groupby(sorted_v, key=itemgetter(1)):
            obs = [obj for obj, _ in v]
            self._put(node, obs, force=force, priority=priority)
    
    def put(self, objects, force=False, flush=False):
        self.init()
        
        if isinstance(objects, basestring) or not iterable(objects):
            objs = [objects, ]
        else:
            objs = objects        
        
        for obj in objs:
            priority = 0
            if not isinstance(obj, basestring):
                priority = getattr(obj, 'priority', 0)
            
            str_ = self._stringfy(obj)
            
            it = self.hash_ring.iterate_nodes(str_)
            
            # put obj into an mq node.
            put_node = next(it)
            self.caches[put_node].append((str_, priority))
            
            for _ in xrange(self.copies):
                backup_node = next(it)
                if backup_node is None: continue
                
                self.backup_caches[backup_node].append((put_node, str_))
        
        for k, v in self.caches.iteritems():
            if not self.caches_inited[k] or self.is_client or \
                len(v) > CACHE_SIZE or flush:
                self._put_priorities_objs(k, v, force=force)
                self.caches[k] = []
            
            if not self.caches_inited[k] or self.is_client:
                self.caches_inited[k] = True
                
        for k, v in self.backup_caches.iteritems():
            if len(v) >= CACHE_SIZE or self.is_client or flush:
                self._put_backup(k, v, force=force)
                self.backup_caches[k] = []
                
    def flush(self):
        self.put([], flush=True)
            
    def get(self, priority=0):
        self.init()
        
        if self.current_node is not None:
            nodes = sorted(self.nodes, key=lambda k: k==self.current_node, reverse=True)
        else:
            nodes = self.nodes
        for n in nodes:
            obj = self._get(n, priority=priority)
            if obj is not None:
                return obj
            
    def remove_node(self, node):
        self._put_priorities_objs(self.current_node, self.caches[node])
        del self.caches[node]
        del self.caches_inited[node]
        del self.backup_caches[node]
        
        self.flush()
        
        self.nodes.remove(node)
        self.hash_ring = HashRing(self.nodes)
        
        backup_store = self.backup_stores[node]
        str_ = backup_store.get()
        objs = []
        while str_ is not None:
            obj = self._destringfy(str_)
            objs.append((str_, getattr(obj, 'priority', 0)))
            if len(obj) >= CACHE_SIZE:
                self._put_priorities_objs(self.current_node, objs)
            str_ = backup_store.get()
        self._put_priorities_objs(self.current_node, objs)
            
        backup_store.shutdown()
        del self.backup_stores[node]
        
    def add_node(self, node, backup_store=None):
        self.caches[node] = []
        self.caches_inited[node] = False
        self.backup_caches[node] = []
        
        self.nodes.append(node)
        self.hash_ring = HashRing(self.nodes)
        if backup_store is not None:
            self.backup_stores[node] = backup_store
        else:
            backup_stores_dir = os.path.join(self.dir_, BACKUP_STORE_FN)
            path = os.path.join(backup_stores_dir, node.replace(':', '_'))
            self.backup_stores[node] = Store(path, 
                                            size=512*1024, mkdirs=True)
            
    def shutdown(self):
        if not self.inited: return
        
        [store.shutdown() for store in self.priority_stores]
        for backup_store in self.backup_stores.values():
            backup_store.shutdown()
        self.inc_store.shutdown()
        self.save()
            
    def __enter__(self):
        return self
    
    def __exit__(self):
        self.shutdown()