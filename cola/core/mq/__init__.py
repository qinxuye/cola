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

from cola.core.utils import get_ip
from cola.core.mq.hash_ring import HashRing
from cola.core.mq.node import Node
from cola.core.rpc import client_call

class MessageQueue(object):
    def __init__(self, nodes, local_node=None, rpc_server=None, 
                 local_store=None, backup_stores=None, copies=1):
        self.nodes = nodes
        self.local_node = local_node
        self.local_store = local_store
        self.rpc_server = rpc_server
        self.backup_stores = backup_stores
        self.hash_ring = HashRing(self.nodes)
        self.copies = max(min(len(self.nodes)-1, copies), 0)
        
        if rpc_server is not None and \
            self.local_store is not None and \
            self.backup_stores is not None:
            self._register_rpc()
            
    def _register_rpc(self):
        self.rpc_server.register_function(self.put_backup, 'put_backup')
        self.rpc_server.register_instance(self.local_store)
            
    def init_store(self, local_store_path, backup_stores_path, 
                   verify_exists_hook=None):
        self.local_store = Node(local_store_path, 
                                verify_exists_hook=verify_exists_hook)
        self.backup_stores_path = backup_stores_path
        
        backup_nodes = self.nodes[:]
        backup_nodes.remove(self.local_node)
        self.backup_stores = {}
        for backup_node in backup_nodes:
            backup_node_dir = backup_node.replace(':', '_')
            backup_path = os.path.join(backup_stores_path, backup_node_dir)
            if not os.path.exists(backup_path):
                os.makedirs(backup_path)
            self.backup_stores[backup_node] = Node(backup_path, 
                                                   size=512*1024)
            
        self._register_rpc()
        
    def _put(self, node, objs, force=False):
        if node == self.local_node:
            self.local_store.put(objs, force=force)
        else:
            client_call(node, 'put', objs, force)
                
    def _put_backup(self, node, src, objs, force=False):
        if node == self.local_node:
            self.put_backup(src, objs, force=force)
        else:
            client_call(node, 'put_backup', src, objs, force)
                
    def _get(self, node):
        if node == self.local_node:
            return self.local_store.get()
        else:
            return client_call(node, 'get')
        
    def put(self, obj_or_objs, force=False):
        def _check(obj):
            if not isinstance(obj, basestring):
                raise ValueError("MessageQueue can only put basestring objects.")
        if isinstance(obj_or_objs, (tuple, list)):
            for obj in obj_or_objs:
                _check(obj)
            objs = obj_or_objs
        else:
            _check(obj_or_objs)
            objs = [obj_or_objs]
        
        puts = {}
        bkup_puts = {}
        for obj in objs:
            if isinstance(obj, unicode):
                obj = obj.encode('utf-8')
            
            it = self.hash_ring.iterate_nodes(obj)
            
            # put obj into an mq node.
            put_node = next(it)
            obs = puts.get(put_node, [])
            obs.append(obj)
            puts[put_node] = obs
            
            for _ in xrange(self.copies):
                bkup_node = next(it)
                if bkup_node is None: continue
                
                kv = bkup_puts.get(bkup_node, {})
                obs = kv.get(put_node, [])
                obs.append(obj)
                kv[put_node] = obs
                bkup_puts[bkup_node] = kv
        
        for k, v in puts.iteritems():
            self._put(k, v, force=force)
        for k, v in bkup_puts.iteritems():
            for src_node, obs in v.iteritems():
                self._put_backup(k, src_node, obs, force=force)
            
    def put_backup(self, src, obj_or_objs, force=False):
        backup_store = self.backup_stores[src]
        backup_store.put(obj_or_objs, force=force)
            
    def get(self):
        if self.local_node is not None:
            nodes = sorted(self.nodes, key=lambda k: k==self.local_node, reverse=True)
        else:
            nodes = self.nodes
        for n in nodes:
            obj = self._get(n)
            if obj is not None:
                return obj
            
    def remove_node(self, node):
        self.nodes.remove(node)
        self.hash_ring = HashRing(self.nodes)
        
        backup_store = self.backup_stores[node]
        obj = backup_store.get()
        while obj is not None:
            self.local_store.put(obj)
            obj = backup_store.get()
            
        backup_store.shutdown()
        del self.backup_stores[node]
        
    def add_node(self, node, backup_store=None):
        self.nodes.append(node)
        self.hash_ring = HashRing(self.nodes)
        if backup_store is not None:
            self.backup_stores[node] = backup_store
        else:
            backup_stores_path = getattr(self, 'backup_stores_path')
            if backup_stores_path is not None:
                path = os.path.join(backup_stores_path, node.replace(':', '_'))
                if not os.path.exists(path):
                    os.makedirs(path)
                self.backup_stores[node] = Node(path, 
                                                size=512*1024)
            
    def shutdown(self):
        self.local_store.shutdown()
        for backup_store in self.backup_stores.values():
            backup_store.shutdown()
            
    def __enter__(self):
        return self
    
    def __exit__(self):
        self.shutdown()