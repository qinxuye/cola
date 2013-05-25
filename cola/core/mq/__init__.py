#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2013-5-23

@author: Chine
'''

import xmlrpclib

from cola.core.utils import get_ip
from cola.core.mq.hash_ring import HashRing

class MessageQueue(object):
    def __init__(self, nodes, local_node, rpc_server, 
                 local_store, backup_store, copies=1):
        self.nodes = nodes
        self.local_node = local_node
        self.local_store = local_store
        self.backup_store = backup_store
        self.hash_ring = HashRing(self.nodes)
        self.copies = max(min(len(self.nodes)-1, copies), 0)
        
        rpc_server.register_function(self.backup_store.put, 'put_backup')
        rpc_server.register_instance(self.local_store)
        
    def _put(self, node, objs, bkup=False):
        if node == self.local_node:
            if not bkup:
                self.local_store.put(objs)
            else:
                self.backup_store.put(objs)
        else:
            serv = xmlrpclib.ServerProxy('http://%s' % node)
            if not bkup:
                serv.put(objs)
            else:
                serv.put_backup(objs)
                
    def _get(self, node):
        if node == self.local_node:
            return self.local_store.get()
        else:
            serv = xmlrpclib.ServerProxy('http://%s' % node)
            return serv.get()
        
    def put(self, obj_or_objs):
        def _check(obj):
            if not isinstance(obj, str):
                raise ValueError("MessageQueue can only put string objects.")
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
            it = self.hash_ring.iterate_nodes(obj)
            
            # put obj into an mq node.
            put_node = next(it)
            obs = puts.get(put_node, [])
            obs.append(obj)
            puts[put_node] = obs
            
            for _ in xrange(self.copies):
                bkup_node = next(it)
                if bkup_node is None: continue
                
                obs = bkup_puts.get(bkup_node, [])
                obs.append(obj)
                bkup_puts[bkup_node] = obs
        
        for k, v in puts.iteritems():
            self._put(k, v)
        for k, v in bkup_puts.iteritems():
            self._put(k, v, bkup=True)
            
    def get(self):
        nodes = sorted(self.nodes, key=lambda k: k==self.local_node, reverse=True)
        for n in nodes:
            obj = self._get(n)
            if obj is not None:
                return obj
            
    def shutdown(self):
        self.local_store.shutdown()
        self.backup_store.shutdown()