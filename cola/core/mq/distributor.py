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

Created on 2014-4-30

@author: chine
'''

from collections import defaultdict

from cola.core.utils import iterable
from cola.core.mq.hash_ring import HashRing
from cola.core.mq.utils import labelize

class Distributor(object):
    def __init__(self, addrs, copies=1):
        self.nodes = list(addrs)
        self.hash_ring = HashRing(self.nodes)
        self.copies = copies
        
    def distribute(self, objs):
        node_objs = defaultdict(list)
        backup_node_objs = defaultdict(lambda: defaultdict(list))
        
        if isinstance(objs, basestring) or not iterable(objs):
            objs = [objs, ]
        
        for obj in objs:
            str_ = labelize(obj)
            
            it = self.hash_ring.iterate_nodes(str_)
            
            # put obj into an mq node.
            put_node = next(it)
            node_objs[put_node].append(obj)
            
            for _ in xrange(self.copies):
                backup_node = next(it)
                if backup_node is None: continue
                
                backup_node_objs[backup_node][put_node].append(obj)
        
        return node_objs, backup_node_objs
    
    def remove_node(self, addr):
        if addr in self.nodes:
            self.nodes.remove(addr)
            self.hash_ring = HashRing(self.nodes)
            
    def add_node(self, addr):
        if addr not in self.nodes:
            self.nodes.append(addr)
            self.hash_ring = HashRing(self.nodes)