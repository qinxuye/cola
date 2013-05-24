#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2013-5-23

@author: Chine
'''

from cola.core.config import main_conf
from cola.core.utils import get_ip
from cola.core.mq.hash_ring import HashRing

class MessageQueue(object):
    def __init__(self, nodes, local_node, dir_):
        self.nodes = nodes
        self.local_node = local_node
        self.hash_ring = HashRing(self.nodes)
        self.dir_ = dir_
        
    def put(self, obj):
        if not isinstance(object, str):
            raise ValueError("MessageQueue can only put string objects.")
        put_node = self.hash_ring.get_node(obj)