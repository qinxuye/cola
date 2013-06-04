#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2013-5-28

@author: Chine
'''

from cola.core.mq.hash_ring import HashRing
from cola.core.mq import MessageQueue

class MessageQueueClient(object):
    
    def __init__(self, nodes, copies=1):
        self.nodes = nodes
        self.hash_ring = HashRing(self.nodes)
        self.copies = max(min(len(self.nodes)-1, copies), 0)
        self.mq = MessageQueue(nodes, copies=copies)
        
    def put(self, objs):
        self.mq.put(objs)
        
    def get(self):
        for n in self.nodes:
            obj = self.mq._get(n)
            if obj is not None:
                return obj