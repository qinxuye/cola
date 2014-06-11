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

import multiprocessing
import threading

from cola.core.mq.node import MessageQueueNodeProxy
from cola.core.mq.client import MessageQueueClient
from cola.core.utils import get_rpc_prefix, \
                            pickle_connection, unpickle_connection

PUT, BATCH_PUT, PUT_BACKUP, GET, EXIST = range(5)

MessageQueueClient = MessageQueueClient

class MessageQueue(MessageQueueNodeProxy):
    def __init__(self, working_dir, rpc_server, addr, addrs, 
                 app_name=None, copies=1, n_priorities=3,
                 deduper=None):
        super(MessageQueue, self).__init__(working_dir, rpc_server, addr, addrs,
                                           copies=copies, n_priorities=n_priorities,
                                           deduper=deduper, app_name=app_name)
        
        self.agent, self.client = multiprocessing.Pipe()
        self._t = threading.Thread(target=self._init_agent, args=(self.agent, ))
        self._t.start()
        
    def get_connection(self):
        return pickle_connection(self.client)
        
    def _init_agent(self, agent):
        while True:
            try:
                need_process = agent.poll(10)
                if not need_process:
                    continue
            
                action, data = agent.recv()
                if action == PUT:
                    objs, force, priority = data
                    self.mq_node.put_proxy(objs, force=force, 
                                           priority=priority)
                    agent.send(1)
                elif action == BATCH_PUT:
                    objs, = data
                    self.mq_node.batch_put_proxy(objs)
                    agent.send(1)
                elif action == PUT_BACKUP:
                    addr, objs, force = data
                    self.mq_node.put_backup_proxy(addr, objs, force=force)
                elif action == GET:
                    size, priority = data
                    agent.send(self.mq_node.get_proxy(size=size, 
                                                      priority=priority))
                elif action == EXIST:
                    obj, = data
                    agent.send(self.mq_node.exist(str(obj)))
                else:
                    raise ValueError('mq client can only put, put_inc, and get')
            except IOError:
                return
            
    def shutdown(self):
        super(MessageQueue, self).shutdown()
        self.agent.close()
        self._t.join()
        
class MessageQueueRPCProxy(object):
    def __init__(self, connection, rpc_server=None, app_name=None):
        self.client = unpickle_connection(connection)
        self.rpc_server = rpc_server
        self.app_name = app_name
        self._register_rpc()
        
    def _register_rpc(self):
        if self.rpc_server:
            self.register_rpc(self, self.rpc_server, app_name=self.app_name)
                
    @classmethod
    def register_rpc(cls, node, rpc_server, app_name=None):
        prefix = get_rpc_prefix(app_name, 'mq')
        rpc_server.register_function(node.put, name='put', 
                                     prefix=prefix)
        rpc_server.register_function(node.batch_put, name='batch_put', 
                                     prefix=prefix)
        rpc_server.register_function(node.put_backup, name='put_backup',
                                     prefix=prefix)
        rpc_server.register_function(node.get, name='get',
                                     prefix=prefix)
        rpc_server.register_function(node.exist, name='exist',
                                     prefix=prefix)
        
    def _call(self, func_name, *args):
        self.client.send((func_name, args))
        while True:
            try:
                need_process = self.client.poll(10)
                if not need_process:
                    continue
                
                return self.client.recv()
            except IOError:
                return
            
    def put(self, objs, force=False, priority=0):
        self._call(PUT, objs, force, priority)
        
    def batch_put(self, objs):
        self._call(BATCH_PUT, objs)
        
    def put_backup(self, addr, objs, force=False):
        self._call(PUT_BACKUP, addr, objs, force)
        
    def get(self, size=1, priority=0):
        result = self._call(GET, size, priority)
        if size > 1 and result is None:
            return []
        return result
    
    def exist(self, obj):
        return bool(self._call(EXIST, obj))