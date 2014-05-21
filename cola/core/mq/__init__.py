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

PUT, PUT_INC, GET, GET_INC, EXIST = range(5)

MessageQueueClient = MessageQueueClient

class MessageQueue(MessageQueueNodeProxy):
    def __init__(self, working_dir, rpc_server, addr, addrs, 
                 app_name=None, copies=1, n_priorities=3,
                 deduper=None):
        super(MessageQueue, self).__init__(working_dir, rpc_server, addr, addrs,
                                           copies=copies, n_priorities=n_priorities,
                                           deduper=deduper, app_name=app_name)

class MpMessageQueue(MessageQueueNodeProxy):
    def __init__(self, working_dir, rpc_server, addr, addrs, 
                 instances=1, app_name=None, copies=1, 
                 n_priorities=3, deduper=None):
        super(MpMessageQueue, self).__init__(
                working_dir, rpc_server, addr, addrs,
                copies=copies, n_priorities=n_priorities,
                deduper=deduper, app_name=app_name)
        self.manager = multiprocessing.Manager()
        self.stopped = self.manager.Event()
        
        self.kw = {}
        
        self.kw['stopped'] = self.stopped
        self.kw['clients'] = []
        
        self.threads = []
        for _ in range(instances):
            agent, client = multiprocessing.Pipe()
            self.kw['clients'].append(client)
            t = threading.Thread(target=self._init_agent, args=(agent, ))
            t.setDaemon(True)
            self.threads.append(t)
            t.start()
         
    def _init_agent(self, agent):
        while not self.stopped.is_set():
            need_process = agent.poll(10)
            if self.stopped.is_set():
                return
            if not need_process:
                continue
            
            action, data = agent.recv()
            if action == PUT:
                objs, flush = data
                self.put(objs, flush=flush)
                agent.send(1)
            elif action == PUT_INC:
                self.put_inc(data)
                agent.send(1)
            elif action == GET:
                size, priority = data
                agent.send(self.get(size=size, 
                                    priority=priority))
            elif action == GET_INC:
                agent.send(self.get_inc(data))
            elif action == EXIST:
                if not self.mq_node.deduper:
                    agent.send(False)
                else:
                    agent.send(self.exist(str(data)))
            else:
                raise ValueError('mq client can only put, put_inc, and get')
                
    def shutdown(self):
        self.stopped.set()
        
    def join(self):
        for t in self.threads:
            t.join()
        
class MpMessageQueueClient(object):
    def __init__(self, instance_id, kw):
        for k, v in kw.iteritems():
            setattr(self, k, v)
        self.client = self.clients[instance_id]
        
    def put(self, objs, flush=False, inc=False):
        if self.stopped.is_set():
            return
        if not inc:
            self.client.send((PUT, (objs, flush)))
        else:
            self.client.send((PUT_INC, objs))
        while not self.stopped.is_set():
            need_process = self.client.poll(10)
            if self.stopped.is_set():
                return
            if not need_process:
                continue
            
            self.client.recv()
            return
        
    def get(self, size=1, priority=0, inc=False):
        if self.stopped.is_set():
            return
        if not inc:
            self.client.send((GET, (size, priority)))
        else:
            self.client.send((GET_INC, size))
        while not self.stopped.is_set():
            need_process = self.client.poll(10)
            if self.stopped.is_set():
                return
            if not need_process:
                continue
            
            return self.client.recv()
            
    def exist(self, obj):
        if self.stopped.is_set():
            return False
        self.client.send((EXIST, str(obj)))
        while not self.stopped.is_set():
            need_process = self.client.poll(10)
            if self.stopped.is_set():
                return False
            if not need_process:
                continue
                
            return self.client.recv()
        
        return False