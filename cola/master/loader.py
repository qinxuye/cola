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

Created on 2013-5-27

@author: Chine
'''

import time
import threading
import signal
import socket
import os
import sys

from cola.core.rpc import ColaRPCServer, client_call
from cola.core.mq.client import MessageQueueClient
from cola.core.utils import get_ip, root_dir, import_job

class JobMasterRunning(Exception): pass

TIME_SLEEP = 1

class JobLoader(object):
    def __init__(self, job, nodes, rpc_server, 
                 context=None, copies=2):
        self.job = job
        self.ctx = context or job.context
        
        self.nodes = nodes
        self.mq_client = MessageQueueClient(self.nodes, copies=copies)
        
        self.not_registered = self.nodes[:]
        self.is_ready = False
        self.stopped = False
        
        # destination size
        self.size = self.ctx.job.size
        self.limit_size = self.size > 0
        self.finishes = 0
        
        # speed limits
        self.limits = self.ctx.job.limits
        self.limit_speed = self.limits > 0
        self.in_minute = 0
        
        # register rpc server
        rpc_server.register_function(self.ready, 'ready')
        rpc_server.register_function(self.get_nodes, 'get_nodes')
        rpc_server.register_function(self.require, 'require')
        rpc_server.register_function(self.stop, 'stop')
        rpc_server.register_function(self.add_node, 'add_node')
        rpc_server.register_function(self.remove_node, 'remove_node')
        
        # register signal
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
    def ready(self, node):
        if node in self.not_registered:
            self.not_registered.remove(node)
            if len(self.not_registered) == 0:
                self.is_ready = True
    
    def get_nodes(self):
        return self.nodes
    
    def require(self, count):
        if self.limit_speed:
            if self.in_minute < self.limit_size:
                res = max(count, self.limit_size - self.in_minute)
                self.in_minute += res
                return res
            else:
                return 0
        return count if not self.stopped else 0
    
    def complete(self, obj):
        if self.limit_size:
            self.finishes += 1
            return self.finishes >= self.size
        return False if not self.stopped else True
    
    def _in_minute_clear(self):
        def _clear():
            self.in_minute = 0
            time.sleep(60)
            if not self.stopped:
                _clear()
        thd = threading.Thread(target=_clear)
        thd.setDaemon(True)
        thd.start()
        
    def signal_handler(self, signum, frame):
        self.stop()
        
    def stop(self):
        for node in self.nodes:
            try:
                client_call(node, 'stop')
            except socket.error:
                pass
        self.stopped = True
        
    def run(self):
        # wait until all the workers initialized
        while not self.is_ready: pass
        
        if self.limit_speed:
            self._in_minute_clear()
            
        self.mq_client.put(self.job.starts)
        for node in self.nodes:
            client_call(node, 'run')
        
        def _run():
            while not self.stopped:
                time.sleep(TIME_SLEEP)
        main_thread = threading.Thread(target=_run)
        main_thread.start()
        main_thread.join()
        
    def add_node(self, node):
        for node in self.nodes:
            client_call(node, 'add_node', node)
        self.nodes.append(node)
        client_call(node, 'run')
        
    def remove_node(self, node):
        for node in self.nodes:
            client_call(node, 'remove_node', node)
        self.nodes.remove(node)
        
def create_rpc_server(job, context=None):
    ctx = context or job.context
    rpc_server = ColaRPCServer((get_ip(), ctx.job.master_port))
    thd = threading.Thread(target=rpc_server.serve_forever)
    thd.setDaemon(True)
    thd.start()
    return rpc_server

def load_job(path, nodes, context=None):
    if not os.path.exists(path):
        raise ValueError('Job definition does not exist.')
        
    job = import_job(path)
    
    job_name = job.name.replace(' ', '_')
    if job.debug:
        job_name += '_debug'
    holder = os.path.join(root_dir(), 'data', 'master', job_name)
    if not os.path.exists(holder):
        os.makedirs(holder)
    
    lock_f = os.path.join(holder, 'lock')
    if os.path.exists(lock_f):
        raise JobMasterRunning('There has been a running job master')
    open(lock_f, 'w').close()
    
    rpc_server = create_rpc_server(job)
    try:
        loader = JobLoader(job, nodes, rpc_server, context=context)
        loader.run()
    finally:
        os.remove(lock_f)
        rpc_server.shutdown()
    
if __name__ == '__main__':
    if len(sys.argv) < 3:
        raise ValueError('Master job loader need at least 2 parameters.')
    
    path = sys.argv[1]
    nodes = sys.argv[2:]
    load_job(path, nodes)