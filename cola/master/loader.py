#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
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
from cola.core.utils import get_ip, root_dir

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
        
        # register signal
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
    def ready(self, node):
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
        
    dir_, name = os.path.split(path)
    if os.path.isfile(path):
        name = name.rstrip('.py')
    sys.path.insert(0, dir_)
    job_module = __import__(name)
    job = job_module.get_job()
    
    holder = os.path.join(root_dir(), 'master', job.name.replace(' ', '_'))
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