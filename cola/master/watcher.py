#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2013-6-5

@author: Chine
'''

import time
import threading

from cola.core.utils import get_ip
from cola.core.rpc import client_call

RUNNING, HANGUP, STOPPED = range(3)
CONTINOUS_HEARTBEAT = 60
HEARTBEAT_INTERVAL = 10
HEARTBEAT_CHECK_INTERVAL = 3*HEARTBEAT_INTERVAL

class MasterJobInfo(object):
    def __init__(self, port, nodes_ip_addresses, worker_port):
        self.job_master = '%s:%s' % (get_ip(), port)
        self.nodes = [
            '%s:%s'%(node_ip, worker_port) for node_ip in nodes_ip_addresses
        ]
        self.worker_port = worker_port
        
    def add_worker(self, node):
        if ':' not in node:
            node = '%s:%s' % (node, self.worker_port)
        self.nodes.append(node)
        client_call(self.job_master, 'add_node', node)
        
    def remove_worker(self, node):
        if ':' not in node:
            node = '%s:%s' % (node, self.worker_port)
        self.nodes.remove(node)
        client_call(self.job_master, 'remove_node', node)
        
    def has_node(self, node):
        if ':' not in node:
            node = '%s:%s' % (node, self.worker_port)
        return node in self.nodes
        
class WatcherInfo(object):
    def __init__(self, watcher):
        self.status = RUNNING
        self.continous_register = 1
        self.last_update = int(time.time())
        
    def register(self):
        self.continous_register += 1
        self.last_update = int(time.time())

class MasterWatcher(object):
    def __init__(self, rpc_server):
        self.nodes_watchers = {}
        self.running_jobs = {}
        self.black_list = []
        self.rpc_server = rpc_server
        self.ip_address = get_ip()
        
        self.stopped = False
        
        self.rpc_server.register_function(self.register_watcher_heartbeat, 
                                          'register_heartbeat')
        self.rpc_server.register_function(self.list_jobs, 'list_jobs')
        self.rpc_server.register_function(self.stop, 'stop')
        
    def register_watcher_heartbeat(self, node_watcher):
        if node_watcher not in self.nodes_watchers:
            watcher_info = WatcherInfo(node_watcher)
            self.nodes_watchers[node_watcher] = watcher_info
        else:
            watcher_info = self.nodes_watchers[node_watcher]
            watcher_info.register()
            
    def start_check_worker(self):
        def _check():
            for watcher, watcher_info in self.nodes_watchers.iteritems():
                ip_addr = watcher.split(':')[0]
                
                # if loose connection
                if int(time.time()) - watcher_info.last_update \
                    > HEARTBEAT_CHECK_INTERVAL:
                    
                    watcher_info.continous_register = 0
                    if watcher_info.status == RUNNING:
                        watcher_info.status = HANGUP
                    elif watcher_info.status == HANGUP:
                        watcher_info.status = STOPPED
                        self.black_list.append(watcher)
                        
                        for job_info in self.running_jobs.values():
                            if job_info.has_node(ip_addr):
                                job_info.remove_worker(ip_addr)
                        
                # if continously connect for more than 10 min
                elif watcher_info.continous_register >= CONTINOUS_HEARTBEAT:
                    if watcher_info.status != RUNNING:
                        watcher_info.status = RUNNING
                    if watcher in self.black_list:
                        self.black_list.remove(watcher)
                        
                    for job_info in self.running_jobs.values():
                        if not job_info.has_node(ip_addr):
                            job_info.add_worker(ip_addr)
                
        def _start():
            while not self.stopped:
                _check()
                time.sleep(HEARTBEAT_CHECK_INTERVAL)
        
        thread = threading.Thread(target=_start)
        thread.setDaemon(True)
        thread.start()
        return thread
        
    def list_jobs(self):
        return self.running_jobs.keys()
    
    def stop(self):
        self.stopped = True
        
    def run(self):
        thread = self.start_check_worker()
        thread.join()