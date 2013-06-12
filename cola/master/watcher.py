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

Created on 2013-6-5

@author: Chine
'''

import time
import threading
import os
import subprocess
import shutil
import socket

from cola.core.utils import get_ip
from cola.core.rpc import client_call, ColaRPCServer, \
    FileTransportServer, FileTransportClient
from cola.core.zip import ZipHandler
from cola.core.utils import import_job, root_dir
from cola.core.config import main_conf

RUNNING, HANGUP, STOPPED = range(3)
CONTINOUS_HEARTBEAT = 60
HEARTBEAT_INTERVAL = 10
HEARTBEAT_CHECK_INTERVAL = 3*HEARTBEAT_INTERVAL

class MasterWatcherRunning(Exception): pass

class MasterJobInfo(object):
    def __init__(self, port, nodes_ip_addresses, worker_port, popen=None):
        self.job_master = '%s:%s' % (get_ip(), port)
        self.nodes = [
            '%s:%s'%(node_ip, worker_port) for node_ip in nodes_ip_addresses
        ]
        self.worker_port = worker_port
        self.popen = None
        
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
        
    def has_worker(self, node):
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
    def __init__(self, rpc_server, zip_dir, job_dir):
        self.rpc_server = rpc_server
        self.zip_dir = zip_dir
        self.job_dir = job_dir
        
        self.nodes_watchers = {}
        self.running_jobs = {}
        self.black_list = []
        self.ip_address = get_ip()
        
        self.stopped = False
        
        self.rpc_server.register_function(self.register_watcher_heartbeat, 
                                          'register_heartbeat')
        self.rpc_server.register_function(self.stop, 'stop')
        self.rpc_server.register_function(self.list_jobs, 'list_jobs')
        self.rpc_server.register_function(self.start_job, 'start_job')
        self.rpc_server.register_function(self.stop_job, 'stop_job')
        self.rpc_server.register_function(self.finish_job, 'finish_job')
        self.rpc_server.register_function(self.clear_job, 'clear_job')
        self.rpc_server.register_function(self.list_job_dirs, 'list_job_dirs')
        self.rpc_server.register_function(self.list_workers, 'list_workers')
        
        self.set_receiver(zip_dir)
        
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
                            if job_info.has_worker(ip_addr):
                                job_info.remove_worker(ip_addr)
                        
                # if continously connect for more than 10 min
                elif watcher_info.continous_register >= CONTINOUS_HEARTBEAT:
                    if watcher_info.status != RUNNING:
                        watcher_info.status = RUNNING
                    if watcher in self.black_list:
                        self.black_list.remove(watcher)
                        
                    for job_info in self.running_jobs.values():
                        if not job_info.has_worker(ip_addr):
                            job_info.add_worker(ip_addr)
                
        def _start():
            while not self.stopped:
                _check()
                time.sleep(HEARTBEAT_CHECK_INTERVAL)
        
        thread = threading.Thread(target=_start)
        thread.setDaemon(True)
        thread.start()
        return thread
    
    def list_workers(self):
        return self.nodes_watchers.keys()
        
    def list_jobs(self):
        return self.running_jobs.keys()
    
    def list_job_dirs(self):
        return os.listdir(self.job_dir)
    
    def set_receiver(self, base_dir):
        serv = FileTransportServer(self.rpc_server, base_dir)
        return serv
    
    def start_job(self, zip_filename, uncompress=True):
        if uncompress:
            zip_file = os.path.join(self.zip_dir, zip_filename)
            
            # transfer zip file to workers
            for watcher in self.nodes_watchers:
                if watcher.split(':')[0] == self.ip_address:
                    continue
                file_trans_client = FileTransportClient(watcher, zip_file)
                file_trans_client.send_file()
            
            job_dir = ZipHandler.uncompress(zip_file, self.job_dir)
        else:
            job_dir = os.path.join(self.job_dir, zip_filename.rsplit('.', 1)[0])
            
        job = import_job(job_dir)
        
        worker_port = job.context.job.port
        port = job.context.job.master_port
        nodes = [watcher.split(':')[0] for watcher in self.nodes_watchers]
        
        if len(nodes) > 0:
            info = MasterJobInfo(port, nodes, worker_port)
            self.running_jobs[job.real_name] = info
            
            dirname = os.path.dirname(os.path.abspath(__file__))
            f = os.path.join(dirname, 'loader.py')
            workers = ['%s:%s'%(node, worker_port) for node in nodes]
            popen = subprocess.Popen('python "%(py)s" "%(job_dir)s" %(nodes)s' % {
                'py': f,
                'job_dir': job_dir,
                'nodes': ' '.join(workers)
            })
            info.popen = popen
            
            # call workers to start job
            for worker_watcher in self.nodes_watchers:
                client_call(worker_watcher, 'start_job', zip_filename, uncompress)
    
    def stop_job(self, job_real_name):
        if job_real_name not in self.running_jobs:
            return False
        job_info = self.running_jobs[job_real_name]
        client_call(job_info.job_master, 'stop')
        
        for watcher in self.nodes_watchers.keys():
            client_call(watcher, 'kill', job_real_name)
        self.kill(job_real_name)
        
        return True
    
    def finish_job(self, job_real_name):
        del self.running_jobs[job_real_name]
    
    def clear_job(self, job_name):
        job_name = job_name.replace(' ', '_')
        path = os.path.join(self.job_dir, job_name)
        shutil.rmtree(path)
        
        for watcher in self.nodes_watchers:
            client_call(watcher, 'clear_job')
    
    def stop(self):
        for watcher in self.nodes_watchers:
            client_call(watcher, 'stop')
        # stop all jobs
        for job_info in self.running_jobs.values():
            try:
                client_call(job_info.job_master, 'stop')
            except socket.error:
                pass
        self.stopped = True
        
    def kill(self, job_realname):
        if job_realname in self.running_jobs.keys():
            self.running_jobs[job_realname].popen.kill()
        
    def run(self):
        thread = self.start_check_worker()
        thread.join()
        
def makedirs(path):
    if not os.path.exists(master_watcher_dir):
        os.makedirs(master_watcher_dir)
        
def create_rpc_server():
    rpc_server = ColaRPCServer((get_ip(), main_conf.master.port))
    thd = threading.Thread(target=rpc_server.serve_forever)
    thd.setDaemon(True)
    thd.start()
    return rpc_server
        
if __name__ == "__main__":
    root = root_dir()
    master_watcher_dir = os.path.join(root, 'data', 'master', 'watcher')
    makedirs(master_watcher_dir)
    zip_dir = os.path.join(root, 'data', 'zip')
    makedirs(zip_dir)
    job_dir = os.path.join(root, 'data', 'jobs')
    makedirs(job_dir)
    
    lock_f = os.path.join(master_watcher_dir, 'lock')
    if os.path.exists(lock_f):
        raise MasterWatcherRunning('There has been a running master watcher.')
    
    rpc_server = create_rpc_server()
    try:
        open(lock_f, 'w').close()
        
        master_watcher = MasterWatcher(rpc_server, zip_dir, job_dir)
        master_watcher.run()
    finally:
        rpc_server.shutdown()
        os.remove(lock_f)