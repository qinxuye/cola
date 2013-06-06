#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2013-6-5

@author: Chine
'''

import time
import threading
import os
import subprocess
import shutil

from cola.core.utils import get_ip
from cola.core.rpc import client_call, ColaRPCServer, \
    FileTransportServer, FileTransportClient
from cola.core.zip import ZipHandler
from cola.core.utils import import_job, root_dir
from cola.job.conf import main_conf

RUNNING, HANGUP, STOPPED = range(3)
CONTINOUS_HEARTBEAT = 60
HEARTBEAT_INTERVAL = 10
HEARTBEAT_CHECK_INTERVAL = 3*HEARTBEAT_INTERVAL

class MasterWatcherRunning(Exception): pass

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
        self.rpc_server.register_function(self.list_jobs, 'list_jobs')
        self.rpc_server.register_function(self.stop, 'stop')
        self.rpc_server.register_function(self.start_job, 'start_job')
        self.rpc_server.register_function(self.stop_job, 'stop_job')
        self.rpc_server.register_function(self.clear_job, 'clear_job')
        
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
        
    def list_jobs(self):
        return self.running_jobs.keys()
    
    def set_receiver(self, base_dir):
        serv = FileTransportServer(self.rpc_server, base_dir)
        return serv
    
    def start_job(self, zip_filename):
        zip_file = os.path.join(self.zip_dir, zip_filename)
        
        # transfer zip file to workers
        for watcher in self.nodes_watchers:
            if watcher.split(':')[0] == self.ip_address:
                continue
            file_trans_client = FileTransportClient(watcher, zip_file)
            file_trans_client.send_file()
        
        job_dir = ZipHandler.uncompress(zip_file, self.job_dir)
        job = import_job(job_dir)
        
        worker_port = job.context.job.port
        port = job.context.job.master_port
        nodes = [watcher.split(':')[0] for watcher in self.nodes_watchers],
        info = MasterJobInfo(port, nodes, worker_port)
        self.running_jobs[job.real_name] = info
        
        dirname = os.path.dirname(os.path.abspath(__file__))
        f = os.path.join(dirname, 'loader.py')
        workers = ['%s:%s'%(node, worker_port) for node in nodes]
        return_code = subprocess.call('python "%(py)s" "%(job_dir)s" %(nodes)s' % {
            'py': f,
            'job_dir': job_dir,
            'nodes': ' '.join(workers)
        })
        
        # call workers to start job
        for worker_watcher in self.nodes_watchers:
            client_call(worker_watcher, 'start_job', zip_filename)
        
        return return_code == 0
    
    def stop_job(self, job_real_name):
        if job_real_name not in self.running_jobs:
            return False
        job_info = self.running_jobs[job_real_name]
        client_call(job_info.job_master, 'stop')
        return True
    
    def clear_job(self, job_name):
        job_name = job_name.replace(' ', '_')
        path = os.path.join(self.job_dir, job_name)
        shutil.rmtree(path)
        
        for watcher in self.nodes_watchers:
            client_call(watcher, 'clear_job')
    
    def stop(self):
        self.stopped = True
        
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