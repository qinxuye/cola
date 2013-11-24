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

import threading
import signal
import os

from cola.core.rpc import client_call
from cola.core.mq.client import MessageQueueClient
from cola.core.utils import get_ip, get_ips, root_dir, import_job
from cola.core.logs import LogRecordSocketReceiver, get_logger, \
                            add_log_client
from cola.core.config import main_conf
from cola.job.loader import JobLoader, LimitionJobLoader

class JobMasterRunning(Exception): pass

TIME_SLEEP = 10

class MasterJobLoader(LimitionJobLoader, JobLoader):
    def __init__(self, job, data_dir, nodes, local_ip=None, client=None,
                 context=None, copies=1, force=False):
        ctx = context or job.context
        master_port = ctx.job.master_port
        if local_ip is None:
            local_ip = get_ip()
        else:
            choices_ips = get_ips()
            if local_ip not in choices_ips:
                raise ValueError('IP address must be one of (%s)' % ','.join(choices_ips))
        local = '%s:%s' % (local_ip, master_port)
        
        JobLoader.__init__(self, job, data_dir, local, 
                           context=ctx, copies=copies, force=force)
        LimitionJobLoader.__init__(self, job, context=ctx)
        
        # check
        self.check()
        
        self.nodes = nodes
        self.not_registered = self.nodes[:]
        self.not_finished = self.nodes[:]
        
        # mq
        self.mq_client = MessageQueueClient(self.nodes, copies=copies)
        
        # lock
        self.ready_lock = threading.Lock()
        self.ready_lock.acquire()
        self.finish_lock = threading.Lock()
        self.finish_lock.acquire()
        
        # logger
        self.logger = get_logger(
            name='cola_master_%s'%self.job.real_name,
            filename=os.path.join(self.root, 'job.log'),
            is_master=True)
        self.client = client
        self.client_handler = None
        if self.client is not None:
            self.client_handler = add_log_client(self.logger, self.client)
        
        self.init_rpc_server()
        self.init_rate_clear()
        self.init_logger_server(self.logger)
        
        # register rpc server
        self.rpc_server.register_function(self.client_stop, 'client_stop')
        self.rpc_server.register_function(self.ready, 'ready')
        self.rpc_server.register_function(self.worker_finish, 'worker_finish')
        self.rpc_server.register_function(self.complete, 'complete')
        self.rpc_server.register_function(self.error, 'error')
        self.rpc_server.register_function(self.get_nodes, 'get_nodes')
        self.rpc_server.register_function(self.apply, 'apply')
        self.rpc_server.register_function(self.require, 'require')
        self.rpc_server.register_function(self.stop, 'stop')
        self.rpc_server.register_function(self.add_node, 'add_node')
        self.rpc_server.register_function(self.remove_node, 'remove_node')
        self.rpc_server.register_function(self.pages, 'pages')
        
        # register signal
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
    def init_logger_server(self, logger):
        self.log_server = LogRecordSocketReceiver(host=get_ip(), logger=logger)
        threading.Thread(target=self.log_server.serve_forever).start()
        
    def stop_logger_server(self):
        if hasattr(self, 'log_server'):
            self.log_server.shutdown()
            
    def client_stop(self):
        if self.client_handler is not None:
            self.logger.removeHandler(self.client_handler)
                
    def check(self):
        env_legal = self.check_env(force=self.force)
        if not env_legal:
            raise JobMasterRunning('There has been a running job master.')
        
    def release_lock(self, lock):
        try:
            lock.release()
        except:
            pass
        
    def finish(self):
        all_pages = self.pages()
        
        self.release_lock(self.ready_lock)
        self.release_lock(self.finish_lock)
        
        LimitionJobLoader.finish(self)
        JobLoader.finish(self)
        self.stop_logger_server()
        
        try:
            for handler in self.logger.handlers:
                handler.close()
        except:
            pass
            
        if self.client is not None:
            rpc_client = '%s:%s' % (
                self.client.split(':')[0], 
                main_conf.client.port
            )
            client_call(rpc_client, 'stop', ignore=True)
            
        self.logger.info('All nodes finishes visiting pages size: %s' % all_pages)
        self.stopped = True
        
    def stop(self):
        for node in self.nodes:
            client_call(node, 'stop', ignore=True)
        self.finish()
        
    def signal_handler(self, signum, frame):
        self.stop()
        
    def get_nodes(self):
        return self.nodes
        
    def ready(self, node):
        if node in self.not_registered:
            self.not_registered.remove(node)
            if len(self.not_registered) == 0:
                self.ready_lock.release()
                
    def worker_finish(self, node):
        if node in self.not_finished:
            self.not_finished.remove(node)
            if len(self.not_finished) == 0:
                self.finish_lock.release()
                
    def add_node(self, node):
        for node in self.nodes:
            client_call(node, 'add_node', node, ignore=True)
        self.nodes.append(node)
        client_call(node, 'run', ignore=True)
        
    def remove_node(self, node):
        for node in self.nodes:
            client_call(node, 'remove_node', node, ignore=True)
        if node in self.nodes:
            self.nodes.remove(node)
            
    def pages(self):
        all_pages = 0
        for node in self.nodes:
            pages = client_call(node, 'pages', ignore=True)
            if pages is not None:
                all_pages += int(pages)
        return all_pages
        
    def run(self):
        self.ready_lock.acquire()
        
        if not self.stopped and len(self.not_registered) == 0:
            self.mq_client.put(self.job.starts)
            for node in self.nodes:
                client_call(node, 'run')
            
        self.finish_lock.acquire()
        
        master_watcher = '%s:%s' % (get_ip(), main_conf.master.port)
        client_call(master_watcher, 'finish_job', self.job.real_name, ignore=True)
        
    def __enter__(self):
        return self
    
    def __exit__(self, type_, value, traceback):
        self.finish()

def load_job(job_path, nodes, ip_address=None, data_path=None, 
             client=None, context=None, force=False):
    if not os.path.exists(job_path):
        raise ValueError('Job definition does not exist.')
        
    job = import_job(job_path)
    
    if data_path is None:
        data_path = os.path.join(root_dir(), 'data')
    root = os.path.join(data_path, 'master', 'jobs', job.real_name)
    if not os.path.exists(root):
        os.makedirs(root)
    
    with MasterJobLoader(job, root, nodes, local_ip=ip_address, client=client, 
                         context=context, force=force) as job_loader:
        job_loader.run()
    
if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser('Cola job loader')
    parser.add_argument('-j', '--job', metavar='job directory', required=True,
                        help='job directory to run')
    parser.add_argument('-d', '--data', metavar='data root directory', nargs='?',
                        default=None, const=None, 
                        help='root directory to put data')
    parser.add_argument('-i', '--ip', metavar='IP address', nargs='?',
                        default=None, const=None, 
                        help='IP Address to start')
    parser.add_argument('-f', '--force', metavar='force start', nargs='?',
                        default=False, const=True, type=bool)
    parser.add_argument('-n', '--nodes', metavar='worker job loaders', required=True, nargs='+',
                        help='worker connected(each in the former of `ip:port`)')
    parser.add_argument('-c', '--client', metavar='client', nargs='?',
                        default=None, const=None,
                        help='client which starts the job')
    args = parser.parse_args()
    
    path = args.job
    data_path = args.data
    ip_address = args.ip
    nodes = args.nodes
    if len(nodes) == 1:
        nodes = nodes[0].split(' ')
    force = args.force
    client = args.client
    load_job(path, nodes, ip_address=ip_address, 
             data_path=data_path, 
             client=client, force=force)