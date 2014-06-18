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

Created on 2014-2-7

@author: Chine
'''

import os
import tempfile
import multiprocessing
import threading
import signal
import shutil

from cola.core.config import Config
from cola.core.utils import get_ip, import_job_desc, Clock
from cola.core.logs import get_logger
from cola.core.mq import MessageQueue
from cola.core.dedup import FileBloomFilterDeduper
from cola.core.rpc import ThreadedColaRPCServer
from cola.core.zip import ZipHandler
from cola.functions.budget import BudgetApplyServer
from cola.functions.speed import SpeedControlServer
from cola.functions.counter import CounterServer
from cola.job import Job, FINISHED
from cola.cluster.master import Master
from cola.cluster.worker import Worker

conf_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'conf')
main_conf = Config(os.path.join(conf_dir, 'main.yaml'))

class ContextManager(multiprocessing.managers.SyncManager):
    pass
ContextManager.register('deduper', FileBloomFilterDeduper)
ContextManager.register('mq', MessageQueue)
ContextManager.register('budget_server', BudgetApplyServer)
ContextManager.register('speed_server', SpeedControlServer)
ContextManager.register('counter_server', CounterServer)

def handler(signum, frame):
    pass
            
def manager_init():
    signal.signal(signal.SIGINT, handler)
                
class Context(object):
    fix_addr = lambda addr: addr if ':' in addr \
                    else '%s:%s'%(addr, main_conf.worker.port)
    fix_ip = lambda addr: addr if ':' not in addr \
                    else addr.split(':', 1)[0]
    
    def __init__(self, local_mode=False, is_master=False, master=None, 
                 is_client=False, working_dir=None, mkdirs=False, 
                 addr=None, addrs=None):
        self.is_local_mode = local_mode
        self.is_master = is_master
        self.is_client = is_client
        
        self.master = master
        self.master_ip = self.master
        if not self.is_local_mode:
            assert self.master is not None
            if ':' not in self.master:
                self.master = '%s:%s' % (self.master, main_conf.master.port)
            else:
                self.master_ip = self.master.split(':', 1)[0]
        
        self.working_dir = working_dir
        if self.working_dir is None:
            tmp = tempfile.gettempdir()
            self.working_dir = os.path.join(tmp, 'cola')
            if mkdirs and not os.path.exists(self.working_dir):
                os.makedirs(self.working_dir)
                
        self.addr = addr
        if self.addr is None:
            self.addr = get_ip()
        if ':' not in self.addr:
            if is_master:
                port = main_conf.master.port
            elif is_client:
                port = main_conf.client.port
            else:
                port = main_conf.worker.port
            self.addr = '%s:%s' % (self.addr, port)
        self.ip = self.addr.split(':', 1)[0]
        
        self.addrs = addrs
        if self.addrs is None:
            self.addrs = [self.addr, ]
        self.ips = [self.fix_ip(address) for address in self.addrs]
        self.addrs = [self.fix_addr(address) for address in self.addrs]
            
        self.manager = ContextManager()
        self.manager.start(manager_init)
        self.env = self.manager.dict({'ip': self.ip, 
                                      'root': self.working_dir,
                                      'is_local': self.is_local_mode, 
                                      'master_ip': self.master_ip})
        self.logger = get_logger('context')
        
        self.rpc_server = None
        
    def _get_name_and_dir(self, working_dir, job_name, 
                          overwrite=False, clear=False):
        src_job_name = job_name
        base_dir = working_dir
        src_working_dir = working_dir \
            = os.path.join(base_dir, job_name)
        idx = 1
        while os.path.exists(working_dir):
            if clear:
                shutil.rmtree(working_dir)
            if overwrite:
                job_name = '%s%s' % (src_job_name, idx)
                working_dir = os.path.join(base_dir, job_name)
                idx += 1
                
        if clear or not overwrite:
            return src_job_name, src_working_dir
        return job_name, working_dir
        
    def _run_local_job(self, job_path, overwrite=False, rpc_server=None):
        job_desc = import_job_desc(job_path)
        base_name = job_desc.uniq_name
        
        working_dir = os.path.join(self.working_dir, 'worker')
        clear = job_desc.settings.job.clear
        job_name, working_dir = self._get_name_and_dir(
            working_dir, base_name, overwrite=overwrite, clear=clear)
                    
        clock = Clock()
        job = Job(self, job_path, job_name=job_name, job_desc=job_desc,
                  working_dir=working_dir, rpc_server=rpc_server,
                  manager=self.manager)
        t = threading.Thread(target=job.run, args=(True, ))
        t.start()
        
        def stop(signum, frame):
            if 'main' not in multiprocessing.current_process().name.lower():
                return
            self.logger.debug("Catch interrupt signal, start to stop")
            job.shutdown()
            if rpc_server:
                rpc_server.shutdown()
            
        signal.signal(signal.SIGINT, stop)
        signal.signal(signal.SIGTERM, stop)
        
        while job.get_status() != FINISHED and t.is_alive():
            try:
                t.join(5)
            except IOError:
                break
        if not job.stopped.is_set() and job.get_status() == FINISHED:
            self.logger.debug('All objects have been fetched, try to finish job')
            job.shutdown()
            if rpc_server:
                rpc_server.shutdown()
        
        self.logger.debug('Job id:%s finished, spend %.2f seconds for running' % (
            job_name, clock.clock()))
        
    def run_job(self, job_path, overwrite=False, init_rpc=False):
        rpc_server = None
        if init_rpc:
            rpc_server = ThreadedColaRPCServer((self.ip, main_conf.worker.port))
            
        if self.is_local_mode:
            self._run_local_job(job_path, overwrite=overwrite, 
                                rpc_server=rpc_server)
        else:
            job_name = import_job_desc(job_path).uniq_name
            
            def create_zip(working_dir):
                zip_dir = os.path.join(self.working_dir, 'zip')
                filename = job_name + '.zip'
                zip_file = os.path.join(zip_dir, filename)
                
                ZipHandler.compress(zip_file, job_path, type_filters=('pyc', ))
                return job_name
            
            if hasattr(self, 'master'):
                create_zip(os.path.join(self.working_dir, 'master'))
                self.master.run_job(job_name, unzip=True)
            elif hasattr(self, 'worker'):
                create_zip(os.path.join(self.working_dir, 'worker'))
                self.worker.prepare(job_name, unzip=True)
                self.worker.run_job(job_name)
            
    def start_master(self):
        if self.rpc_server is None:
            self.rpc_server = ThreadedColaRPCServer((self.ip, main_conf.worker.port))
        
        self.master = Master(self)
        self.master.run()
        
    def start_worker(self):
        if self.rpc_server is None:
            self.rpc_server = ThreadedColaRPCServer((self.ip, main_conf.worker.port))
            
        self.worker = Worker(self)
        self.worker.run()