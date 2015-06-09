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
from cola.core.dedup import FileBloomFilterDeduper, MapDeduper
from cola.core.rpc import ThreadedColaRPCServer, client_call
from cola.core.zip import ZipHandler
from cola.functions.budget import BudgetApplyServer
from cola.functions.speed import SpeedControlServer
from cola.functions.counter import CounterServer
from cola.job import Job, FINISHED, IDLE
from cola.cluster.master import Master
from cola.cluster.worker import Worker

conf_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'conf')
main_conf = Config(os.path.join(conf_dir, 'main.yaml'))

MAX_IDLE_TIMES = 50

class ContextManager(multiprocessing.managers.SyncManager):
    pass

ContextManager.register('FileBloomFilterDeduper', FileBloomFilterDeduper)
ContextManager.register('MapDeduper', MapDeduper)
ContextManager.register('mq', MessageQueue)
ContextManager.register('budget_server', BudgetApplyServer)
ContextManager.register('speed_server', SpeedControlServer)
ContextManager.register('counter_server', CounterServer)

def handler(signum, frame):
    pass
            
def manager_init():
    signal.signal(signal.SIGINT, handler)
                
class Context(object):
    fix_addr = lambda _, addr: addr if ':' in addr \
                    else '%s:%s'%(addr, main_conf.worker.port)
    fix_ip = lambda _, addr: addr if ':' not in addr \
                    else addr.split(':', 1)[0]
    
    def __init__(self, local_mode=False, is_master=False, master_addr=None, 
                 is_client=False, working_dir=None, mkdirs=False, 
                 ip=None, ips=None):
        self.is_local_mode = local_mode
        self.is_master = is_master
        self.is_client = is_client
        
        self.master_addr = master_addr
        self.master_ip = self.master_addr
        if not self.is_local_mode:
            if self.master_addr is None:
                raise ValueError('Master address must be supplied when local_mode is False')
                
            if ':' not in self.master_addr:
                self.master_addr = '%s:%s' % (self.master_addr, main_conf.master.port)
            else:
                self.master_ip = self.master_addr.split(':', 1)[0]
        
        self.working_dir = working_dir
        if self.working_dir is None:
            tmp = tempfile.gettempdir()
            self.working_dir = os.path.join(tmp, 'cola')
            if mkdirs and not os.path.exists(self.working_dir):
                os.makedirs(self.working_dir)
                
        self.ip = ip
        if self.ip is None:
            if self.is_master:
                self.ip = self.master_ip
            else:
                self.ip = get_ip()
                if self.is_local_mode and not self.ip:
                    self.ip = '127.0.0.1'
        if self.master_addr is None: self.master_addr = '%s:%s' % (self.ip, main_conf.master.port)
        self.worker_addr = '%s:%s' % (self.ip, main_conf.worker.port)
        
        self.ips = ips if ips is not None else []
        if not self.ips:
            self.ips.append(self.ip)
        self.addrs = [self.fix_addr(_ip) for _ip in self.ips]
            
        self.manager = ContextManager()
        self.manager.start(manager_init)
        self.env = self.manager.dict({'ip': self.ip, 
                                      'root': self.working_dir,
                                      'is_local': self.is_local_mode, 
                                      'master_ip': self.master_ip,
                                      'job_desc' : {}
                                      })
        self.logger = get_logger('cola_context')
        
        self.master_rpc_server = None
        self.worker_rpc_server = None
        
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
            if not clear and not overwrite:
                break
                
        if clear or not overwrite:
            return src_job_name, src_working_dir
        return job_name, working_dir
        
    def _run_local_job(self, job_path, overwrite=False, rpc_server=None, settings=None):
        job_desc = import_job_desc(job_path)
        if settings is not None: job_desc.update_settings(settings)
        base_name = job_desc.uniq_name
        self.env['job_desc'][base_name] = job_desc
        
        working_dir = os.path.join(self.working_dir, 'worker')
        clear = job_desc.settings.job.clear
        job_name, working_dir = self._get_name_and_dir(
            working_dir, base_name, overwrite=overwrite, clear=clear)
                    
        clock = Clock()
        job = Job(self, job_path, job_name, job_desc=job_desc,
                  working_dir=working_dir, rpc_server=rpc_server,
                  manager=self.manager)
        t = threading.Thread(target=job.run, args=(True, ))
        t.start()
        
        stopped = multiprocessing.Event()
        def stop(signum, frame):
            if 'main' not in multiprocessing.current_process().name.lower():
                return
            if stopped.is_set():
                return
            else:
                stopped.set()
                
            self.logger.debug("Catch interrupt signal, start to stop")
            job.shutdown()
            if rpc_server:
                rpc_server.shutdown()
            
        signal.signal(signal.SIGINT, stop)
        signal.signal(signal.SIGTERM, stop)
        
        idle_times = 0
        while t.is_alive():
            if job.get_status() == FINISHED:
                break
            if job.get_status() == IDLE:
                idle_times += 1
                if idle_times > MAX_IDLE_TIMES:
                    break
            else:
                idle_times = 0
            
            try:
                t.join(5)
            except IOError:
                break
            
        need_shutdown = False
        if not job.stopped.is_set() and job.get_status() == FINISHED:
            self.logger.debug('All objects have been fetched, try to finish job')
            need_shutdown = True
        elif not stopped.is_set() and not t.is_alive():
            need_shutdown = True
        elif not job.stopped.is_set() and job.get_status() == IDLE:
            self.logger.debug('No bundle or url to perform, try to finish job')
            need_shutdown = True
            
        if need_shutdown is True:
            job.shutdown()
            if rpc_server:
                rpc_server.shutdown()

        self.logger.debug('Job id:%s finished, spend %.2f seconds for running' % (
            job_name, clock.clock()))
        
    def run_job(self, job_path, overwrite=False, init_rpc=False, settings=None):
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
        if not self.is_master:
            return
        
        if self.master_rpc_server is None:
            self.master_rpc_server = ThreadedColaRPCServer((self.ip, 
                                                            main_conf.master.port))
        
        self.master = Master(self)
        self.master.run()
        
        return self.master
        
    def start_worker(self):
        if self.worker_rpc_server is None:
            self.worker_rpc_server = ThreadedColaRPCServer((self.ip, 
                                                            main_conf.worker.port))
            
        self.worker = Worker(self)
        self.worker.run()
        
        return self.worker
    
    def kill_master(self):
        if self.is_master and self.master is not None:
            self.master.shutdown()
        elif self.is_client:
            client_call(self.master_addr, 'shutdown')
            
    def list_workers(self):
        if self.is_master and self.master is not None:
            return self.master.list_workers()
        else:
            return client_call(self.master_addr, 'list_workers')
        
    def list_jobs(self):
        jobs = {}
        if self.is_master and self.master is not None:
            runnable_jobs = self.master.list_runnable_jobs()
            running_jobs = self.master.job_tracker.running_jobs
        else:
            runnable_jobs = client_call(self.master_addr, 'runnable_jobs')
            running_jobs = client_call(self.master_addr, 'running_jobs')
        for job_id, job_name in runnable_jobs.iteritems():
            jobs[job_id] = {'name': job_name}
            if job_id in running_jobs:
                jobs[job_id]['status'] = 'running'
            else:
                jobs[job_id]['status'] = 'stopped'
        
        return jobs

    def kill_job(self, job_id):
        if self.is_master and self.master is not None:
            self.master.stop_job(job_id)
        else:
            client_call(self.master_addr, 'stop_job', job_id)
            
    def get_job_counter(self, job_id):
        if self.is_master and self.master is not None:
            return self.master.counter_server.output()
        else:
            from cola.functions.counter import FUNC_PREFIX
            from cola.core.utils import get_rpc_prefix
            
            func_name = '%s%s' % (get_rpc_prefix(job_id, FUNC_PREFIX), 'get_global')
            
            return client_call(self.master_addr, func_name)
        
    def pack_job_error(self, job_id):
        if self.is_master and self.master is not None:
            return self.master.pack_job_error(job_id)
        else:
            client_call(self.master_addr, 'pack_job_error', job_id)