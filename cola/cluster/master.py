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

Created on 2014-6-12

@author: chine
'''

import os
import time
import threading
try:
    import cPickle as pickle
except ImportError:
    import pickle

from cola.functions.counter import CounterServer
from cola.functions.budget import BudgetApplyServer, ALLFINISHED
from cola.functions.speed import SpeedControlServer
from cola.cluster.tracker import WorkerTracker, JobTracker
from cola.cluster.stage import Stage
from cola.core.rpc import FileTransportServer, FileTransportClient, \
                            client_call
from cola.core.zip import ZipHandler
from cola.core.utils import import_job_desc
from cola.core.logs import get_logger, LogRecordSocketReceiver

RUNNING, HANGUP, STOPPED = range(3)
STATUSES = ['RUNNING', 'HANGUP', 'STOPPED']
CONTINOUS_HEARTBEAT = 90
HEARTBEAT_INTERVAL = 20
HEARTBEAT_CHECK_INTERVAL = 3*HEARTBEAT_INTERVAL
JOB_CHECK_INTERVAL = 5
JOB_META_STATUS_FILENAME = 'job.meta.status'

class JobMaster(object):
    def __init__(self, ctx, job_name, job_desc, workers):
        self.working_dir = os.path.join(ctx.working_dir, 'master', 
                                        'tracker', job_name)
        if not os.path.exists(self.working_dir):
            os.makedirs(self.working_dir)
            
        self.job_name = job_name
        self.job_desc = job_desc
        self.settings = job_desc.settings
        self.rpc_server = ctx.master_rpc_server
        
        self.inited = False
        self.init()
        
        self.workers = workers
            
    def _init_counter_server(self):
        counter_dir = os.path.join(self.working_dir, 'counter')
        self.counter_server = CounterServer(counter_dir, self.settings,
                                            rpc_server=self.rpc_server, 
                                            app_name=self.job_name)
        
    def _init_budget_server(self):
        budget_dir = os.path.join(self.working_dir, 'budget')
        self.budget_server = BudgetApplyServer(budget_dir, self.settings,
                                          rpc_server=self.rpc_server, 
                                          app_name=self.job_name)
        
    def _init_speed_server(self):
        speed_dir = os.path.join(self.working_dir, 'speed')
        self.speed_server = SpeedControlServer(speed_dir, self.settings,
                                               rpc_server=self.rpc_server,
                                               app_name=self.job_name)
        
    def init(self):
        if self.inited:
            return
        
        self._init_counter_server()
        self._init_budget_server()
        self._init_speed_server()
        
        self.inited = True
                
    def remove_worker(self, worker):
        if worker not in self.workers:
            return
        
        # rpc call the other workers to remove this worker
        self.workers.remove(worker)
        for node in self.workers:
            client_call(node, 'remove_node', worker)
        
    def add_worker(self, worker):
        if worker in self.workers:
            return
        
        # rpc call the other workers to add this worker
        for node in self.workers:
            client_call(node, 'add_node', worker)
        self.workers.append(worker)
        
    def has_worker(self, worker):
        return worker in self.workers
    
    def shutdown(self):
        if not self.inited:
            return
        
        self.counter_server.shutdown()
        self.budget_server.shutdown()
        self.speed_server.shutdown()
        
        self.inited = False

class Master(object):
    def __init__(self, ctx):
        self.ctx = ctx
        self.rpc_server = self.ctx.master_rpc_server
        assert self.rpc_server is not None
        
        self.working_dir = os.path.join(self.ctx.working_dir, 'master')
        self.zip_dir = os.path.join(self.working_dir, 'zip')
        self.job_dir = os.path.join(self.working_dir, 'jobs')
        
        self.worker_tracker = WorkerTracker()
        self.job_tracker = JobTracker()
        
        self.stopped = threading.Event()
        
        self.logger = get_logger("cola_master")
        self._init_log_server(self.logger)
        
        self._register_rpc()
        self.load()
        FileTransportServer(self.rpc_server, self.zip_dir)
        
    def load(self):
        self.runned_job_metas = {}
        
        job_meta_file = os.path.join(self.working_dir, JOB_META_STATUS_FILENAME)
        if os.path.exists(job_meta_file) and \
            os.path.getsize(job_meta_file) > 0:
            try:
                with open(job_meta_file) as f:
                    self.runned_job_metas = pickle.load(f)
            except:
                pass
    
    def save(self):
        job_meta_file = os.path.join(self.working_dir, JOB_META_STATUS_FILENAME)
        with open(job_meta_file, 'w') as f:
            pickle.dump(self.runned_job_metas, f)
        
    def _register_rpc(self):
        self.rpc_server.register_function(self.run_job, 'run_job')
        self.rpc_server.register_function(self.stop_job, 'stop_job')
        self.rpc_server.register_function(self.list_runnable_jobs, 
                                          'runnable_jobs')
        self.rpc_server.register_function(lambda: self.job_tracker.running_jobs,
                                          'running_jobs')
        self.rpc_server.register_function(self.list_workers,
                                          'list_workers')
        self.rpc_server.register_function(self.shutdown, 'shutdown')
        self.rpc_server.register_function(self.register_heartbeat, 
                                          'register_heartbeat')
        
    def register_heartbeat(self, worker):
        self.worker_tracker.register_worker(worker)
        return self.worker_tracker.workers.keys()
    
    def _init_log_server(self, logger):
        self.log_server = LogRecordSocketReceiver(host=self.ctx.ip, 
                                                  logger=self.logger)
        self.log_t = threading.Thread(target=self.log_server.serve_forever)
        self.log_t.start()
        
    def _shutdown_log_server(self):
        if hasattr(self, 'log_server'):
            self.log_server.shutdown()
            self.log_t.join()
    
    def _check_workers(self):
        while not self.stopped.is_set():
            for worker, info in self.worker_tracker.workers.iteritems():
                # if loose connection
                if int(time.time()) - info.last_update \
                    > HEARTBEAT_CHECK_INTERVAL:
                    
                    info.continous_register = 0
                    if info.status == RUNNING:
                        info.status = HANGUP
                    elif info.status == HANGUP:
                        info.status = STOPPED
                        self.black_list.append(worker)
                        
                        for job in self.job_tracker.running_jobs:
                            self.job_tracker.remove_worker(job, worker)
                        
                # if continously connect for more than 10 min
                elif info.continous_register >= CONTINOUS_HEARTBEAT:
                    if info.status != RUNNING:
                        info.status = RUNNING
                    if worker in self.black_list:
                        self.black_list.remove(worker)
                        
                    for job in self.job_tracker.running_jobs:
                        if not client_call(worker, 'has_job'):
                            client_call(worker, 'prepare', job)
                            client_call(worker, 'run_job', job)
                        self.job_tracker.add_worker(job, worker)
                
            self.stopped.wait(HEARTBEAT_CHECK_INTERVAL)
                        
    def _check_jobs(self):
        while not self.stopped.is_set():
            for job_master in self.job_tracker.running_jobs.values():
                if job_master.budget_server.get_status() == ALLFINISHED:
                    self.stop_job(job_master.job_name)
                    self.job_tracker.remove_job(job_master.job_name)
            self.stopped.wait(JOB_CHECK_INTERVAL)
                        
    def _unzip(self, job_name):
        zip_file = os.path.join(self.zip_dir, job_name)
        if os.path.exists(zip_file):
            ZipHandler.uncompress(zip_file, self.job_dir)
            
    def _register_runned_job(self, job_name, job_desc):
        self.runned_job_metas[job_name] = {'job_name': job_desc.name,
                                           'created': time.time()}
                        
    def run(self):
        self._worker_t = threading.Thread(target=self._check_workers)
        self._worker_t.start()
        
        self._job_t = threading.Thread(target=self._check_jobs)
        self._job_t.start()
        
    def run_job(self, job_name, unzip=False, 
                wait_for_workers=False):
        if wait_for_workers:
            while not self.stopped.is_set():
                if len(self.worker_tracker.workers) > 0:
                    break
                stopped = self.stopped.wait(3)
                if stopped:
                    return
        
        if unzip:
            self._unzip(job_name)
        
        job_path = os.path.join(self.job_dir, job_name)
        job_desc = import_job_desc(job_path)
        job_master = JobMaster(self.ctx, job_name, job_desc, 
                               self.worker_tracker.workers.keys())
        job_master.init()
        self.job_tracker.register_job(job_name, job_master)
        self._register_runned_job(job_name, job_desc)
        
        zip_file = os.path.join(self.zip_dir, job_name+'.zip')
        for worker in job_master.workers:
            FileTransportClient(worker, zip_file).send_file()
        
        self.logger.debug(
            'entering the master prepare stage, job id: %s' % job_name)
        self.logger.debug(
            'job available workers: %s' % job_master.workers)
        stage = Stage(job_master.workers, 'prepare')
        stage.barrier(True, job_name)
        
        self.logger.debug(
            'entering the master run_job stage, job id: %s' % job_name)
        stage = Stage(job_master.workers, 'run_job')
        stage.barrier(True, job_name)
        
    def stop_job(self, job_name):
        job_master = self.job_tracker.get_job_master(job_name)
        stage = Stage(job_master.workers, 'stop_job')
        stage.barrier(True, job_name)
        
        stage = Stage(job_master.workers, 'clear_job')
        stage.barrier(True, job_name)
        
        job_master.shutdown()
        
    def list_runnable_jobs(self):
        job_dirs = filter(lambda s: os.path.isdir(os.path.join(self.job_dir, s)), 
                          os.listdir(self.job_dir))
        
        jobs = {}
        for job_dir in job_dirs:
            desc = import_job_desc(os.path.join(self.job_dir, job_dir))
            jobs[job_dir] = desc.name
        return jobs
        
    def has_running_jobs(self):
        return len(self.job_tracker.running_jobs) > 0
    
    def list_workers(self):
        return [(worker, STATUSES[worker_info.status]) for worker, worker_info \
                in self.worker_tracker.workers.iteritems()]
        
    def _stop_all_jobs(self):
        for job_name in self.job_tracker.running_jobs.keys():
            self.stop_job(job_name)
            del self.job_tracker.running_jobs[job_name]
            
    def _shutdown_all_workers(self):
        stage = Stage(self.worker_tracker.workers.keys(), 'shutdown')
        stage.barrier(True)
        
    def shutdown(self):
        if not hasattr(self, '_worker_t'):
            return
        if not hasattr(self, '_job_t'):
            return
        
        self.logger.debug('master starts to shutdown')
        
        self.stopped.set()
        self._stop_all_jobs()
        self._shutdown_all_workers()
        
        self._worker_t.join()
        self._job_t.join()
        
        self.save()
        self.rpc_server.shutdown()
        self.logger.debug('master shutdown finished')
        self._shutdown_log_server()