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

from cola.functions.counter import CounterServer
from cola.functions.budget import BudgetApplyServer
from cola.functions.speed import SpeedControlServer
from cola.cluster.tracker import WorkerTracker, JobTracker

RUNNING, HANGUP, STOPPED = range(3)
CONTINOUS_HEARTBEAT = 90
HEARTBEAT_INTERVAL = 20
HEARTBEAT_CHECK_INTERVAL = 3*HEARTBEAT_INTERVAL

class JobMaster(object):
    def __init__(self, ctx, job_name, job_desc):
        self.working_dir = os.path.join(ctx.working_dir, 'master', 'tracker')
        if not os.path.exists(self.working_dir):
            os.makedirs(self.working_dir)
            
        self.job_name = job_name
        self.job_desc = job_desc
        self.settings = job_desc.settings
        self.rpc_server = ctx.rpc_server
        
        self.init()
        
        self.workers = ctx.addrs[:]
            
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
        self._init_counter_server()
        self._init_budget_server()
        self._init_speed_server()
        
    def remove_worker(self, worker):
        if worker not in self.workers:
            return
        
        # rpc call the other workers to remove this worker
        
    def add_worker(self, worker):
        if worker in self.workers:
            return
        
        # rpc call the other workers to add this worker
        
    def has_worker(self, worker):
        return worker in self.workers

class Master(object):
    def __init__(self, ctx):
        self.ctx = ctx
        
        self.worker_tracker = WorkerTracker()
        self.job_tracker = JobTracker()
        
        self.stopped = threading.Event()
        
    def register_heartbeat(self, worker):
        self.worker_tracker.register_worker(worker)
        return self.worker_tracker.workers.keys()
    
    def _check(self):
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
                        self.job_tracker.add_worker(job, worker)
                        
    def run(self):
        self._t = threading.Thread(target=self._check)
        self._t.setDaemon(True)
        self._t.start()
        
    def shutdown(self):
        if not hasattr(self, '_t'):
            return
        self.stopped.set()
        self._t.join()