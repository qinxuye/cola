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

Created on 2014-5-17

@author: chine
'''

import os
import multiprocessing
import threading

from cola.core.utils import import_job_desc, get_ip
from cola.core.logs import get_logger
from cola.job.task import Task
from cola.functions.budget import BudgetApplyClient
from cola.functions.speed import SpeedControlClient
from cola.functions.counter import CounterClient

MAX_IDLE_TIMES = 5

class Container(object):
    def __init__(self, container_id, working_dir, 
                 job_path, job_name, env, mq,
                 counter_server, budget_server, speed_server,
                 stopped, nonsuspend, idle_statuses, n_tasks=1, 
                 is_local=False, master_ip=None, logger=None,
                 task_start_id=0):
        self.container_id = container_id
        self.working_dir = working_dir
        self.mq = mq
        self.env = env
        self.job_name = job_name
        self.job_desc = env.get('job_desc_%s' % job_name) or \
                        import_job_desc(job_path)
        
        self.counter_server = counter_server
        self.budget_server = budget_server
        self.speed_server = speed_server
        
        self.stopped = stopped
        self.nonsuspend = nonsuspend
        self.idle_statuses = idle_statuses
        self.n_tasks = n_tasks
        self.is_local = is_local
        self.master_ip = master_ip
        self.logger = logger
        
        self.task_start_id = task_start_id
        self.ip = self.env.get('ip', None) or get_ip()
        
        self.counter_clients = [None for _ in range(self.n_tasks)]
        self.budget_clients = [None for _ in range(self.n_tasks)]
        self.speed_clients = [None for _ in range(self.n_tasks)]
        
        self.task_threads = []
        
        self.inited = False
        self.lock = multiprocessing.Lock()
        
    def init(self):
        with self.lock:
            if self.inited: return
            
            self.log_file = os.path.join(self.working_dir, 'job.log')
            self.logger = self.logger or get_logger(name='cola_task',
                                                    filename=self.log_file, 
                                                    server=self.master_ip)
            
            for i in range(self.n_tasks):
                self.counter_clients[i] = CounterClient(self.counter_server,
                                                        app_name=self.job_name)
                self.budget_clients[i] = BudgetApplyClient(self.budget_server,
                                                           app_name=self.job_name)
                self.speed_clients[i] = SpeedControlClient(self.speed_server, self.ip,
                                                           self.task_start_id+i,
                                                           app_name=self.job_name)
            self.init_tasks()
            self._init_counter_sync()
            self._init_idle_status_checker()
            
            self.inited = True
    
    def init_tasks(self):
        self.tasks = []
        for i in range(self.n_tasks):
            task_id = self.task_start_id + i
            task_dir = os.path.join(self.working_dir, str(task_id))
            task = Task(task_dir, self.job_desc, task_id, self.mq, 
                        self.stopped, self.nonsuspend,
                        self.counter_clients[i], 
                        self.budget_clients[i], 
                        self.speed_clients[i],
                        logger=self.logger, env=self.env, 
                        is_local=self.is_local, job_name=self.job_name)
            t = threading.Thread(target=task.run)
            self.tasks.append(task)
            self.task_threads.append(t)
            
    def _init_counter_sync(self):
        def _sync():
            for task in self.tasks:
                task.counter_client.sync()
        
        def sync():
            try:
                while not self.stopped.is_set():
                    _sync()
                    self.stopped.wait(5)
            finally:
                _sync()
        self.sync_t = threading.Thread(target=sync)
        
    def _init_idle_status_checker(self):
        def check():
            idle_times = 0
            while not self.stopped.is_set():
                self.idle_statuses[self.container_id] = \
                    all([task.is_idle() for task in self.tasks])
                if  self.idle_statuses[self.container_id]:
                    idle_times += 1
                    if self.job_desc.settings.job.size=='auto' and idle_times > MAX_IDLE_TIMES:
                        break
                else:
                    idle_times = 0
                self.stopped.wait(5)
        self.check_idle_t = threading.Thread(target=check)
            
    def run(self, block=False):
        self.init()
        
        for task in self.task_threads:
            task.start()
        self.sync_t.start()
        self.check_idle_t.start()
        
        if block:
            self.wait_for_stop()
            
    def wait_for_stop(self):
        if not self.inited: return
        
        for task in self.task_threads:
            try:
                task.join()
            except KeyboardInterrupt:
                continue
        try:
            self.sync_t.join()
        except KeyboardInterrupt:
            pass
        try:
            self.check_idle_t.join()
        except KeyboardInterrupt:
            pass