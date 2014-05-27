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
import threading
from multiprocessing import Process

from cola.core.utils import import_job_desc, get_ip
from cola.core.logs import get_logger
from cola.job.task import Task
from cola.functions.budget import BudgetApplyClient
from cola.functions.speed import SpeedControlClient
from cola.functions.counter import CounterClient

class Container(object):
    def __init__(self, container_id, working_dir, mq,
                 job_path, env, job_name,
                 stopped, nonsuspend, n_tasks=1, 
                 is_local=False, master=None, logger=None,
                 task_start_id=0):
        self.container_id = container_id
        self.working_dir = working_dir
        self.mq = mq
        self.job_desc = import_job_desc(job_path)
        self.env = env
        self.job_name = job_name
        self.stopped = stopped
        self.nonsuspend = nonsuspend
        self.n_tasks = n_tasks
        self.is_local = is_local
        
        self.task_start_id = task_start_id
        self.ip = self.env.get('ip', None) or get_ip()
            
        self.log_file = os.path.join(working_dir, 'job.log')
        self.logger = logger or get_logger(filename=self.log_file, 
                                           server=master)
        
        self.counter_clients = [None for _ in range(self.n_tasks)]
        self.budget_clients = [None for _ in range(self.n_tasks)]
        self.speed_clients = [None for _ in range(self.n_tasks)]
        
        self.task_threads = []
        
    def init(self, counters, budgets, speeds):
        for i in range(self.n_tasks):
            self.counter_clients[i] = CounterClient(counters[i],
                                                    app_name=self.job_name)
            self.budget_clients[i] = BudgetApplyClient(budgets[i],
                                                       app_name=self.job_name)
            self.speed_clients[i] = SpeedControlClient(speeds[i], self.ip,
                                                       self.task_start_id+i,
                                                       app_name=self.job_name)
        self.init_tasks()
        self._init_counter_sync()
    
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
            self.task_threads.append(t)
            
    def _init_counter_sync(self):
        def sync():
            try:
                while not self.stopped.is_set():
                    for task in self.tasks:
                        task.counter_client.sync()
                    self.stopped.wait(5)
            finally:
                sync()
        self.sync_t = t = threading.Thread(target=sync)
        t.setDaemon(True)
        t.start()
            
    def run(self):
        for task in self.task_threads:
            task.start()
            
    def wait_for_stop(self):
        for task in self.task_threads:
            task.join()
        self.sync_t.join()
            
class MultiProcessContainer(Process, Container):
    def __init__(self, *args, **kwargs):
        Container.__init__(self, *args, **kwargs)
        Process.__init__(self)