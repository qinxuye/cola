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

Created on 2014-5-13

@author: chine
'''

import os
try:
    import cPickle as pickle
except ImportError:
    import pickle

from cola.job.executor import UrlExecutor, BundleExecutor
from cola.core.utils import Clock

MAX_RUNNING_SECONDS = 10 * 60 # max seconds for a unit in some mq to run
MAX_BUNDLE_RUNNING_SECONDS = 2 * 60  # max seconds for a bundle to run
DEFAULT_URL_APPLY_SIZE = 5

TASK_STATUS_FILENAME = 'task.status'

class Task(object):
    def __init__(self, working_dir, job_desc, task_id, 
                 mq, stopped, nonsuspend, 
                 counter_client, budget_client, speed_client,
                 is_local=False, logger=None, 
                 env=None, job_name=None):
        self.dir_ = working_dir
        self.job_desc = job_desc
        self.settings = job_desc.settings
        self.task_id = task_id
        self.mq = mq
        self.stopped = stopped
        self.nonsuspend = nonsuspend
        
        self.counter_client = counter_client
        self.budget_client = budget_client
        self.speed_client = speed_client
        
        self.is_local = is_local
        self.logger = logger
        
        self.inc = self.settings.job.inc
        self.n_priorities = self.settings.job.priorities
        # the last one is the inc mq if inc=True
        self.full_priorities = self.n_priorities if not self.inc else \
                                self.n_priorities+1
        self.priorities_secs = tuple(
            [MAX_RUNNING_SECONDS/(2**i) for i in range(self.full_priorities)])
        self.priorities_objs = [[]] * self.full_priorities
        
        self.is_bundle = self.settings.job.mode == 'bundle'
        self.budgets = 0
        
        executor_cls = BundleExecutor if self.is_bundle else UrlExecutor
        self.executor = executor_cls(self.task_id, self.job_desc,
            self.mq, self.dir_, self.stopped, self.nonsuspend, 
            self.budget_client, self.speed_client, 
            self.counter_client, is_local=self.is_local, 
            env=env, logger=self.logger)
        
        self.prepare()
        self.load()
        
    def save(self):
        save_file = os.path.join(self.dir_, TASK_STATUS_FILENAME)
        with open(save_file, 'w') as f:
            pickle.dump(self.priorities_objs, f)
            
    def load(self):
        save_file = os.path.join(self.dir_, TASK_STATUS_FILENAME)
        if os.path.exists(save_file):
            with open(save_file) as f:
                self.priorities_objs = pickle.load(f)
            
    def finish(self):
        self.save()
        
    def prepare(self):
        if not os.path.exists(self.dir_):
            os.makedirs(self.dir_)
        
        self.executor.login()
        if self.task_id < len(self.job_desc.starts):
            start = self.job_desc.starts[self.task_id]
            if not self.mq.exist(start):
                self.priorities_objs[0].append(self.job_desc.unit_cls(start))
        
    def _get_unit(self, priority, runnings):
        if len(self.priorities_objs[priority]) > 0:
            runnings.append(self.priorities_objs[priority].pop(0))
        else:
            is_inc = priority == self.n_priorities
            if not is_inc:
                running = self.mq.get(priority=priority)
            else:
                running = self.mq.get_inc(priority=priority)
            if running:
                if isinstance(running, str):
                    running = self.job_desc.unit_cls(running)
                runnings.append(running)
        
    def run(self):
        try:
            curr_priority = 0
            while not self.stopped.is_set():
                while not self.nonsuspend.wait(5):
                    continue
                if self.stopped.is_set():
                    break
                
                last = self.priorities_secs[curr_priority]
                clock = Clock()
                runnings = []
                try:
                    while not self.stopped.is_set():
                        if clock.clock() >= last:
                            break
                        
                        if self.is_bundle:
                            if self.budget_client.apply(1) == 0:
                                if self.stopped.wait(5):
                                    break
                                continue
                        else:
                            if self.budgets == 0:
                                self.budgets = self.budget_client.apply(DEFAULT_URL_APPLY_SIZE)
                            if self.budgets == 0:
                                if self.stopped.wait(5):
                                    return
                                continue
                            else:
                                self.budgets -= 1
                        
                        self._get_unit(curr_priority, runnings)
                        if len(runnings) == 0:
                            break
                        if self.is_bundle:
                            rest = min(last - clock.clock(), MAX_BUNDLE_RUNNING_SECONDS)
                            obj = self.executor.execute(runnings.pop(), rest)
                        else:
                            obj = self.executor.execute(runnings.pop())
                            
                        if obj is not None:
                            runnings.insert(0, obj)           
                finally:
                    self.priorities_objs[curr_priority].extend(runnings)
                    
                curr_priority = (curr_priority+1) % self.full_priorities
        finally:
            self.counter_client.sync()
            self.save()