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
NO_BUDGETS_RETRY_TIMES = 5
DEFAULT_URL_APPLY_SIZE = 5

TASK_STATUS_FILENAME = 'task.status'

APPLY_SUCCESS, CANNOT_APPLY, APPLY_FAIL = range(3)

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
        self.priorities_objs = [[] for _ in range(self.full_priorities)]
        
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
        starts = []
        i = self.task_id
        while i < len(self.job_desc.starts):
            starts.append(self.job_desc.starts[i])
            i += self.settings.job.instances

        for start in starts:
            if not self.mq.exist(start):
                if not isinstance(start, self.job_desc.unit_cls):
                    start = self.job_desc.unit_cls(start)
                self.priorities_objs[0].append(start)
        
        if self.is_local:
            size = self.job_desc.settings.job.size
            if size <= 0:
                return
            
            for obj in self.priorities_objs[0][size:]:
                self.mq.put(obj)
            self.priorities_objs[0] = self.priorities_objs[0][:size]
        
    def _get_unit(self, priority, runnings):
        if len(self.priorities_objs[priority]) > 0:
            runnings.append(self.priorities_objs[priority].pop(0))
        else:
            is_inc = priority == self.n_priorities
            if not is_inc:
                running = self.mq.get(priority=priority)
            else:
                running = self.mq.get_inc()
            if running:
                if isinstance(running, str):
                    running = self.job_desc.unit_cls(running)
                runnings.append(running)
                
    def _has_not_finished(self, priority):
        return len(self.priorities_objs[priority]) > 0
        
    def _exceed_no_budgets_retry_times(self, retry_times):
        return retry_times > NO_BUDGETS_RETRY_TIMES
    
    def _apply(self, no_budgets_times):
        if self.is_bundle:
            if self.budget_client.apply(1) == 0:
                self.logger.debug('no budget left to process')
                no_budgets_times += 1
                if self._exceed_no_budgets_retry_times(no_budgets_times) or \
                    self.stopped.wait(5):
                    return CANNOT_APPLY
                return APPLY_FAIL
        else:
            if self.budgets == 0:
                self.budgets = self.budget_client.apply(DEFAULT_URL_APPLY_SIZE)
            if self.budgets == 0:
                self.logger.debug('no budget left to process')
                no_budgets_times += 1
                if self._exceed_no_budgets_retry_times(no_budgets_times) or \
                    self.stopped.wait(5):
                    return CANNOT_APPLY
                return APPLY_FAIL
            else:
                self.budgets -= 1
                
        return APPLY_SUCCESS
        
    def run(self):
        try:
            curr_priority = 0
            while not self.stopped.is_set():
                priority_name = 'inc' if curr_priority == self.n_priorities \
                                    else curr_priority
                is_inc = priority_name == 'inc'
                
                while not self.nonsuspend.wait(5):
                    continue
                if self.stopped.is_set():
                    break
                
                self.logger.debug('start to process priority: %s' % priority_name)
                
                last = self.priorities_secs[curr_priority]
                clock = Clock()
                runnings = []
                try:
                    no_budgets_times = 0
                    while not self.stopped.is_set():
                        if clock.clock() >= last:
                            break
                        
                        status = self._apply(no_budgets_times)
                        if status == CANNOT_APPLY:
                            break
                        elif status == APPLY_FAIL:
                            no_budgets_times += 1
                            if not self._has_not_finished(curr_priority) and \
                                len(runnings) == 0:
                                continue
                            
                            if len(runnings) == 0:
                                self._get_unit(curr_priority, runnings)
                        else:
                            no_budgets_times = 0
                            self._get_unit(curr_priority, runnings)
                            
                        if len(runnings) == 0:
                            break
                        if self.is_bundle:
                            self.logger.debug(
                                'process bundle from priority %s' % priority_name)
                            rest = min(last - clock.clock(), MAX_BUNDLE_RUNNING_SECONDS)
                            if rest <= 0:
                                break
                            obj = self.executor.execute(runnings.pop(), rest, is_inc=is_inc)
                        else:
                            obj = self.executor.execute(runnings.pop(), is_inc=is_inc)
                            
                        if obj is not None:
                            runnings.insert(0, obj)  
                finally:
                    self.priorities_objs[curr_priority].extend(runnings)
                    
                curr_priority = (curr_priority+1) % self.full_priorities
        finally:
            self.counter_client.sync()
            self.save()