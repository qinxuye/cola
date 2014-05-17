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

import time

from cola.job.executor import UrlExecutor, BundleExecutor
from cola.core.errors import ConfigurationError

MAX_RUNNING_SECONDS = 10 * 60 # max seconds for a unit in some mq to run
MAX_BUNDLE_RUNNING_SECONDS = 5 * 60  # max seconds for a bundle to run

class Task(object):
    def __init__(self, working_dir, job_desc, task_id, 
                 mq, stopped, nonsuspend, 
                 counter_client, budget_client, speed_client,
                 is_local=False, logger=None, job_name=None):
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
        
        self.prepare()
        
    def prepare(self):
        self.opener = self.job_desc.opener_cls()
        if self.is_local:
            if not self.login():
                raise
        else:
            while not self.stopped.is_set():
                if self.login():
                    break
                if self.stopped.wait(5):
                    break
        if self.task_id < len(self.job_desc.starts):
            start = self.job_desc.starts[self.task_id]
            if not self.mq.verify(start):
                self.priorities_objs[0].append(start)
        
    def login(self):
        if self.job_desc.login_hook is not None:
            if 'login' not in self.settings.job or \
                not isinstance(self.settings.job.login, list):
                raise ConfigurationError('If login_hook set, config files must contains `login`')
            
            kws = self.settings.job.login
            idx = self.task_id % len(kws)
            kws = kws[idx:] + kws[:idx]
            
            for kw in kws:
                login_result = self.job.login_hook(self.opener, **kw)
                if isinstance(login_result, tuple) and len(login_result) == 2 and \
                    not login_result[0]:
                    if self.logger:
                        self.logger.error('instance %s, login fail, reason: %s' % \
                                          (self.task_id, login_result[1]))
                    continue
                elif not login_result:
                    if self.logger:
                        self.logger.error('instance %s: login fail' % self.task_id)
                    continue
                return login_result
            return False
        
        return True
    
    def clear_and_relogin(self):
        self.opener = self.job_desc.opener_cls()
        self.login()
        
    def _get_unit(self, priority, runnings):
        if len(self.priorities_objs[priority]) > 0:
            runnings.append(self.priorities_objs[priority].pop(0))
        else:
            is_inc = priority == self.n_priorities
            running = self.mq.get(priority=priority, inc=is_inc)
            if running:
                runnings.append(running)
        
    def run(self):
        curr_priority = 0
        while not self.stopped.is_set():
            while not self.nonsuspend.wait(5):
                continue
            if self.stopped.is_set():
                break
            
            last = self.priorities_secs[curr_priority]
            start = time.time()
            runnings = []
            try:
                while not self.stopped.is_set():
                    curr = time.time()
                    if curr - start >= last:
                        break
                    self._get_unit(curr_priority, runnings)
                    if len(runnings) == 0:
                        break
                    if self.is_bundle:
                        rest = min(last - (curr - start), MAX_BUNDLE_RUNNING_SECONDS)
                        executor = BundleExecutor(runnings.pop(), self.stopped, 
                                                  self.nonsuspend, self.counter_client,
                                                  rest)
                    else:
                        executor = UrlExecutor(runnings.pop(), self.stopped,
                                               self.nonsuspend, self.counter_client)
                    obj = executor.execute()
                    if obj is not None:
                        runnings.insert(0, obj)           
            finally:
                self.priorities_objs[curr_priority].extend(runnings)
                
            curr_priority = (curr_priority+1) % self.full_priorities