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

Created on 2014-5-12

@author: chine
'''

import re
import hashlib
import os
import multiprocessing

from cola.core.errors import ConfigurationError
from cola.core.utils import base58_encode, get_cpu_count, \
                            import_job_desc
from cola.core.mq import MessageQueue, MpMessageQueue, \
                            MpMessageQueueClient
from cola.core.dedup import FileBloomFilterDeduper
from cola.core.unit import Bundle, Url
from cola.core.logs import get_logger
from cola.settings import Settings
from cola.functions.budget import BudgetApplyServer, \
                                    MpBudgetApplyServer
from cola.functions.speed import SpeedControlServer, \
                                    MpSpeedControlServer
from cola.functions.counter import CounterServer, \
                                    MpCounterServer
from cola.job.container import Container

JOB_NAME_RE = re.compile(r'(\w| )+')
UNLIMIT_BLOOM_FILTER_CAPACITY = 1000000

class JobRunning(Exception): pass

class JobDescription(object):
    def __init__(self, name, url_patterns, opener_cls, user_conf, starts, 
                 unit_cls=None, login_hook=None, **kw):
        self.name = name
        if not JOB_NAME_RE.match(name):
            raise ConfigurationError('Job name can only contain alphabet, number and space.')
        self.uniq_name = self._get_uniq_name(self.name)
        
        self.url_patterns = url_patterns
        self.opener_cls = opener_cls
        
        self.user_conf = user_conf
        self.starts = starts
        self.login_hook = login_hook
        
        self.settings = Settings(user_conf=user_conf, **kw)
        self.unit_cls = unit_cls or \
            (Bundle if self.settings.job.mode == 'bundle' else Url)
        
    def _get_uniq_name(self, name):
        hash_val = hashlib.md5(name).hexdigest()[8:-8]
        return base58_encode(int(hash_val, 16))
        
    def add_urlpattern(self, url_pattern):
        self.url_patterns += url_pattern
        
class Job(object):
    def __init__(self, ctx, job_def_path, job_name=None, 
                 job_desc=None, working_dir=None, rpc_server=None):
        self.ctx = ctx
        self.job_def_path = job_def_path
        self.rpc_server = rpc_server
        self.job_name = job_name or self.job_desc.uniq_name
        self.working_dir = working_dir or \
            os.path.join(self.ctx.working_dir, self.job_name)
            
        if not os.path.exists(self.working_dir):
            os.makedirs(self.working_dir)
            
        self.logger = get_logger(name='cola_job')
        
        self.stopped = multiprocessing.Event()
        self.nonsuspend = multiprocessing.Event()
        self.nonsuspend.set()
        
        self.job_desc = job_desc or import_job_desc(job_def_path)
        self.settings = self.job_desc.settings
        self.is_bundle = self.settings.job.mode == 'bundle'
        
        self.n_instances = self.job_desc.settings.job.instances
        self.n_containers = min(get_cpu_count(), max(self.n_instances, 1))
        self.is_multi_process = self.n_containers > 1
        self.containers = []
        
        self.shutdown_callbacks = []
        self.inited = False
        
    def init_deduper(self):
        base = 1 if not self.is_bundle else 1000
        size = self.job_desc.settings.job.size
        capacity = UNLIMIT_BLOOM_FILTER_CAPACITY
        if size > 0:
            capacity = max(base * size * 10, capacity)
        deduper_path = os.path.join(self.working_dir, 'dedup')
        self.deduper = FileBloomFilterDeduper(deduper_path, capacity)
        # register shutdown callback
        def shutdown():
            self.logger.debug('shutdown deduper')
            self.deduper.shutdown()
        self.shutdown_callbacks.append(shutdown)
        
    def init_mq(self):
        mq_dir = os.path.join(self.working_dir, 'mq')
        copies = self.job_desc.settings.job.copies
        n_priorities = self.job_desc.settings.job.priorities
        
        kw = {'app_name': self.job_name, 'copies': copies, 
              'n_priorities': n_priorities, 'deduper': self.deduper}
        if not self.is_multi_process:
            self.mq = MessageQueue(mq_dir, self.rpc_server, self.ctx.addr, 
                                   self.ctx.addrs, **kw)
        else:
            kw['instances'] = self.n_containers
            self.mq = MpMessageQueue(mq_dir, self.rpc_server, self.ctx.addr,
                                     self.ctx.addrs, **kw)
        # register shutdown callback
        def shutdown():
            self.logger.debug('shutdown mq')
            self.mq.shutdown()
        self.shutdown_callbacks.append(shutdown)
        
    def _init_function_servers(self):
        budget_dir = os.path.join(self.working_dir, 'budget')
        if not self.is_multi_process:
            self.budget_server = BudgetApplyServer(budget_dir, self.settings, 
                                                   self.rpc_server, self.job_name)
        else:
            self.budget_server = MpBudgetApplyServer(budget_dir, self.settings,
                                                     self.n_instances, self.stopped,
                                                     self.rpc_server, self.job_name)
        def budget_shutdown():
            self.logger.debug('shutdown budget server')
            self.budget_server.shutdown()
        self.shutdown_callbacks.append(budget_shutdown)
        
        counter_dir = os.path.join(self.working_dir, 'counter')
        if not self.is_multi_process:
            self.counter_server = CounterServer(counter_dir, self.settings,
                                                self.rpc_server, self.job_name)
        else:
            self.counter_server = MpCounterServer(counter_dir, self.settings,
                                                  self.n_containers, self.stopped,
                                                  self.rpc_server, self.job_name)
        def counter_shutdown():
            self.logger.debug('shutdown counter server')
            self.counter_server.shutdown()
        self.shutdown_callbacks.append(counter_shutdown)
        
        speed_dir = os.path.join(self.working_dir, 'speed')
        if not self.is_multi_process:
            self.speed_server = SpeedControlServer(speed_dir, self.settings,
                                                   self.rpc_server, self.job_name,
                                                   self.counter_server, self.ctx.ips)
        else:
            self.speed_server = MpSpeedControlServer(speed_dir, self.settings,
                                                     self.n_instances, self.stopped,
                                                     self.rpc_server, self.job_name,
                                                     self.counter_server, self.ctx.ips)
        def speed_shutdown():
            self.logger.debug('shutdown speed server')
            self.speed_server.shutdown()
        self.shutdown_callbacks.append(speed_shutdown)
        
    def init_functions(self):
        if self.ctx.is_local_mode:
            self._init_function_servers()
            if self.is_multi_process:
                self.counter_args = [self.counter_server.get_pipe(i) \
                                     for i in range(self.n_instances)]
                self.budget_args = [self.budget_server.get_pipe(i) \
                                    for i in range(self.n_instances)]
                self.speed_args = [self.speed_server.get_pipe(i) \
                                   for i in range(self.n_instances)]
            else:
                self.counter_args = [self.counter_server \
                                     for _ in range(self.n_instances)]
                self.budget_args = [self.budget_server \
                                    for _ in range(self.n_instances)]
                self.speed_args = [self.speed_server \
                                   for _ in range(self.n_instances)]
        else:
            self.counter_args, self.budget_args, self.speed_args = \
                [self.ctx.master for _ in range(self.n_instances)] 
            
    def init_containers(self):
        acc = 0
        for container_id in range(self.n_containers):
            if self.is_multi_process:
                mq_client = MpMessageQueueClient(container_id, self.mq.kw)
            else:
                mq_client = self.mq
            
            n_tasks = self.n_instances / self.n_containers
            if container_id < self.n_instances % self.n_containers:
                n_tasks += 1
            container = Container(container_id, self.working_dir, 
                                  mq_client, self.job_def_path, 
                                  self.ctx.env, self.job_name, 
                                  self.counter_args[acc: acc+n_tasks], 
                                  self.budget_args[acc: acc+n_tasks],
                                  self.speed_args[acc: acc+n_tasks],
                                  self.stopped, self.nonsuspend, 
                                  n_tasks=n_tasks,
                                  is_local=self.ctx.is_local_mode,
                                  master_ip=self.ctx.master_ip,
                                  task_start_id=acc)
            self.containers.append(container)
            acc += n_tasks
        
    def init(self):
        if self.inited:
            return
        
        self.lock_file = os.path.join(self.working_dir, 'lock')
        
        if os.path.exists(self.lock_file):
            raise JobRunning('The job has already started')
        open(self.lock_file, 'w').close()
        
        self.init_deduper()
        self.init_mq()
        self.init_functions()
        self.init_containers()
        
        self.inited = True
        
    def run(self, block=False):
        self.init()
        if self.is_multi_process:
            processes = []
            for container in self.containers:
                process = multiprocessing.Process(target=container.run, 
                                                  args=(block, ))
                process.start()
                processes.append(process)
            for process in processes:
                process.join()
        else:
            for container in self.containers:
                container.run()
            if block:
                self.wait_for_stop()
            
    def wait_for_stop(self):
        [container.wait_for_stop() for container in self.containers]
        
    def shutdown(self):
        try:
            self.stopped.set()
            self.wait_for_stop()
            for cb in self.shutdown_callbacks:
                cb()
            self.logger.debug('shutdown finished')
        finally:
            if os.path.exists(self.lock_file):
                os.remove(self.lock_file)
            
    def suspend(self):
        self.nonsuspend.clear()
        
    def resume(self):
        self.nonsuspend.set()