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

Created on 2014-6-8

@author: chine
'''

import os
import threading
import shutil

from cola.core.utils import import_job_desc, Clock
from cola.core.rpc import FileTransportServer
from cola.job import Job

class WorkerJobInfo(object):
    def __init__(self, job_name, working_dir):
        self.job_name = job_name
        self.working_dir = working_dir
        
        self.job = None
        self.thread = None
        self.clock = None

class Worker(object):
    def __init__(self, ctx):
        self.ctx = ctx
        self.working_dir = os.path.join(self.ctx.working_dir, 'worker')
        self.job_dir = os.path.join(self.working_dir, 'jobs')
        self.zip_dir = os.path.join(self.working_dir, 'zip')
        self.running_jobs = {}
        
        self.rpc_server = self.ctx.rpc_server
        assert self.rpc_server is not None
        
        FileTransportServer(self.rpc_server, self.zip_dir)
        
    def _register_rpc(self):
        if self.rpc_server:
            self.rpc_server.register_function(self.prepare, 'prepare')
            self.rpc_server.register_function(self.run_job, 'run_job')
            self.rpc_server.register_function(self.stop_running_job, 
                                              'stop_job')
            self.rpc_server.register_function(self.clear_running_job,
                                              'clear_job')
            self.rpc_server.register_function(self.add_node, 'add_node')
            self.rpc_server.register_function(self.remove_node, 'remove_node')
        
    def prepare(self, job_name, overwrite=False, settings=None):
        src_job_name = job_name
        job_path = os.path.join(self.job_dir, job_name)
        
        if not os.path.exists(job_path):
            return False
        
        job_desc = import_job_desc(job_path)
        if settings is not None:
            job_desc.update(settings)
        
        clear = job_desc.settings.job.clear
        job_name, working_dir = self.ctx._get_name_and_dir(
            self.working_dir, job_name, overwrite=overwrite, clear=clear)
        
        job = Job(self, job_path, job_name=job_name, job_desc=job_desc,
                  working_dir=working_dir, rpc_server=self.rpc_server,
                  manager=self.ctx.manager)
        t = threading.Thread(target=job.run, args=(True, ))
        
        job_info = WorkerJobInfo(job_name, working_dir)
        job_info.job = job
        job_info.thread = t
        self.running_jobs[src_job_name] = job_info
        return True
        
    def run_job(self, job_name):
        if job_name not in self.running_jobs:
            return False
        
        job_info = self.running_jobs[job_name]
        
        clock = Clock()
        job_info.clock = clock
        job_info.thread.start()
        
    def stop_running_job(self, job_name):
        job_info = self.running_jobs.get(job_name)
        if job_info:
            job_info.job.stop_running()
            
    def clear_running_job(self, job_name):
        job_info = self.running_jobs.get(job_name)
        if job_info:
            job_info.job.clear_running()
            job_info.thread.join()
            return job_info.clock.clock()
        
    def add_node(self, worker):
        for job_info in self.running_jobs.values():
            job_info.job.add_node(worker)
            
    def remove_node(self, worker):
        for job_info in self.running_jobs.values():
            job_info.job.remove_node(worker)