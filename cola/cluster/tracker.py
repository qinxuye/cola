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

Created on 2014-6-14

@author: chine
'''

import time
import threading

RUNNING, HANGUP, STOPPED = range(3)
class WorkerInfo(object):
    def __init__(self, worker):
        self.worker = worker
        self.continous_register = 1
        self.last_update = int(time.time())
        self.status = RUNNING
        
    def update(self):
        self.continous_register += 1
        self.last_update = int(time.time())

class WorkerTracker(object):
    def __init__(self):
        self.workers = {}
        self.black_list = []
        
        self.stopped = threading.Event()
        
    def register_worker(self, worker):
        if worker not in self.workers:
            self.workers[worker] = WorkerInfo(worker)
        else:
            self.workers[worker].update()

class JobTracker(object):
    def __init__(self):
        self.running_jobs = {}
        
    def register_job(self, job_name, job_master):
        self.running_jobs[job_name] = job_master
        
    def get_job_master(self, job_name):
        return self.running_jobs[job_name]
        
    def remove_worker(self, job_name, worker):
        if job_name in self.running_jobs:
            self.running_jobs[job_name].remove_worker(worker)
        
    def add_worker(self, job_name, worker):
        if job_name in self.running_jobs:
            self.running_jobs[job_name].add_worker(worker)
            
    def has_worker(self, job_name, worker):
        if job_name in self.running_jobs:
            self.running_jobs[job_name].has_worker(worker)
        return False