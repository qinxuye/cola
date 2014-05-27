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

Created on 2014-2-7

@author: Chine
'''

import os
import tempfile
import multiprocessing
import signal
import threading

from cola.core.config import Config
from cola.core.utils import get_ip, import_job_desc
from cola.job import Job

conf_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'conf')
main_conf = Config(os.path.join(conf_dir, 'main.yaml'))
                
class Context(object):
    def __init__(self, local_mode=False, is_master=False, master=None, 
                 is_client=False, working_dir=None, mkdirs=False, 
                 addr=None, addrs=None):
        self.is_local_mode = local_mode
        self.is_master = is_master
        self.is_client = is_client
        
        self.master = master
        self.master_ip = self.master
        if not self.is_local_mode:
            assert self.master is not None
            if ':' not in self.master:
                self.master = '%s:%s' % (self.master, main_conf.master.port)
            else:
                self.master_ip = self.master.split(':', 1)[0]
        
        self.working_dir = working_dir
        if self.working_dir is None:
            tmp = tempfile.gettempdir()
            self.working_dir = os.path.join(tmp, 'cola')
            if mkdirs and not os.path.exists(self.working_dir):
                os.makedirs(self.working_dir)
                
        self.addr = addr
        if self.addr is None:
            self.addr = get_ip()
        if ':' not in self.addr:
            if is_master:
                port = main_conf.master.port
            elif is_client:
                port = main_conf.client.port
            else:
                port = main_conf.worker.port
            self.addr = '%s:%s' % (self.addr, port)
        self.ip = self.addr.split(':', 1)[0]
            
        fix_addr = lambda addr: addr if ':' in addr \
                    else '%s:%s'%(addr, main_conf.worker.port)
        fix_ip = lambda addr: addr if ':' not in addr \
                    else addr.split(':', 1)[0]
        self.addrs = addrs
        if self.addrs is None:
            self.addrs = [self.addr, ]
        self.ips = [fix_ip(address) for address in self.addrs]
        self.addrs = [fix_addr(address) for address in self.addrs]
            
        self.manager = multiprocessing.Manager()
        self.env = self.manager.dict({'ip': self.ip, 
                                      'root': self.working_dir})
        
    def _run_local_job(self, job_path, overwrite=False):
        job_desc = import_job_desc(job_path)
        base_name = job_desc.uniq_name
        job_name = base_name
        working_dir = os.path.join(self.working_dir, 'worker', job_name)
            
        if overwrite:
            idx = 1
            while os.path.exists(working_dir):
                job_name = '%s%s' % (base_name, idx)
                working_dir = os.path.join(self.working_dir, job_name)
                idx += 1
            
        job = Job(self, job_path, job_name=job_name, job_desc=job_desc,
                      working_dir=working_dir)
        t = threading.Thread(target=job.run, args=(True, ))
        t.setDaemon(True)
        t.start()
        
        def stop(signum, frame):
            job.shutdown()
        signal.signal(signal.SIGINT, stop)
        signal.signal(signal.SIGTERM, stop)
        
        t.join()
        
    def run_job(self, job_path, overwrite=False):
        if self.is_local_mode:
            self._run_local_job(job_path, overwrite=overwrite)