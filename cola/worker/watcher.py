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

Created on 2013-6-5

@author: Chine
'''

import threading
import time
import os
import subprocess
import shutil

from cola.core.rpc import ColaRPCServer, client_call, FileTransportServer
from cola.core.utils import get_ip, import_job, root_dir
from cola.core.zip import ZipHandler
from cola.core.config import main_conf

TIME_SLEEP = 10

class WorkerWatcherRunning(Exception): pass

class WorkerJobInfo(object):
    def __init__(self, port, popen):
        self.node = '%s:%s' % (get_ip(), port)
        self.popen = popen

class WorkerWatcher(object):
    def __init__(self, master, root, zip_dir, job_dir, force=False):
        self.master = master
        self.host = get_ip()
        self.port = main_conf.worker.port
        self.node = '%s:%s' % (self.host, self.port)
        
        self.root = root
        self.zip_dir = zip_dir
        self.job_dir = job_dir
        self.force = force
        
        self.stopped = False
        
        self.running_jobs = {}
        
        self.check(force=force)
        self.init_rpc_server()
        
        self.rpc_server.register_function(self.stop, 'stop')
        self.rpc_server.register_function(self.start_job, 'start_job')
        self.rpc_server.register_function(self.clear_job, 'clear_job')
        self.set_file_receiver(self.zip_dir)
        
    def init_rpc_server(self):
        rpc_server = ColaRPCServer((self.host, self.port))
        thd = threading.Thread(target=rpc_server.serve_forever)
        thd.setDaemon(True)
        thd.start()
        self.rpc_server = rpc_server
        
    def check(self, force=False):
        if not self.check_env(force=force):
            raise WorkerWatcherRunning('There has been a running master watcher.')
        
    def check_env(self, force=False):
        lock_f = os.path.join(self.root, 'lock')
        if os.path.exists(lock_f) and not force:
            return False
        if os.path.exists(lock_f) and force:
            try:
                os.remove(lock_f)
            except:
                return False
            
        open(lock_f, 'w').close()
        return True
    
    def finish(self):
        lock_f = os.path.join(self.root, 'lock')
        if os.path.exists(lock_f):
            os.remove(lock_f)
        self.rpc_server.shutdown()
        self.stopped = True
        
    def set_file_receiver(self, base_dir):
        serv = FileTransportServer(self.rpc_server, base_dir)
        return serv
    
    def register_heartbeat(self):
        client_call(self.master, 'register_heartbeat', self.node)
        
    def start_job(self, zip_filename, uncompress=True):
        if uncompress:
            zip_file = os.path.join(self.zip_dir, zip_filename)
            job_dir = ZipHandler.uncompress(zip_file, self.job_dir)
        else:
            job_dir = os.path.join(self.job_dir, zip_filename.rsplit('.', 1)[0])
            
        job = import_job(job_dir)
        
        master_port = job.context.job.master_port
        master = '%s:%s' % (self.master.split(':')[0], master_port)
        dirname = os.path.dirname(os.path.abspath(__file__))
        f = os.path.join(dirname, 'loader.py')
        
        popen = subprocess.Popen('python "%s" "%s" %s' % (f, job_dir, master))
        self.running_jobs[job.real_name] = WorkerJobInfo(job.context.job.port, popen)
    
    def clear_job(self, job_name):
        job_name = job_name.replace(' ', '_')
        shutil.rmtree(os.path.join(self.job_dir, job_name))
        
    def kill(self, job_name):
        if job_name in self.running_jobs:
            self.running_jobs[job_name].popen.kill()
        
    def run(self):
        def _start():
            while not self.stopped:
                self.register_heartbeat()
                time.sleep(TIME_SLEEP)
        
        thread = threading.Thread(target=_start)
        thread.setDaemon(True)
        thread.start()
        thread.join()
        
    def stop(self):
        self.finish()
        
    def __enter__(self):
        return self
    
    def __exit__(self, type_, value, traceback):
        self.finish()
        
def makedirs(dir_):
    if not os.path.exists(dir_):
        os.makedirs(dir_)

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        raise ValueError('Worker watcher need at least 1 parameters.')
    master = sys.argv[1]
    if ':' not in master:
        master = '%s:%s' % (master, main_conf.master.port)
    
    data_path = os.path.join(root_dir(), 'data')
    root = os.path.join(data_path, 'worker', 'watcher')
    zip_dir = os.path.join(data_path, 'zip')
    job_dir = os.path.join(data_path, 'jobs')
    for dir_ in (root, zip_dir, job_dir):
        makedirs(dir_)
    
    with WorkerWatcher(master, root, zip_dir, job_dir) \
        as master_watcher:
        master_watcher.run()