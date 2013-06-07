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

Created on 2013-6-7

@author: Chine
'''

import socket
import os
import tempfile
import shutil

from cola.core.rpc import client_call, FileTransportClient
from cola.core.zip import ZipHandler
from cola.core.utils import get_ip, import_job
from cola.job.conf import main_conf

class ClientAction(object):
    def __init__(self, master):
        self.master = master
        
    def action(self, name):
        if name == 'stop all':
            print 'Trying to stop master and all workers.'
            try:
                client_call(self.master, 'stop')
            except socket.error:
                print 'Cannot connect to cola master.'
            else:
                print 'Cola cluster has been shutdown.'
        elif name == 'list jobs':
            print 'Running jobs: '
            for job in client_call(self.master, 'list_jobs'):
                print job
        elif name == 'list workers':
            print 'Cola workers: '
            for worker in client_call(self.master, 'list_workers'):
                print worker
        elif name == 'list job dirs':
            print 'Runnable job dirs: '
            for dir_ in client_call(self.master, 'list_job_dirs'):
                print dir_
        elif name.startswith('run remote job '):
            print 'Remote job will run in background.'
            
            job_dir = name[len('run remote job '):]
            if job_dir not in client_call(self.master, 'list_job_dirs'):
                print 'Remote job dir not exists!'
            else:
                client_call(self.master, 'start_job', job_dir, False)
        elif name.startswith('run local job '):
            print 'Job has been committed and will run in background.'
            
            start = len('run local job ')
            path = name[start:].strip().strip('"').strip("'")
            if not os.path.exists(path):
                print 'Job path not exists!'
            else:
                try:
                    job = import_job(path)
                except (ImportError, AttributeError):
                    print 'Job path is illegal!'
                    
                dir_ = tempfile.mkdtemp()
                try:
                    zip_filename = os.path.split(path)[1].replace(' ', '_') + '.zip'
                    zip_file = os.path.join(dir_, zip_filename)
                    
                    ZipHandler.compress(zip_file, path, type_filters=("pyc", ))
                    FileTransportClient(self.master, zip_file).send_file()
                    
                    client_call(self.master, 'start_job', zip_filename)
                finally:
                    shutil.rmtree(dir_)
            
if __name__ == "__main__":
    import sys
    
    master = None
    if len(sys.argv) == 1:
        connect_to_localhost = raw_input("Connect to localhost? (yes or no) ")
        conn = connect_to_localhost.lower().strip()
        if conn == 'yes' or conn == 'y':
            master = '%s:%s' % (get_ip(), main_conf.master.port)
        elif conn == 'no' or conn == 'n':
            master = raw_input("Please input the master(form: \"ip:port\" or \"ip\"): ")
            if ':' not in master:
                master += ':%s' % main_conf.master.port
        else:
            print 'Input illegal!'
    else:
        master = sys.argv[1]
        if ':' not in master:
            master += ':%s' % main_conf.master.port
            
    client = ClientAction(master)
            
    while master is not None:
        cmd = raw_input("> Input command(h for help): ").strip()
        if cmd == 'q' or cmd == 'quit':
            break
        else:
            client.action(cmd)