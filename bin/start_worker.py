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

import subprocess
import os

from cola.core.utils import root_dir, get_ip
from cola.core.config import main_conf

def start_worker(master, data_path=None, force=False):
    path = os.path.join(root_dir(), 'cola', 'worker', 'watcher.py')
    
    print 'Start worker at %s:%s' % (get_ip(), main_conf.worker.port)
    print 'Worker will run in background. Please do not shut down the terminal.'
    
    cmds = ['python', path, '-m', master]
    if data_path is not None:
        cmds.extend(['-d', data_path])
    if force is True:
        cmds.append('-f')
    subprocess.Popen(cmds)
    
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser('Cola worker')
    parser.add_argument('-m', '--master', metavar='master watcher', nargs='?',
                        default=None, const=None,
                        help='master connected to(in the former of `ip:port` or `ip`)')
    parser.add_argument('-d', '--data', metavar='data root directory', nargs='?',
                        default=None, const=None, 
                        help='root directory to put data')
    parser.add_argument('-f', '--force', metavar='force start', nargs='?',
                        default=False, const=True, type=bool)
    args = parser.parse_args()
    
    master = args.master
    if master is None:
        connect_to_localhost = raw_input("Connect to localhost? (yes or no) ")
        conn = connect_to_localhost.lower().strip()
        if conn == 'yes' or conn == 'y':
            master = '%s:%s' % (get_ip(), main_conf.master.port)
        elif conn == 'no' or conn == 'n':
            master = raw_input("Please input the master(form: \"ip:port\" or \"ip\") ")
            if ':' not in master:
                master += ':%s' % main_conf.master.port
        else:
            print 'Input illegal!'
    else:
        if ':' not in master:
            master += ':%s' % main_conf.master.port
    
    if master is not None: 
        start_worker(master, data_path=args.data, force=args.force)