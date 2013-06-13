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

Created on 2013-6-6

@author: Chine
'''

import subprocess
import os

from cola.core.utils import root_dir, get_ip
from cola.core.config import main_conf

def start_master(data_path=None, force=False):
    path = os.path.join(root_dir(), 'cola', 'master', 'watcher.py')
    
    print 'Start master at %s:%s' % (get_ip(), main_conf.master.port)
    print 'Master will run in background. Please do not shut down the terminal.'
    
    cmds = ['python', path]
    if data_path is not None:
        cmds.extend(['-d', data_path])
    if force is True:
        cmds.append('-f')
    subprocess.Popen(cmds)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser('Cola master')
    parser.add_argument('-d', '--data', metavar='data root directory', nargs='?',
                        default=None, const=None, 
                        help='root directory to put data')
    parser.add_argument('-f', '--force', metavar='force start', nargs='?',
                        default=False, const=True, type=bool)
    args = parser.parse_args()
    
    start_master(args.data, args.force)