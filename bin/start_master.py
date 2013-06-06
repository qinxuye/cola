#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2013-6-6

@author: Chine
'''

import subprocess
import os

from cola.core.utils import root_dir, get_ip
from cola.job.conf import main_conf

def start_master():
    path = os.path.join(root_dir(), 'cola', 'master', 'watcher.py')
    
    print 'Start master at %s:%s' % (get_ip(), main_conf.master.port)
    print 'Master will run in background even close the terminal.'
    
    subprocess.Popen(['python', path])

if __name__ == "__main__":
    start_master()