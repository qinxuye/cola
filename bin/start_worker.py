#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2013-6-7

@author: Chine
'''

import subprocess
import os

from cola.core.utils import root_dir, get_ip
from cola.job.conf import main_conf

def start_worker(master):
    path = os.path.join(root_dir(), 'cola', 'worker', 'watcher.py')
    
    print 'Start worker at %s:%s' % (get_ip(), main_conf.worker.port)
    print 'Worker will run in background even close the terminal.'
    
    subprocess.Popen(['python', path, master])
    
if __name__ == "__main__":
    import sys
    
    master = None
    if len(sys.argv) == 1:
        connect_to_localhost = raw_input("Connect to localhost? (yes or no) ")
        conn = connect_to_localhost.lower().strip()
        if conn == 'yes' or conn == 'y':
            master = '%s:%s' % (get_ip(), main_conf.master.port)
        elif conn == 'no' or conn == 'n':
            master = raw_input("Please input the master(form: \"ip:port\" or \"ip\") ")
            if ':' not in master:
                master += ':%s' % main_conf.master.port
    else:
        master = sys.argv[1]
        if ':' not in master:
            master += ':%s' % main_conf.master.port
            
    start_worker(master)