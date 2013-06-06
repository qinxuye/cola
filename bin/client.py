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

from cola.core.rpc import client_call
from cola.core.utils import get_ip
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
        master = sys.argv[1]
        if ':' not in master:
            master += ':%s' % main_conf.master.port
            
    client = ClientAction(master)
            
    while True:
        cmd = raw_input("Input command(h for help): ").strip()
        if cmd == 'q' or cmd == 'quit':
            break
        else:
            client.action(cmd)