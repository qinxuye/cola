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

Created on 2013-6-27

@author: Chine
'''

import os

from cola.core.mq.client import MessageQueueClient
from cola.core.rpc import client_call
from cola.core.utils import get_ip

from conf import user_config

PUTSIZE = 50
keywords_f = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'keywords.txt')

def put_starts(master=None):
    if master is None:
        nodes = ['%s:%s' % (get_ip(), getattr(user_config.job, 'port'))]
    else:
        nodes = client_call(master, 'get_nodes')
        
    mq_client = MessageQueueClient(nodes)
    with open(keywords_f) as f:
        keys = []
        size = 0
        for keyword in f.xreadlines():
            keys.append(keyword)
            size += 1
            if size >= PUTSIZE:
                mq_client.put(keys)
                size = 0
                keys = []
        if len(keys) > 0:
            mq_client.put(keys)
            
def main(master=None):
    if master is not None:
        if ':' not in master:
            master = '%s:%s' % (master, getattr(user_config.job, 'master_port'))
    put_starts(master)
            
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser('Weibo search')
    parser.add_argument('-m', '--master', metavar='master ip', nargs='?',
                        default=None, const=None,
                        help='master ip connected to')
    args = parser.parse_args()
    
    main(args.master)