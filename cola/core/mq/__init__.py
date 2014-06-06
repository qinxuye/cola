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

Created on 2013-5-23

@author: Chine
'''

from cola.core.mq.node import MessageQueueNodeProxy
from cola.core.mq.client import MessageQueueClient

PUT, PUT_INC, GET, GET_INC, EXIST = range(5)

MessageQueueClient = MessageQueueClient

class MessageQueue(MessageQueueNodeProxy):
    def __init__(self, working_dir, rpc_server, addr, addrs, 
                 app_name=None, copies=1, n_priorities=3,
                 deduper=None):
        super(MessageQueue, self).__init__(working_dir, rpc_server, addr, addrs,
                                           copies=copies, n_priorities=n_priorities,
                                           deduper=deduper, app_name=app_name)