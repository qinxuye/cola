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

import multiprocessing
import threading

class MpFunctionServer(object):
    def __init__(self, instances, stopped):
        self.stopped = stopped
        self.clients = []
        self.threads = []
        for _ in range(instances):
            agent, client = multiprocessing.Pipe()
            self.clients.append(client)
            t = threading.Thread(target=self._init_agent, args=(agent, ))
            t.setDaemon(True)
            self.threads.append(t)
            t.start()
            
    def join(self):
        for t in self.threads:
            t.join()
        
    def _init_agent(self, agent):
        try:
            while not self.stopped.is_set():
                need_process = agent.poll(10)
                if not need_process:
                    continue
                
                func, args = agent.recv()
                agent.send(getattr(self, func)(*args))
        finally:
            agent.close()
            
    def get_pipe(self, instance_id):
        return self.clients[instance_id]