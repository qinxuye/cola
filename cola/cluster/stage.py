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

Created on 2014-5-10

@author: chine
'''

import threading

from cola.core.rpc import client_call
from cola.core.utils import get_rpc_prefix

class Stage(object):
    '''
    Used for master to control the workers and will not move to the next stage
    until this one has been finished
    '''
    def __init__(self, workers, func, prefix=None, app_name=None, logger=None):
        self.workers = workers
        self.func = func
        self.prefix = get_rpc_prefix(app_name, prefix)
        self.remote_func = self.prefix + func
        self.logger = logger
        
    def barrier(self, parallel, *args):
        results = [False] * len(self.workers)

        def _call(i, worker):
            try:
                result = client_call(worker, self.remote_func, *args)
            except Exception, e:
                if self.logger:
                    self.logger.error(e)
                result = False
            if isinstance(result, bool):
                results[i] = result
            else:
                results[i] = True
        
        if not parallel:
            for i, worker in enumerate(self.workers):
                _call(i, worker)
        else:
            threads = []
            for i, worker in enumerate(self.workers):
                t = threading.Thread(target=_call, args=(i, worker, ))
                t.setDaemon(True)
                threads.append(t)
            for t in threads:
                t.start()               
            for t in threads:
                t.join()

        return all(results)
