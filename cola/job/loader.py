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

Created on 2013-6-11

@author: Chine
'''

import threading
import os
import time

from cola.core.rpc import ColaRPCServer

class JobLoader(object):
    
    def __init__(self, job, dir_, local, 
                 context=None, copies=1, force=False):
        self.job = job
        self.ctx = context or job.context
        
        self.root = dir_
        self.host, self.port = tuple(local.split(':', 1))
        self.port = int(self.port)
        self.copies = copies
        self.force = force
        
        # status
        self.stopped = False
        
    def check_env(self, force=False):
        lock_f = os.path.join(self.root, 'lock')
        if os.path.exists(lock_f) and not force:
            return False
        if os.path.exists(lock_f) and force:
            try:
                os.remove(lock_f)
            except:
                return False
            
        open(lock_f, 'w').close()
        return True
        
    def init_rpc_server(self):
        rpc_server = ColaRPCServer((self.host, self.port))
        thd = threading.Thread(target=rpc_server.serve_forever)
        thd.setDaemon(True)
        thd.start()
        self.rpc_server = rpc_server
        
    def finish(self):
        lock_f = os.path.join(self.root, 'lock')
        if os.path.exists(lock_f):
            os.remove(lock_f)
        self.rpc_server.shutdown()
        
    def stop(self):
        self.stopped = True
        self.finish()
        
    def require(self, count):
        raise NotImplementedError
    
    def apply(self):
        raise NotImplementedError
    
    def complete(self, obj):
        raise NotImplementedError
        
class LimitionJobLoader(object):
    def __init__(self, job, context=None):
        self.job = job
        self.ctx = context or job.context
        # status
        self.stopped = False
        
        self.size = self.ctx.job.size
        self.size_limit = self.size > 0
        self.started = 0
        self.completed = 0
        
        self.rate = self.ctx.job.limits
        self.rate_limit = self.rate > 0
        self.current_rate = 0
        
        # locks
        self.op_lock = threading.Lock()
        self.size_lock = threading.Lock()
        self.size_lock_acquire = self.size_lock.acquire
        self.size_lock_release = self._size_lock_release
        
    def init_rate_clear(self):
        if self.rate_limit:
            def _clear():
                self.current_rate = 0
                time.sleep(60)
                if not self.stopped:
                    _clear()
            thd = threading.Thread(target=_clear)
            thd.setDaemon(True)
            thd.start()
            
    def _size_lock_release(self):
        try:
            self.size_lock.release()
        except:
            pass
        
    def finish(self):
        self.size_lock_release()
        
    def stop(self):
        self.stopped = True
        self.finish()
        
    def _apply(self):
        if self.completed >= self.size or \
            self.stopped:
            return False
        
        if self.started >= self.size:
            self.size_lock_acquire()
            return self._apply()
            
        return True
            
    def apply(self):
        if self.completed >= self.size or \
            self.stopped:
            return False
            
        self.op_lock.acquire()
        try:
            if self.started < self.size:
                self.started += 1
                if self.started >= self.size:
                    self.size_lock_acquire()
                return True
        finally:
            self.op_lock.release()
            
        if self.started >= self.size:
            self.size_lock_acquire()
            return self._apply()
            
        return True
    
    def error(self, obj):
        self.started -= 1
        self.size_lock_release()
        
    def complete(self, obj):
        if not self.size_limit: return False
        
        self.completed += 1
        if self.completed >= self.size:
            self.stopped = True
        self.size_lock_release()
        
        return self.completed >= self.size
        
    def require(self, count):
        if not self.rate_limit:
            if not self.stopped:
                return count
            else:
                return 0
        
        size = max(min(self.rate - self.current_rate, count), 0)
        self.current_rate += size
        return size