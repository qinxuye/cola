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

@author: chine
'''

import os
import threading
try:
    import cPickle as pickle
except ImportError:
    import pickle

from cola.core.utils import get_rpc_prefix
from cola.core.rpc import client_call

FUNC_PREFIX = "budget_apply_"

SUFFICIENT, NOAPPLIED, ALLFINISHED = range(3)
DEFAULT_BUDGETS = 3
BUDGET_APPLY_STATUS_FILENAME = 'budget.apply.status'

def synchronized(func):
    def inner(self, *args, **kw):
        with self.lock:
            return func(self, *args, **kw)
    return inner

class BudgetApplyServer(object):
    
    def __init__(self, working_dir, settings, 
                 rpc_server=None, app_name=None):
        self.dir_ = working_dir
        self.settings = settings
        self.is_local = rpc_server is None
        self.rpc_server = rpc_server
        self.app_name = app_name
        self.prefix = get_rpc_prefix(self.app_name, FUNC_PREFIX)
        
        self.budgets = settings.job.size
        self.limit = self.budgets >= 0
        self.applied = 0
        self.finished = 0
        
        self.lock = threading.Lock()
        
        self.load()
        self.set_status()
        self._register_rpc()
        
    def _register_rpc(self):
        if self.rpc_server is not None:
            self.rpc_server.register_function(self.set_budgets, 
                                              name='set_budgets', prefix=self.prefix)
            self.rpc_server.register_function(self.inc_budgets, 
                                              name='inc_budgets', prefix=self.prefix)
            self.rpc_server.register_function(self.dec_budgets, 
                                              name='dec_budgets', prefix=self.prefix)
            self.rpc_server.register_function(self.apply, 
                                              name='apply', prefix=self.prefix)
            self.rpc_server.register_function(self.finish, 
                                              name='finish', prefix=self.prefix)
            self.rpc_server.register_function(self.error, 
                                              name='error', prefix=self.prefix)
            
    def set_status(self):
        assert self.finished <= self.applied
        
        if not self.limit or self.applied < self.budgets:
            self.status = SUFFICIENT
        elif self.applied >= self.budgets and \
            self.finished < self.budgets:
            self.status = NOAPPLIED
        elif self.finished >= self.budgets:
            self.status = ALLFINISHED
        else:
            raise RuntimeError('size of applied and finished is impossible')
    
    def shutdown(self):
        self.save()
        
    def save(self):
        save_file = os.path.join(self.dir_, BUDGET_APPLY_STATUS_FILENAME)
        with open(save_file, 'w') as f:
            t = (self.applied, self.finished)
            pickle.dump(t, f)
    
    def load(self):
        save_file = os.path.join(self.dir_, BUDGET_APPLY_STATUS_FILENAME)
        if os.path.exists(save_file):
            with open(save_file) as f:
                self.applied, self.finished = pickle.load(f)
        
    @synchronized
    def set_budgets(self, budgets):
        self.budgets = budgets
        self.limit = self.budgets >= 0
        self.set_status()
    
    @synchronized
    def inc_budgets(self, budgets):
        if self.limit:
            self.budgets += budgets
            self.set_status()
        
    @synchronized
    def dec_budgets(self, budgets):
        if self.limit:
            self.budgets -= budgets
            self.set_status()
        
    @synchronized
    def apply(self, budget):
        if not self.limit:
            result = budget
        else:
            rest = self.budgets - self.applied
            result = max(min(budget, rest), 0)
        self.applied += result
        self.set_status()
        return result
    
    @synchronized
    def finish(self, size=1):
        self.finished += size
        self.finished = min(self.applied, self.finished)
        self.set_status()
        
    @synchronized
    def error(self, size=1):
        self.applied -= size
        self.applied = max(self.applied, self.finished)
        self.set_status()
        
class BudgetApplyClient(object):
    def __init__(self, server, app_name=None):
        if isinstance(server, BudgetApplyServer):
            self.remote = False
        else:
            self.remote = True
        self.server = server
        self.prefix = get_rpc_prefix(app_name, FUNC_PREFIX)
        
    def _call(self, func, *args):
        if self.remote:
            return client_call(self.server, self.prefix+func, *args)
        else:
            return getattr(self.server, func)(*args) 
        
    def apply(self, budget):
        return self._call('apply', budget)
        
    def finish(self, size=1):
        return self._call('finish', size)
    
    def error(self, size=1):
        return self._call('error', size)
    
    def set_budget(self, budget):
        return self._call('set_budget', budget)
    
    def inc_budget(self, budget):
        return self._call('inc_budget', budget)
    
    def dec_budget(self, budget):
        return self._call('dec_budget', budget)