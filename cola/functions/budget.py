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

from cola.core.utils import get_rpc_prefix

FUNC_PREFIX = "budget_apply_"



class BudgetApplyServer(object):
    
    def __init__(self, settings, rpc_server=None, app_name=None):
        self.settings = settings
        self.is_local = rpc_server is None
        self.rpc_server = rpc_server
        self.app_name = app_name
        self.prefix = get_rpc_prefix(self.app_name, FUNC_PREFIX)
        
        self.budgets = settings.job.size
        
        self.applied = 0
        self.finished = 0
        
    def set_budgets(self, budgets):
        self.budgets = budgets
    
    def inc_budgets(self, budgets):
        self.budgets += budgets
        
    def apply(self):
