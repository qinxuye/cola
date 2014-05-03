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

Created on 2014-5-2

@author: chine
'''

import time
import threading

from cola.core.utils import get_rpc_prefix

FUNC_PREFIX = 'speed_control_'

class SpeedControlServer(object):
    '''
    global inc counters:
    {
        'global': {}
        'ip1#inst1': {
            'pages': x
            'secs': t # accumulate the time from opening a webpage to parsing finished
        }
    }
    global acc counters:
    {
        'ip1#inst1': {
            'banned': [(t1, t2), ...]
            'normal': [(t3, t4, pages), ...] 
        }
    }
    '''
    
    def __init__(self, working_dir, settings, 
                 rpc_server=None, app_name=None,
                 counter_server=None):
        self.dir_ = working_dir
        self.settings = settings
        self.counter_server = counter_server
        self.rpc_server = rpc_server
        self.app_name = app_name
        self.prefix = get_rpc_prefix(self.app_name, FUNC_PREFIX)
        
        self.speed = self.settings.speed.max
        self.instance_speed = self.settings.speed.single
        self.limit = self.speed >= 0
        self.instance_limit = self.instance_speed >= 0
        self.adaptive = self.settings.speed.adaptive
        
        self.rates = 0
        self.instance_calc_rates = {}
        self.instance_rates = {}
        self.instance_spans = {}
        
        self.stopped = False
        
    def _init_rate_service(self):
        def clear():
            self.rates = 0
            time.sleep(60)
            if not self.stopped:
                clear()
        
        if self.limit or self.instance_limit:
            t = threading.Thread(target=clear)
            t.setDaemon(True)
            t.start()
        
    def shutdown(self):
        self.stopped = True
        
    def set_speed(self, speed):
        self.speed = speed
        self.limit = self.speed >= 0
        
    def set_instance_speed(self, speed):
        self.instance_speed = speed
        self.instance_limit = self.instance_speed >= 0
        
    def _default_rate(self, addr):
        pass
        
    def _calc_rate(self, addr):
        pass
        
    def _calc_span(self, addr):
        pass
        
    def require(self, addr, instance_id, size=1):
        addr = '#'.join((addr, str(instance_id)))
        
        