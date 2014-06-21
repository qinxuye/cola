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

import os
import threading
try:
    import cPickle as pickle
except ImportError:
    import pickle
from collections import defaultdict

from cola.core.rpc import client_call
from cola.core.utils import get_rpc_prefix

FUNC_PREFIX = 'speed_control_'
SPEED_CONTROL_STATUS_FILENAME = 'speed.control.status'

TRACK_BANNED_SIZE = 3

'''
counter_server may look like:

global inc counters:
{
    'global': {}
    'ip1#inst1': {
        'pages': x,
        'secs': t # accumulate the time from opening a webpage to parsing finished
    }
}
global acc counters:
{
    'ip1#inst1': {
        'banned_start': [t1, t3, ...],
        'banned_end': [t2, t4, ...],
        'normal_start': [t5, t7, ...],
        'normal_end' : [t6, t8, ...],
        'normal_pages': [p1, p2, ...]
    }
}
'''
class SpeedControlServer(object):
    def __init__(self, working_dir, settings, 
                 rpc_server=None, app_name=None,
                 counter_server=None, addrs=[]):
        self.dir_ = working_dir
        self.settings = settings
        self.rpc_server = rpc_server
        self.app_name = app_name
        self.counter_server = counter_server
        self.addrs = addrs
        
        self.speed = self.settings.job.speed.max
        self.instance_speed = self.settings.job.speed.single
        self.limit = self.speed >= 0
        self.instance_limit = self.instance_speed >= 0
        self.adaptive = self.settings.job.speed.adaptive
        
        self.lock = threading.Lock()
        
        instance_size = self.settings.job.instances
        self.instances = ['%s#%s'%(addr, instance_id) for addr in self.addrs \
                          for instance_id in range(instance_size)]
        
        self.instance_page_secs = {}
        self.instance_calc_rates = {}
        self.instance_curr_rates = defaultdict(lambda: 0)
        self.instance_spans = {}
        
        if not os.path.exists(self.dir_):
            os.makedirs(self.dir_)
        
        self.load()
        
        self.stopped = threading.Event()
        self._register_rpc()
        
        self.rate_service_stated = False
        self._init_rate_service()
        
    def _register_rpc(self):
        if self.rpc_server is not None:
            self.register_rpc(self, self.rpc_server, self.app_name)
            
    @classmethod
    def register_rpc(cls, speed_server, rpc_server, app_name=None):
        prefix = get_rpc_prefix(app_name, FUNC_PREFIX)
        rpc_server.register_function(speed_server.require, 'require', 
                                     prefix=prefix)
        rpc_server.register_function(speed_server.set_speed, 'set_speed',
                                     prefix=prefix)
        rpc_server.register_function(speed_server.set_instance_speed, 
                                     'set_instance_speed', prefix=prefix)
        
    def load(self):
        save_file = os.path.join(self.dir_, SPEED_CONTROL_STATUS_FILENAME)
        if os.path.exists(save_file):
            with open(save_file) as f:
                speed, instance_speed, adaptive, \
                instance_page_secs, instance_calc_rates, \
                instance_spans = pickle.load(f)
                
                if speed == self.speed and instance_speed == self.instance_speed and \
                    adaptive == self.adaptive:
                    self.instance_page_secs = instance_page_secs
                    self.instance_calc_rates = instance_calc_rates
                    self.instance_spans = instance_spans
    
    def save(self):
        save_file = os.path.join(self.dir_, SPEED_CONTROL_STATUS_FILENAME)
        if os.path.exists(save_file):
            with open(save_file) as f:
                t = (self.speed, self.instance_speed, self.adaptive, 
                    self.instance_page_secs, self.instance_calc_rates, self.instance_spans)
                pickle.dump(t, f)
     
    def _need_rate_service(self):
        return self.limit or self.instance_limit or self.adaptive
           
    def _init_rate_service(self):
        def clear():
            while not self.stopped.is_set():
                stopped = self.stopped.wait(60)
                if not stopped:
                    self.reset()
                    self.calc_spans()
        
        if self._need_rate_service() and \
            not self.rate_service_stated:
            self.rate_service_stated = True
            self.calc_spans()
            t = threading.Thread(target=clear)
            t.setDaemon(True)
            t.start()
        
    def shutdown(self):
        self.stopped.set()
        self.save()
        
    def set_speed(self, speed):
        self.speed = speed
        self.limit = self.speed >= 0
        self._init_rate_service()
        
    def set_instance_speed(self, speed):
        self.instance_speed = speed
        self.instance_limit = self.instance_speed >= 0
        self._init_rate_service()
        
    def set_adaptive(self, adaptive):
        self.adaptive = bool(adaptive)
        self._init_rate_service()
        
    def _calc_page_secs(self):
        for instance in self.instances:
            if self.counter_server is not None:
                pages = self.counter_server.inc_counter.get(instance, 'pages')
                secs = self.counter_server.inc_counter.get(instance, 'secs')
                if pages and secs:
                    self.instance_page_secs[instance] = float(secs) / pages
        vals = self.instance_page_secs.values()
        if len(vals) > 0:
            min_sec = min(vals)
        else:
            min_sec = 0.1 # default 0.1 sec as the time of a page processing
        for instance in self.instances:
            self.instance_page_secs[instance] = min_sec
        
    def _calc_rate(self):
        for instance in self.instances:
            max_ = int(60.0/self.instance_page_secs[instance])
            if self.instance_limit:
                self.instance_calc_rates[instance] = min(max_, 
                                                         self.instance_speed)
            else:
                self.instance_calc_rates[instance] = max_
                
        sum_ = sum(self.instance_calc_rates.values())
        if self.limit and sum_ > self.speed:
            ratio = float(self.speed) / sum_
            for instance, pages in self.instance_calc_rates.iteritems():
                self.instance_calc_rates[instance] = int(pages*ratio)
                
        if self.adaptive and self.counter_server is not None:
            counter = self.counter_server.acc_counter
            for instance in self.instances:
                banned_starts = counter.get(instance, 'banned_start', [])
                banned_ends = counter.get(instance, 'banned_end', [])
                banned = zip(banned_starts, banned_ends)[::-1][:TRACK_BANNED_SIZE]
                normal_starts = counter.get(instance, 'normal_start', [])
                normal_ends = counter.get(instance, 'normal_end', [])
                normal_pages = counter.get(instance, 'normal_pages', [])
                normal = zip(normal_starts, normal_ends, normal_pages)[::-1]
                
                if banned and normal:
                    pages = []
                    for ban in banned:
                        nearest_nor = None
                        for nor in normal:
                            if nor[1] <= ban[0]:
                                nearest_nor = nor
                                break
                        if nearest_nor is not None:
                            secs = ban[1] - nearest_nor[0]
                            ps = nearest_nor[2]
                            ave_pages = int(float(ps) / secs * 60.0)
                            pages.append(ave_pages)
                    if len(pages) > 0:
                        adaptive_pages = min(pages)
                        if adaptive_pages < self.instance_calc_rates[instance]:
                            self.instance_calc_rates[instance] = adaptive_pages
        
    def calc_spans(self):
        with self.lock:
            self._calc_page_secs()
            self._calc_rate()
        
            if self.counter_server is None:
                for instance in self.instances:
                    self.instance_spans[instance] = 0.0
            else:
                for instance in self.instances:
                    rate = self.instance_calc_rates[instance]
                    page_sec = self.instance_page_secs[instance]
                    if rate <= 0:
                        self.instance_spans[instance] = 60.0
                    else:
                        span = (60.0 - rate*page_sec) / rate
                        self.instance_spans[instance] = max(span, 0.0)
            
    def reset(self):
        with self.lock:
            for addr in self.instances:
                self.instance_curr_rates[addr] = 0
        
    def require(self, addr, instance_id, size=1):
        addr = '#'.join((addr, str(instance_id)))
        
        if not self.limit and not self.instance_limit and \
            not self.adaptive:
            return size, 0.0
        
        if addr not in self.instances:
            self.instances.append(addr)
            self.calc_spans()
        
        with self.lock:
            rest = self.instance_calc_rates[addr] - \
                self.instance_curr_rates[addr]            
            result = min(max(rest, 0), size)
            self.instance_curr_rates[addr] += result
            
        return result, self.instance_spans[addr]
    
class SpeedControlClient(object):
    def __init__(self, server, addr, instance_id, app_name=None):
        self.server = server
        self.addr = addr
        self.instance_id = instance_id
        self.app_name = app_name
        self.prefix = get_rpc_prefix(self.app_name, FUNC_PREFIX)
        
    def require(self, size=1):
        if isinstance(self.server, basestring):
            return client_call(self.server, self.prefix+'require', self.addr, 
                               self.instance_id, size)
        else:
            return self.server.require(self.addr, self.instance_id, 
                                       size=size)