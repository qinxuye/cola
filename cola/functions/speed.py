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

TRACK_BANNED_SIZE = 3

class SpeedControlServer(object):
    '''
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
    
    def __init__(self, working_dir, settings, 
                 rpc_server=None, app_name=None,
                 counter_server=None, addrs=[]):
        self.dir_ = working_dir
        self.settings = settings
        self.rpc_server = rpc_server
        self.app_name = app_name
        self.counter_server = counter_server
        self.addrs = addrs
        self.prefix = get_rpc_prefix(self.app_name, FUNC_PREFIX)
        
        self.speed = self.settings.job.speed.max
        self.instance_speed = self.settings.job.speed.single
        self.limit = self.speed >= 0
        self.instance_limit = self.instance_speed >= 0
        self.adaptive = self.settings.speed.adaptive
        
        self.lock = threading.Lock()
        
        instance_size = self.settings.job.instances
        self.instances = ['%s#%s'%(addr, instance_id) for addr in self.addrs \
                          for instance_id in range(instance_size)]
        
        self.instance_page_sec = {}
        self.instance_calc_rates = {}
        self.instance_curr_rates = {}
        self.instance_spans = {}
        
        self.stopped = False
        self._init_rate_service()
        
    def _init_rate_service(self):
        def clear():
            self.calc_span()
            self.reset()
            
            for _ in range(10):
                if not self.stopped:
                    time.sleep(6)
                    
            if not self.stopped:
                clear()
        
        if self.limit or self.instance_limit or self.adaptive:
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
        
    def _calc_page_sec(self):
        for instance in self.instances:
            pages = self.counter_server.inc_counter.get(instance, 'pages')
            secs = self.counter_server.inc_counter.get(instance, 'secs')
            if pages and secs:
                self.instance_page_sec[instance] = float(secs) / pages
        vals = self.instance_page_sec.values()
        if len(vals) > 0:
            min_sec = min(vals)
        else:
            min_sec = 1.0 # default 1 sec as the time of a page processing
        for instance in self.instances:
            if self.instance_page_sec.get(instance, None) is None:
                self.instance_page_sec[instance] = min_sec
        
    def _calc_rate(self):
        for instance in self.instances:
            max_ = int(60.0/self.instance_page_sec[instance])
            if self.instance_limit:
                self.instance_calc_rates[instance] = min(max_, 
                                                         self.instance_limit)
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
                        nor_ = None
                        for nor in normal:
                            if nor[1] < ban[0]:
                                nor_ = nor
                                break
                        if nor_ is not None:
                            secs = ban[1] - nor_[0]
                            ps = nor_[2]
                            ave_pages = int(float(ps) / secs * 60.0)
                            pages.append(ave_pages)
                    if len(pages) > 0:
                        adaptive_pages = min(pages)
                        if adaptive_pages < self.instance_calc_rates[instance]:
                            self.instance_calc_rates[instance] = adaptive_pages
        
    def calc_span(self):
        with self.lock:
            self._calc_page_sec()
            self._calc_rate()
        
        if self.counter_server is None:
            for addr in self.instances:
                self.instance_spans[addr] = 0.0
        else:
            for addr in self.instances:
                rate = self.instance_calc_rates[addr]
                page_sec = self.instance_page_sec[addr]
                if rate <= 0:
                    self.instance_spans[addr] = 60.0
                else:
                    span = (60.0 - rate*page_sec) / rate
                    self.instance_spans[addr] = max(span, 0.0)
            
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
            self.calc_span()
            return size, 0.0
        
        with self.lock:
            rest = self.instance_calc_rates[addr] - \
                self.instance_curr_rates[addr]
            result = min(max(rest, 0), size)
            self.instance_curr_rates[addr] += result
            
        return result, self.instance_spans[addr]