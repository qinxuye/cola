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

import threading

class Aggregator(object):
    def create_combiner(self, val):
        raise NotImplementedError
    
    def merge_combiner(self, combiner1, combiner2):
        raise NotImplementedError
    
    def merge_val(self, combiner, val):
        raise NotImplementedError
    
class AddAggregator(Aggregator):
    def create_combiner(self, val):
        return val
    
    def merge_combiner(self, combiner1, combiner2):
        return combiner1 + combiner2
    
    def merge_val(self, combiner, val):
        return combiner + val
    
class MergeAggregator(Aggregator):
    def create_combiner(self, val):
        return [val, ]
    
    def merge_combiner(self, combiner1, combiner2):
        combiner1.extend(combiner2)
        return combiner1
            
    def merge_val(self, combiner, val):
        combiner.append(val)
        return combiner
    
class UniqAggregator(Aggregator):
    def create_combiner(self, val):
        return set(val)
    
    def merge_combiner(self, combiner1, combiner2):
        combiner1 |= combiner2
        return combiner1
    
    def merge_val(self, combiner, val):
        combiner.add(val)
        return combiner
    
class OverwriteAggregator(Aggregator):
    def create_combiner(self, val):
        return val
    
    def merge_combiner(self, combiner1, combiner2):
        return combiner2
    
    def merge_val(self, combiner, val):
        return val

class Counter(object):
    def __init__(self, agg=AddAggregator(), container=None):
        self.container = container if container is not None else dict()
        self.agg = agg
        
        self.lock = threading.Lock()
        
    def inc(self, group, item, val=1):
        with self.lock:
            if group not in self.container:
                self.container[group] = {}
            if item not in self.container[group]:
                self.container[group][item] = self.agg.create_combiner(val)
            else:
                src_combiner = self.container[group][item]
                self.container[group][item] = \
                    self.agg.merge_val(src_combiner, val)
                    
    def get(self, group, item, default_val=None):
        if group not in self.container:
            return default_val
        return self.container[group].get(item, default_val)
            
    def merge(self, other_counter):
        if self.agg.__class__ != other_counter.agg.__class__:
            raise ValueError('merged counter must have the same aggregator class')
        
        with self.lock:
            for group, kv in other_counter.container.iteritems():
                for item, val in kv.iteritems():
                    if group not in self.container:
                        self.container[group] = {}
                    if item not in self.container[group]:
                        self.container[group][item] = val
                    else:
                        self.container[group][item] = self.agg.merge_combiner(
                            self.container[group][item], val)
                    
    def reset(self, container=None):
        with self.lock:
            self.container = container if container is not None else dict()