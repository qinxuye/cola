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

from cola.core.utils import iterable

class Aggregator(object):
    """
    Aggregator which provide three abstract functions,
    first is to create a ``combiner`` by given a value,
    second is to merge two combiners,
    the last one is to merge value to a given combiner.
    """
    def create_combiner(self, val):
        raise NotImplementedError
    
    def merge_combiner(self, combiner1, combiner2):
        raise NotImplementedError
    
    def merge_val(self, combiner, val):
        raise NotImplementedError


class AddAggregator(Aggregator):
    """
    Just adding values.

    >>> agg = AddAggregator()
    >>> agg.create_combiner(0)
    0
    >>> agg.merge_combiner(0, 5)
    5
    >>> agg.merge_val(0, 5)
    5
    >>> agg.merge_val('0', '5')
    '05'
    """
    def create_combiner(self, val):
        return val
    
    def merge_combiner(self, combiner1, combiner2):
        combiner1 += combiner2
        return combiner1
    
    def merge_val(self, combiner, val):
        combiner1 += val
        return combiner


class MergeAggregator(Aggregator):
    """
    Each combiner is a ``list``.

    >>> agg = MergeAggregator()
    >>> agg.create_combiner(0)
    [0]
    >>> agg.merge_combiner([0], [5])
    [0, 5]
    >>> agg.merge_val([0], 5)
    [0, 5]
    """
    def create_combiner(self, val):
        return [val, ]
    
    def merge_combiner(self, combiner1, combiner2):
        combiner1.extend(combiner2)
        return combiner1
            
    def merge_val(self, combiner, val):
        combiner.append(val)
        return combiner


class UniqAggregator(Aggregator):
    """
    Each combiner is a ``set``.

    >>> agg1 = agg.create_combiner(0)
    >>> agg1
    set([0])
    >>> agg2 = agg.create_combiner([0, 5])
    >>> agg2
    set([0, 5])
    >>> agg.merge_combiner(agg1, agg2)
    set([0, 5])
    >>> agg1
    set([0, 5])
    >>> agg2
    set([0, 5])
    >>> agg.merge_val(agg1, 10)
    set([0, 10, 5])
    >>> agg1
    set([0, 10, 5])
    """
    def create_combiner(self, val):
        if not iterable(val):
            val = [val]
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
        combiner1 = combiner2
        return combiner1
    
    def merge_val(self, combiner, val):
        combiner = val
        return combiner


class Counter(object):
    """
    A counter can have several groups, each group is a dict.
    Besides, an instance of :class:`Aggregator` will decide
    how each item in a group to aggregate.
    """
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